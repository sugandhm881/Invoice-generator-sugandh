import os
import io
import json
import logging
import base64
import zipfile
import tempfile
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders
from datetime import date, datetime, timedelta
from urllib.parse import unquote

import pandas as pd
from flask import Flask, request, send_file, jsonify, render_template, redirect, url_for, flash, session
from flask_login import LoginManager, UserMixin, login_user, login_required, logout_user, current_user
from dotenv import load_dotenv
from fpdf import FPDF
from PIL import Image  # <--- NEW IMPORT FOR IMAGE PROCESSING

# --- LOAD ENV ---
load_dotenv()

# --- FIREBASE SETUP ---
import firebase_admin
from firebase_admin import credentials, firestore

if not firebase_admin._apps:
    firebase_creds_env = os.getenv("FIREBASE_CREDENTIALS")
    if firebase_creds_env:
        cred_json = json.loads(base64.b64decode(firebase_creds_env))
        cred = credentials.Certificate(cred_json)
    else:
        # LOCAL FALLBACK
        key_path = "invoice-generator-5c42c-firebase-adminsdk-fbsvc-dd71702a41.json"
        if os.path.exists(key_path):
            cred = credentials.Certificate(key_path)
        else:
            # Create a dummy app context if no creds (prevents crash on deployment w/o env)
            cred = None
            logging.warning("No Firebase Credentials found. DB calls will fail.")
    
    if cred:
        firebase_admin.initialize_app(cred)

db = firestore.client() if firebase_admin._apps else None

# ------------------ CONFIG ------------------
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
CALIBRI_FONT_PATH = os.path.join(BASE_DIR, "CALIBRI.TTF")
DEFAULT_LOGO = os.path.join(BASE_DIR, "static", "logo.png") 
DEFAULT_SIGNATURE = os.path.join(BASE_DIR, "static", "Signatory.png")
TEMPLATE_DIR = os.path.join(BASE_DIR, "templates")

app = Flask(__name__, template_folder=TEMPLATE_DIR, static_folder=os.path.join(BASE_DIR, "static"))
app.secret_key = os.getenv("SECRET_KEY", "change_this_secret")

EMAIL_HOST = os.getenv('EMAIL_HOST', 'smtp.gmail.com')
EMAIL_PORT = int(os.getenv('EMAIL_PORT', 587))
EMAIL_USER = os.getenv('EMAIL_USER')
EMAIL_PASSWORD = os.getenv('EMAIL_PASSWORD')

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

# ------------------ AUTH ------------------
login_manager = LoginManager()
login_manager.init_app(app)
login_manager.login_view = "login"

MASTER_USERNAME = os.getenv("LOGIN_USER", "admin")
MASTER_PASSWORD = os.getenv("LOGIN_PASS", "password")

class User(UserMixin):
    def __init__(self, id, is_master=False):
        self.id = id
        self.is_master = is_master

@login_manager.user_loader
def load_user(user_id):
    if user_id == MASTER_USERNAME:
        return User(user_id, is_master=True)
    
    try:
        user_doc = db.collection('app_users').document(user_id).get()
        if user_doc.exists:
            return User(user_id, is_master=False)
    except: pass
    return None

# ------------------ IMAGE HELPER (NEW) ------------------
def compress_image(file_storage, max_width=400):
    """Resizes and compresses image to keep it under 1MB for Firestore."""
    try:
        img = Image.open(file_storage)
        
        # Calculate new height to maintain aspect ratio
        width_percent = (max_width / float(img.size[0]))
        
        # Only resize if the image is actually larger than max_width
        if width_percent < 1:
            new_height = int((float(img.size[1]) * float(width_percent)))
            img = img.resize((max_width, new_height), Image.Resampling.LANCZOS)
        
        output = io.BytesIO()
        
        # Preserve PNG transparency (for signatures), Convert others to JPEG
        if img.format == 'PNG' or img.mode == 'RGBA':
            img.save(output, format='PNG', optimize=True)
        else:
            img = img.convert('RGB')
            img.save(output, format='JPEG', quality=70, optimize=True)
            
        return base64.b64encode(output.getvalue()).decode('utf-8')
    except Exception as e:
        logging.error(f"Error processing image: {e}")
        return None

# ------------------ DATABASE CONTEXT HELPER ------------------
def get_db_base(target_user=None):
    if target_user:
        if target_user == MASTER_USERNAME: return db
        return db.collection('users').document(target_user)

    if current_user.is_authenticated and not current_user.is_master:
        return db.collection('users').document(current_user.id)

    view_mode = session.get('view_mode')
    if current_user.is_authenticated and current_user.is_master and view_mode and view_mode != MASTER_USERNAME:
        return db.collection('users').document(view_mode)

    return db

def get_all_users():
    users = [MASTER_USERNAME]
    try:
        docs = db.collection('app_users').stream()
        for doc in docs:
            users.append(doc.id)
    except: pass
    return sorted(users)

@app.context_processor
def inject_global_data():
    if current_user.is_authenticated:
        profile = get_seller_profile_data()
        all_users = get_all_users() if current_user.is_master else []
        viewing_user = session.get('view_mode', current_user.id)
        return dict(profile=profile, current_user=current_user, all_users=all_users, viewing_user=viewing_user)
    return dict(profile={}, current_user=None, all_users=[], viewing_user=None)

# ------------------ FIRESTORE HELPERS ------------------
def get_seller_profile_data(target_user_id=None):
    try:
        base = get_db_base(target_user=target_user_id)
        is_root = (base == db)
        
        if is_root:
            doc = base.collection('config').document('seller_profile').get()
        else:
            doc = base.collection('config').document('profile').get()

        if doc.exists:
            return doc.to_dict()
    except Exception as e:
        logging.error(f"Error fetching profile: {e}")
    
    return {
        "company_name": "SM Tech",
        "invoice_prefix": "SMT",
    }

def save_seller_profile_data(data, target_user_id=None):
    base = get_db_base(target_user=target_user_id)
    is_root = (base == db)
    if is_root:
        base.collection('config').document('seller_profile').set(data, merge=True)
    else:
        base.collection('config').document('profile').set(data, merge=True)

def load_clients():
    base = get_db_base()
    docs = base.collection('clients').stream()
    return {doc.id: doc.to_dict() for doc in docs}

def save_single_client(name, data):
    base = get_db_base()
    base.collection('clients').document(name).set(data, merge=True)

def load_invoices():
    base = get_db_base()
    docs = base.collection('invoices').stream()
    return [doc.to_dict() for doc in docs]

def save_single_invoice(invoice_data):
    base = get_db_base()
    doc_id = invoice_data['bill_no'].replace('/', '_')
    base.collection('invoices').document(doc_id).set(invoice_data)

def load_particulars():
    base = get_db_base()
    docs = base.collection('particulars').stream()
    return {doc.id: doc.to_dict() for doc in docs}

def save_single_particular(name, data):
    base = get_db_base()
    base.collection('particulars').document(name).set(data, merge=True)

def get_next_counter(is_credit_note=False):
    base = get_db_base()
    doc_ref = base.collection('config').document('counters')
    @firestore.transactional
    def update_in_transaction(transaction, doc_ref):
        snapshot = doc_ref.get(transaction=transaction)
        if not snapshot.exists:
            new_data = {"counter": 0, "cn_counter": 0}
            transaction.set(doc_ref, new_data)
            current_val = 0
        else:
            current_data = snapshot.to_dict()
            field = "cn_counter" if is_credit_note else "counter"
            current_val = current_data.get(field, 0)
        new_val = current_val + 1
        field = "cn_counter" if is_credit_note else "counter"
        transaction.update(doc_ref, {field: new_val})
        return new_val
    return update_in_transaction(db.transaction(), doc_ref)

# ------------------ UTILS & PDF ------------------
def convert_to_words(number):
    units = ["","One","Two","Three","Four","Five","Six","Seven","Eight","Nine","Ten",
             "Eleven","Twelve","Thirteen","Fourteen","Fifteen","Sixteen","Seventeen","Eighteen","Nineteen"]
    tens = ["","","Twenty","Thirty","Forty","Fifty","Sixty","Seventy","Eighty","Ninety"]
    def two_digit(n):
        return units[n] if n < 20 else tens[n//10] + (" " + units[n%10] if n%10 else "")
    def three_digit(n):
        s = ""
        if n >= 100: s += units[n//100] + " Hundred" + (" " if n % 100 else "")
        if n % 100: s += two_digit(n%100)
        return s
    n = int(abs(number))
    paise = round((abs(number) - n) * 100)
    crore = n // 10000000; n %= 10000000
    lakh = n // 100000; n %= 100000
    thousand = n // 1000; n %= 1000
    hundred = n
    parts = []
    if crore: parts.append(three_digit(crore) + " Crore")
    if lakh: parts.append(three_digit(lakh) + " Lakh")
    if thousand: parts.append(three_digit(thousand) + " Thousand")
    if hundred: parts.append(three_digit(hundred))
    words = " ".join(parts) if parts else "Zero"
    if paise: words += f" and {two_digit(paise)} Paise"
    return words + " Only"

def send_email_with_attachment(to_email, subject, body, attachment_bytes, filename):
    msg = MIMEMultipart()
    msg['From'] = EMAIL_USER
    msg['To'] = to_email
    msg['Subject'] = subject
    msg.attach(MIMEText(body, 'plain'))
    part = MIMEBase('application', 'octet-stream')
    part.set_payload(attachment_bytes.getvalue())
    encoders.encode_base64(part)
    part.add_header('Content-Disposition', f'attachment; filename={filename}')
    msg.attach(part)
    with smtplib.SMTP(EMAIL_HOST, EMAIL_PORT) as server:
        server.starttls()
        server.login(EMAIL_USER, EMAIL_PASSWORD)
        server.send_message(msg)

def PDF_Generator(invoice_data, is_credit_note=False):
    pdf = FPDF()
    pdf.add_page()
    pdf.add_font("Calibri", "", CALIBRI_FONT_PATH, uni=True)
    pdf.add_font("Calibri", "B", CALIBRI_FONT_PATH, uni=True)

    profile = get_seller_profile_data()
    
    margin = 15
    page_width = pdf.w - 2 * margin 
    col_width = (page_width / 2) - 5 
    line_height = 5 
    
    logo_data = profile.get('logo_base64')
    if logo_data:
        try:
            with tempfile.NamedTemporaryFile(delete=False, suffix=".png") as tmp:
                tmp.write(base64.b64decode(logo_data))
                tmp_path = tmp.name
            pdf.image(tmp_path, x=15, y=8, w=30)
            os.unlink(tmp_path)
        except: pass
    elif os.path.exists(DEFAULT_LOGO):
        pdf.image(DEFAULT_LOGO, x=15, y=8, w=30)
    
    pdf.set_font("Calibri", "B", 22)
    is_non_gst = invoice_data.get('is_non_gst', False)
    if is_credit_note:
        pdf.set_text_color(220, 38, 38)
        header_title = "CREDIT NOTE"
    elif is_non_gst:
        pdf.set_text_color(0, 128, 0)
        header_title = "BILL OF SUPPLY"
    else:
        pdf.set_text_color(255, 165, 0)
        header_title = "TAX INVOICE"

    pdf.cell(page_width, 10, profile.get('company_name', 'SM Tech'), ln=True, align='C')
    pdf.set_font("Calibri", "B", 14)
    pdf.set_text_color(0, 0, 0)
    pdf.cell(page_width, 8, header_title, ln=True, align='C')
    pdf.set_font("Calibri", "", 10)
    
    my_gstin = profile.get('gstin', '')
    address_str = f"{profile.get('address_1','')}\n{profile.get('address_2','')}\nPhone: {profile.get('phone','')} | E-mail: {profile.get('email','')}\nGSTIN: {my_gstin}"
    pdf.multi_cell(page_width, line_height, address_str, align='C')
    pdf.ln(5)
    pdf.line(margin, pdf.get_y(), pdf.w - margin, pdf.get_y())

    if is_credit_note:
        pdf.ln(3)
        pdf.set_font("Calibri", "B", 11)
        pdf.set_text_color(220, 38, 38)
        ref_bill = invoice_data.get('original_invoice_no', '')
        if not ref_bill:
             ref_bill = invoice_data.get('bill_no', '').replace('CN-', '').replace('TE-CN', 'TE')
        pdf.cell(0, 7, f"This is a credit note against Invoice No: {ref_bill}", ln=True, align='C')
        pdf.set_text_color(0, 0, 0)
    pdf.ln(5)

    bill_to_text = f"{invoice_data.get('client_name','')}\n{invoice_data.get('client_address1','')}\n{invoice_data.get('client_address2','')}\nGSTIN: {invoice_data.get('client_gstin','')}\nEmail: {invoice_data.get('client_email','')}\nMobile: {invoice_data.get('client_mobile','')}"
    ship_to_text = f"{invoice_data.get('shipto_name','')}\n{invoice_data.get('shipto_address1','')}\n{invoice_data.get('shipto_address2','')}\nGSTIN: {invoice_data.get('shipto_gstin','')}\nEmail: {invoice_data.get('shipto_email','')}\nMobile: {invoice_data.get('shipto_mobile','')}"
    invoice_no_text = f"{header_title} No: {invoice_data.get('bill_no','')}"
    invoice_date_text = f"Date: {invoice_data.get('invoice_date','')}"
    po_number_text = f"PO Number: {invoice_data.get('po_number','')}"

    y_start = pdf.get_y()
    pdf.set_font("Calibri", "B", 12)
    pdf.cell(col_width, line_height, "Bill To:", ln=True)
    pdf.set_font("Calibri", "", 10)
    pdf.multi_cell(col_width, line_height, bill_to_text)
    y_left = pdf.get_y()
    pdf.set_y(y_start)
    pdf.set_x(margin + col_width + 10)
    pdf.set_font("Calibri", "B", 10)
    pdf.multi_cell(col_width, line_height, f"{invoice_no_text}\n{invoice_date_text}")
    y_right = pdf.get_y()
    pdf.set_y(max(y_left, y_right))
    pdf.ln(5) 
    
    y_start = pdf.get_y()
    pdf.set_font("Calibri", "B", 12)
    pdf.cell(col_width, line_height, "Ship To:", ln=True)
    pdf.set_font("Calibri", "", 10)
    pdf.multi_cell(col_width, line_height, ship_to_text)
    y_left = pdf.get_y()
    pdf.set_y(y_start)
    pdf.set_x(margin + col_width + 10)
    pdf.set_font("Calibri", "B", 10)
    pdf.multi_cell(col_width, line_height, po_number_text)
    y_right = pdf.get_y()
    pdf.set_y(max(y_left, y_right))
    pdf.ln(10) 
    
    particulars_w, hsn_w, qty_w, rate_w, tax_percent_w, taxable_amt_w, tax_amt_w, total_w = 60, 20, 10, 20, 15, 20, 20, 15
    pdf.set_fill_color(255, 204, 153)
    pdf.set_font("Calibri", "B", 10)
    pdf.cell(particulars_w, 8, "Particulars", 1, 0, 'L', True)
    pdf.cell(hsn_w, 8, "HSN", 1, 0, 'C', True)
    pdf.cell(qty_w, 8, "Qty", 1, 0, 'C', True)
    pdf.cell(rate_w, 8, "Rate", 1, 0, 'R', True)
    pdf.cell(tax_percent_w, 8, "Tax %", 1, 0, 'R', True)
    pdf.cell(taxable_amt_w, 8, "Taxable", 1, 0, 'R', True)
    pdf.cell(tax_amt_w, 8, "Tax Amt", 1, 0, 'R', True)
    pdf.cell(total_w, 8, "Total", 1, 1, 'R', True)

    pdf.set_font("Calibri", "", 10)
    particulars = invoice_data.get('particulars', [])
    hsns = invoice_data.get('hsns', [])
    qtys = invoice_data.get('qtys', [])
    rates = invoice_data.get('rates', [])
    taxrates = invoice_data.get('taxrates', [])
    amounts = invoice_data.get('amounts', []) 
    line_tax_amounts = invoice_data.get('line_tax_amounts', [])
    line_total_amounts = invoice_data.get('line_total_amounts', [])
    
    for i in range(len(particulars)):
        start_y, start_x = pdf.get_y(), pdf.get_x()
        pdf.multi_cell(particulars_w, 7, str(particulars[i]), 0, 'L')
        y_after = pdf.get_y()
        row_h = y_after - start_y
        pdf.set_xy(start_x + particulars_w, start_y)
        display_hsn = "" if is_non_gst else (str(hsns[i]) if i < len(hsns) else '')
        pdf.cell(hsn_w, row_h, display_hsn, 1, 0, 'C')
        pdf.cell(qty_w, row_h, str(abs(float(qtys[i]))), 1, 0, 'C')
        pdf.cell(rate_w, row_h, f"{abs(float(rates[i])):.2f}", 1, 0, 'R')
        pdf.cell(tax_percent_w, row_h, f"{abs(float(taxrates[i])):.2f}%", 1, 0, 'R')
        pdf.cell(taxable_amt_w, row_h, f"{abs(float(amounts[i])):.2f}", 1, 0, 'R')
        pdf.cell(tax_amt_w, row_h, f"{abs(float(line_tax_amounts[i])):.2f}", 1, 0, 'R')
        pdf.cell(total_w, row_h, f"{abs(float(line_total_amounts[i])):.2f}", 1, 0, 'R')
        pdf.rect(start_x, start_y, particulars_w, row_h)
        pdf.set_y(y_after)

    pdf.set_font("Calibri", "B", 10)
    pdf.set_fill_color(230, 230, 230)
    def add_total(label, val):
        pdf.cell(150, 7, label, 1, 0, 'R', True)
        pdf.cell(30, 7, f"{abs(val):.2f}", 1, 1, 'R', True)
    
    add_total("Sub Total", invoice_data.get('sub_total',0))
    add_total("IGST", invoice_data.get('igst',0))
    add_total("CGST", invoice_data.get('cgst',0))
    add_total("SGST", invoice_data.get('sgst',0))
    add_total("Grand Total", invoice_data.get('grand_total',0))
    pdf.ln(10)

    pdf.set_font("Calibri", "", 10)
    bank_text = f"Rupees: {convert_to_words(invoice_data.get('grand_total',0))}\nBank Name: {profile.get('bank_name','')}\nAccount Holder: {profile.get('account_holder','')}\nAccount No: {profile.get('account_no','')}\nIFSC: {profile.get('ifsc','')}"
    pdf.multi_cell(page_width, line_height, bank_text)
    pdf.ln(5)
    pdf.set_font("Calibri", "B", 10)
    pdf.cell(0, 5, f"For {profile.get('company_name', 'SM Tech')}", ln=True, align='R')

    sig_data = profile.get('signature_base64')
    if sig_data:
        try:
            with tempfile.NamedTemporaryFile(delete=False, suffix=".png") as tmp:
                tmp.write(base64.b64decode(sig_data))
                tmp_path = tmp.name
            pdf.image(tmp_path, x=pdf.w - margin - 40, y=pdf.get_y(), w=40)
            os.unlink(tmp_path)
        except: pass
    elif os.path.exists(DEFAULT_SIGNATURE):
        pdf.image(DEFAULT_SIGNATURE, x=pdf.w - margin - 40, y=pdf.get_y(), w=40) 
    
    return io.BytesIO(pdf.output(dest="S").encode("latin-1"))

# ------------------ ROUTES ------------------

@app.route("/", methods=["GET"])
def root():
    if current_user.is_authenticated: return redirect(url_for("home"))
    return redirect(url_for("login"))

@app.route("/login", methods=["GET","POST"])
def login():
    if current_user.is_authenticated: return redirect(url_for("home"))
    error = None
    if request.method=="POST":
        username = request.form.get("username","")
        password = request.form.get("password","")
        
        # 1. Master Check
        if username == MASTER_USERNAME and password == MASTER_PASSWORD:
            login_user(User(username, is_master=True))
            return redirect(url_for("home"))
        
        # 2. Sub-User Check (Firestore)
        try:
            user_doc = db.collection('app_users').document(username).get()
            if user_doc.exists:
                user_data = user_doc.to_dict()
                if user_data.get('password') == password:
                    login_user(User(username, is_master=False))
                    return redirect(url_for("home"))
        except: pass
        
        error = "Invalid Credentials"
    return render_template("login.html", error=error)

@app.route("/api/get-branding/<username>")
def get_branding(username):
    """API for login page to fetch custom logo and name."""
    try:
        # Master Check
        if username == MASTER_USERNAME:
            profile = get_seller_profile_data(target_user_id=MASTER_USERNAME)
            return jsonify({
                "found": True,
                "company_name": profile.get('company_name', 'SM Tech'),
                "logo_base64": profile.get('logo_base64', None)
            })

        # Sub-User Check
        user_doc = db.collection('app_users').document(username).get()
        if user_doc.exists:
            profile = get_seller_profile_data(target_user_id=username)
            return jsonify({
                "found": True,
                "company_name": profile.get('company_name', 'SM Tech'),
                "logo_base64": profile.get('logo_base64', None)
            })
            
        return jsonify({"found": False})

    except Exception as e:
        return jsonify({"found": False, "error": str(e)})

@app.route("/home", methods=["GET"])
@login_required
def home():
    return render_template("index.html")

@app.route("/logout")
@login_required
def logout():
    session.pop('view_mode', None)
    logout_user()
    return redirect(url_for("login"))

@app.route("/set-view-mode/<user_id>")
@login_required
def set_view_mode(user_id):
    if not current_user.is_master:
        return "Unauthorized", 403
    
    session['view_mode'] = user_id
    flash(f"Now viewing data as: {user_id}", "info")
    return redirect(url_for("home"))

@app.route('/profile', methods=['GET', 'POST'])
@login_required
def user_profile():
    # --- GET: RENDER PROFILE ---
    if request.method == 'GET':
        target_user = request.args.get('edit_user') 
        
        if not current_user.is_master:
            target_user = current_user.id
        elif not target_user:
            target_user = MASTER_USERNAME

        profile_data = get_seller_profile_data(target_user_id=target_user)
        return render_template('user_profile.html', profile=profile_data, target_user=target_user)

    # --- POST: UPDATE PROFILE (MASTER ONLY) ---
    if request.method == 'POST':
        # Create New User
        if 'new_username' in request.form:
            if not current_user.is_master: return "Unauthorized", 403
            new_u = request.form.get('new_username')
            new_p = request.form.get('new_password')
            if new_u and new_p:
                db.collection('app_users').document(new_u).set({"password": new_p})
                flash(f"User {new_u} created successfully!", "success")
            return redirect(url_for('user_profile'))

        # Update Profile
        target_user = request.form.get('target_user_id')
        
        if not current_user.is_master:
            flash("You are not authorized to update profile settings.", "error")
            return redirect(url_for('user_profile'))

        data = {
            "company_name": request.form.get('company_name'),
            "invoice_prefix": request.form.get('invoice_prefix', 'TE'),
            "address_1": request.form.get('address_1'),
            "address_2": request.form.get('address_2'),
            "phone": request.form.get('phone'),
            "email": request.form.get('email'),
            "gstin": request.form.get('gstin'),
            "bank_name": request.form.get('bank_name'),
            "account_holder": request.form.get('account_holder'),
            "account_no": request.form.get('account_no'),
            "ifsc": request.form.get('ifsc'),
        }

        # --- PROCESS IMAGES WITH COMPRESSION ---
        logo_file = request.files.get('logo')
        if logo_file and logo_file.filename:
            compressed_logo = compress_image(logo_file, max_width=400)
            if compressed_logo:
                data['logo_base64'] = compressed_logo
            else:
                flash("Error processing logo image.", "error")
        else:
            existing = get_seller_profile_data(target_user_id=target_user)
            if 'logo_base64' in existing: data['logo_base64'] = existing['logo_base64']

        sig_file = request.files.get('signature')
        if sig_file and sig_file.filename:
            compressed_sig = compress_image(sig_file, max_width=300)
            if compressed_sig:
                data['signature_base64'] = compressed_sig
            else:
                flash("Error processing signature image.", "error")
        else:
            existing = get_seller_profile_data(target_user_id=target_user)
            if 'signature_base64' in existing: data['signature_base64'] = existing['signature_base64']

        save_seller_profile_data(data, target_user_id=target_user)
        flash(f'Profile for {target_user} Updated!', 'success')
        return redirect(url_for('user_profile', edit_user=target_user))

@app.route("/generate-invoice", methods=["POST"])
@login_required
def handle_invoice():
    try:
        data = request.json or {}
        is_non_gst = data.get('is_non_gst', False)
        client_name = data.get('client_name','').strip()
        particulars = data.get('particulars', [])
        if isinstance(particulars, list):
            particulars = [str(p).strip() for p in particulars]
        else:
            particulars = [str(particulars).strip()]
            
        qtys = data.get('qtys', [])
        rates = data.get('rates', [])
        taxrates = data.get('taxrates', [])
        hsns = data.get('hsns', [])
        amounts_inclusive = data.get("amounts", [])

        # Save Particulars & Client
        for i, item_name in enumerate(particulars):
            if item_name:
                storage_key = f"{item_name}_NONGST" if is_non_gst else item_name
                hsn_val = "" if is_non_gst else (hsns[i] if i < len(hsns) else "")
                rate_val = rates[i] if i < len(rates) else 0
                tax_val = 0 if is_non_gst else (taxrates[i] if i < len(taxrates) else 0)
                save_single_particular(storage_key, {"hsn": hsn_val, "rate": rate_val, "taxrate": tax_val})

        if client_name:
            save_single_client(client_name, {
                "address1": data.get('client_address1',''),
                "address2": data.get('client_address2',''),
                "gstin": data.get('client_gstin',''),
                "email": data.get('client_email',''),
                "mobile": data.get('client_mobile','')
            })

        auto_generate = data.get("auto_generate", True)
        if auto_generate:
            counter = get_next_counter(is_credit_note=False)
            profile = get_seller_profile_data()
            prefix = profile.get('invoice_prefix', 'TE').upper()
            bill_no = f"{prefix}/2025-26/{counter:04d}"
            invoice_date_str = date.today().strftime('%d-%b-%Y')
        else:
            bill_no = str(data.get("manual_bill_no","")).strip()
            invoices = load_invoices()
            if any(inv['bill_no']==bill_no for inv in invoices): return jsonify({"error": "Duplicate Invoice"}), 409
            manual_date = data.get("manual_invoice_date","")
            invoice_date_str = datetime.strptime(manual_date, '%Y-%m-%d').strftime('%d-%b-%Y') if manual_date else date.today().strftime('%d-%b-%Y')
        
        my_gstin = get_seller_profile_data().get('gstin', '')
        my_state_code = my_gstin[:2] if my_gstin else '06'
        
        line_taxable = []
        line_tax = []
        line_total = []
        total_igst, total_cgst, total_sgst = 0.0, 0.0, 0.0

        for i in range(len(amounts_inclusive)):
            inclusive = float(amounts_inclusive[i])
            tax_rate = 0 if is_non_gst else (float(taxrates[i]) if i < len(taxrates) else 0)
            taxable = round(inclusive / (1 + tax_rate/100), 2)
            tax_amt = round(inclusive - taxable, 2)
            
            line_taxable.append(taxable)
            line_tax.append(tax_amt)
            line_total.append(inclusive)
            
            if not is_non_gst:
                if data.get('client_gstin','').startswith(my_state_code):
                    cgst_amt = round(tax_amt/2, 2)
                    sgst_amt = tax_amt - cgst_amt
                    total_cgst += cgst_amt
                    total_sgst += sgst_amt
                else:
                    total_igst += tax_amt

        invoice_data = {
            "bill_no": bill_no,
            "invoice_date": invoice_date_str,
            "is_non_gst": is_non_gst,
            "client_name": client_name,
            "client_address1": data.get('client_address1'),
            "client_address2": data.get('client_address2'),
            "client_gstin": data.get('client_gstin'),
            "client_email": data.get('client_email'),
            "client_mobile": data.get('client_mobile'),
            "shipto_name": data.get('shipto_name'),
            "shipto_address1": data.get('shipto_address1'),
            "shipto_address2": data.get('shipto_address2'),
            "shipto_gstin": data.get('shipto_gstin'),
            "shipto_email": data.get('shipto_email'),
            "shipto_mobile": data.get('shipto_mobile'),
            "po_number": data.get('po_number'),
            "my_gstin": my_gstin,
            "particulars": particulars,
            "qtys": qtys,
            "rates": rates,
            "taxrates": taxrates,
            "hsns": hsns,
            "amounts": line_taxable,
            "sub_total": round(sum(line_taxable), 2),
            "igst": round(total_igst, 2),
            "cgst": round(total_cgst, 2),
            "sgst": round(total_sgst, 2),
            "grand_total": round(sum(line_taxable) + total_igst + total_cgst + total_sgst, 2),
            "line_tax_amounts": line_tax,
            "line_total_amounts": line_total
        }
        
        save_single_invoice(invoice_data)
        pdf_file = PDF_Generator(invoice_data)
        return send_file(pdf_file, mimetype="application/pdf", as_attachment=True, download_name=f"Invoice_{bill_no.replace('/','_')}.pdf")
    except Exception as e:
        logging.error(f"Error generating invoice: {e}", exc_info=True)
        return jsonify({"error":str(e)}),500

@app.route('/send-daily-report', methods=['GET'])
def send_daily_report():
    try:
        invoices = load_invoices() 
        if not invoices: return "No invoices found."

        profile = get_seller_profile_data()
        seller_email = profile.get('email', EMAIL_USER)

        report_data = []
        for inv in invoices:
            report_data.append({
                "Date": inv.get('invoice_date'),
                "Bill No": inv.get('bill_no'),
                "Client": inv.get('client_name'),
                "Total": inv.get('grand_total')
            })

        df = pd.DataFrame(report_data)
        output = io.BytesIO()
        with pd.ExcelWriter(output, engine='openpyxl') as writer:
            df.to_excel(writer, index=False)
        output.seek(0)
        
        subject = f"Daily Sales Report (All Time) - {date.today()}"
        body = "Attached is the cumulative sales report generated today."
        
        send_email_with_attachment(seller_email, subject, body, output, f"All_Time_Report_{date.today()}.xlsx")
        return f"Report sent to {seller_email}!"
    except Exception as e:
        return f"Error: {str(e)}", 500

@app.route('/clients', methods=['GET'])
@login_required
def get_clients_route():
    return jsonify(load_clients())

@app.route('/particulars', methods=['GET'])
@login_required
def get_particulars_route():
    return jsonify(load_particulars())

@app.route('/invoices-list', methods=['GET'])
@login_required
def invoices_list_route():
    return jsonify(load_invoices())

@app.route('/download-zip', methods=['POST'])
@login_required
def download_zip():
    try:
        data = request.json or {}
        bill_nos = data.get('bill_nos', [])
        if not bill_nos: return jsonify({"error": "No invoices selected"}), 400

        all_invoices = load_invoices()
        mem_zip = io.BytesIO()

        with zipfile.ZipFile(mem_zip, mode="w", compression=zipfile.ZIP_DEFLATED) as zf:
            for bno in bill_nos:
                inv = next((i for i in all_invoices if i['bill_no'] == bno), None)
                if inv:
                    is_cn = inv.get('is_credit_note', False)
                    pdf_bytes = PDF_Generator(inv, is_credit_note=is_cn)
                    filename = f"{'CreditNote' if is_cn else 'Invoice'}_{bno.replace('/','_')}.pdf"
                    zf.writestr(filename, pdf_bytes.getvalue())

        mem_zip.seek(0)
        return send_file(mem_zip, mimetype="application/zip", as_attachment=True, download_name="Invoices_Bundle.zip")
    except Exception as e:
        logging.error(f"Error zipping: {e}")
        return jsonify({"error": str(e)}), 500

@app.route('/email-invoice/<path:bill_no>', methods=['POST'])
@login_required
def email_invoice(bill_no):
    try:
        bill_no = unquote(bill_no)
        invoices = load_invoices()
        inv = next((i for i in invoices if i['bill_no'] == bill_no), None)
        if not inv: return jsonify({"error": "Invoice not found"}), 404
        
        client_email = inv.get('client_email')
        if not client_email: return jsonify({"error": "Client email not found in invoice data"}), 400

        is_cn = inv.get('is_credit_note', False)
        doc_type = "Credit Note" if is_cn else "Invoice"
        pdf_bytes = PDF_Generator(inv, is_credit_note=is_cn)
        
        profile = get_seller_profile_data()
        subject = f"{doc_type} {bill_no} from {profile.get('company_name','SM Tech')}"
        body = f"Dear {inv.get('client_name')},\n\nPlease find attached {doc_type} {bill_no}.\n\nRegards,\n{profile.get('company_name','SM Tech')}"
        
        send_email_with_attachment(client_email, subject, body, pdf_bytes, f"{doc_type}_{bill_no.replace('/','_')}.pdf")
        return jsonify({"message": "Email sent successfully!"})
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/download-invoice/<path:bill_no>', methods=['GET'])
@login_required
def download_invoice(bill_no):
    bill_no = unquote(bill_no)
    invoices = load_invoices()
    invoice_data = next((inv for inv in invoices if inv['bill_no']==bill_no),None)
    if not invoice_data: return jsonify({"error":"Invoice not found"}),404
    pdf_file = PDF_Generator(invoice_data)
    return send_file(pdf_file, mimetype="application/pdf", as_attachment=True, download_name=f"Invoice_{bill_no.replace('/','_')}.pdf")

@app.route('/generate-credit-note/<path:bill_no>', methods=['GET'])
@login_required
def generate_credit_note(bill_no):
    try:
        bill_no = unquote(bill_no)
        invoices = load_invoices()
        existing_cn = next((inv for inv in invoices if inv.get('original_invoice_no') == bill_no), None)
        if not existing_cn:
            possible_old_cn_id = f"CN-{bill_no}"
            existing_cn = next((inv for inv in invoices if inv['bill_no'] == possible_old_cn_id), None)
        if existing_cn:
            pdf_file = PDF_Generator(existing_cn, is_credit_note=True)
            return send_file(pdf_file, mimetype="application/pdf", as_attachment=True, download_name=f"CreditNote_{existing_cn['bill_no'].replace('/','_')}.pdf")

        original_inv = next((inv for inv in invoices if inv['bill_no'] == bill_no), None)
        if not original_inv: return jsonify({"error": "Original Invoice not found"}), 404

        cn_counter = get_next_counter(is_credit_note=True)
        profile = get_seller_profile_data()
        prefix = profile.get('invoice_prefix', 'TE').upper()
        new_cn_bill_no = f"{prefix}-CN/2025-26/{cn_counter:04d}"
        
        cn_data = original_inv.copy()
        cn_data['bill_no'] = new_cn_bill_no
        cn_data['original_invoice_no'] = bill_no
        cn_data['invoice_date'] = date.today().strftime('%d-%b-%Y')
        cn_data['is_credit_note'] = True
        cn_data['sub_total'] = -abs(original_inv.get('sub_total', 0))
        cn_data['igst'] = -abs(original_inv.get('igst', 0))
        cn_data['cgst'] = -abs(original_inv.get('cgst', 0))
        cn_data['sgst'] = -abs(original_inv.get('sgst', 0))
        cn_data['grand_total'] = -abs(original_inv.get('grand_total', 0))
        cn_data['qtys'] = [-abs(float(q)) for q in original_inv.get('qtys', [])]
        cn_data['amounts'] = [-abs(float(a)) for a in original_inv.get('amounts', [])]
        cn_data['line_tax_amounts'] = [-abs(float(t)) for t in original_inv.get('line_tax_amounts', [])]
        cn_data['line_total_amounts'] = [-abs(float(t)) for t in original_inv.get('line_total_amounts', [])]

        save_single_invoice(cn_data)
        pdf_file = PDF_Generator(cn_data, is_credit_note=True)
        return send_file(pdf_file, mimetype="application/pdf", as_attachment=True, download_name=f"CreditNote_{new_cn_bill_no.replace('/','_')}.pdf")
    except Exception as e:
        logging.error(f"Error CN: {e}"); return jsonify({"error": str(e)}), 500

@app.route('/download-report')
@login_required
def download_excel_report():
    try:
        invoices = load_invoices()
        report_data = []
        for inv in invoices:
            part_list = inv.get('particulars', [])
            hsn_list = inv.get('hsns', [])
            qty_list = inv.get('qtys', [])
            rate_list = inv.get('rates', []) 
            tax_rate_list = inv.get('taxrates', [])
            taxable_list = inv.get('amounts', []) 
            tax_amt_list = inv.get('line_tax_amounts', [])
            total_list = inv.get('line_total_amounts', [])
            for i in range(len(part_list)):
                doc_type = "Tax Invoice"
                if inv.get('is_credit_note'): doc_type = "Credit Note"
                elif inv.get('is_non_gst'): doc_type = "Bill of Supply"
                report_data.append({
                    "Invoice Date": inv.get('invoice_date'),
                    "Bill No": inv.get('bill_no'),
                    "Client Name": inv.get('client_name'),
                    "Client GSTIN": inv.get('client_gstin'),
                    "Item Name": part_list[i] if i < len(part_list) else "",
                    "HSN": hsn_list[i] if i < len(hsn_list) else "",
                    "Qty": float(qty_list[i]) if i < len(qty_list) else 0,
                    "Rate (Incl Tax)": float(rate_list[i]) if i < len(rate_list) else 0,
                    "GST %": float(tax_rate_list[i]) if i < len(tax_rate_list) else 0,
                    "Taxable Value": float(taxable_list[i]) if i < len(taxable_list) else 0,
                    "Tax Amount": float(tax_amt_list[i]) if i < len(tax_amt_list) else 0,
                    "Line Total": float(total_list[i]) if i < len(total_list) else 0,
                    "Doc Type": doc_type
                })
        df = pd.DataFrame(report_data)
        output = io.BytesIO()
        with pd.ExcelWriter(output, engine='openpyxl') as writer:
            df.to_excel(writer, index=False, sheet_name='Sales Register')
        output.seek(0)
        return send_file(output, mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet', as_attachment=True, download_name=f'Sales_Report_{date.today()}.xlsx')
    except Exception as e:
        return f"Error generating report: {str(e)}", 500

if __name__ == "__main__":
    app.run(debug=True, host="0.0.0.0", port=int(os.getenv("PORT",5000)))