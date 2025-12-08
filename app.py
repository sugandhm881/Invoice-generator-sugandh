import os
import io
import json
import logging
import base64
import pandas as pd
from datetime import date, datetime
from urllib.parse import unquote

from flask import Flask, request, send_file, jsonify, render_template, redirect, url_for
from flask_login import LoginManager, UserMixin, login_user, login_required, logout_user, current_user
from dotenv import load_dotenv
from fpdf import FPDF

# --- FIREBASE SETUP ---
import firebase_admin
from firebase_admin import credentials, firestore

# Initialize Firebase using the Environment Variable
if not firebase_admin._apps:
    # Decode the Base64 string back to JSON
    cred_json = json.loads(base64.b64decode(os.getenv("FIREBASE_CREDENTIALS")))
    cred = credentials.Certificate(cred_json)
    firebase_admin.initialize_app(cred)

db = firestore.client()

# ------------------ BASE DIR ------------------
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
CALIBRI_FONT_PATH = os.path.join(BASE_DIR, "CALIBRI.TTF")
SIGNATURE_IMAGE = os.path.join(BASE_DIR, "Signatory.png")
TEMPLATE_DIR = os.path.join(BASE_DIR, "templates")

os.makedirs(os.path.join(BASE_DIR, "generated_invoices"), exist_ok=True)

# ------------------ LOAD ENV ------------------
load_dotenv()

# ------------------ FLASK APP ------------------
app = Flask(__name__, template_folder=TEMPLATE_DIR, static_folder=os.path.join(BASE_DIR, "static"))
app.secret_key = os.getenv("SECRET_KEY", "change_this_secret")

# ------------------ LOGGING ------------------
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

# ------------------ FLASK-LOGIN ------------------
login_manager = LoginManager()
login_manager.init_app(app)
login_manager.login_view = "login"

class User(UserMixin):
    def __init__(self, id):
        self.id = id

AUTH_USERNAME = os.getenv("LOGIN_USER", "admin")
AUTH_PASSWORD = os.getenv("LOGIN_PASS", "password")

@login_manager.user_loader
def load_user(user_id):
    return User(user_id)

# ------------------ FIRESTORE HELPERS ------------------

def load_clients():
    """Fetches all clients from Firestore 'clients' collection."""
    try:
        docs = db.collection('clients').stream()
        clients = {}
        for doc in docs:
            clients[doc.id] = doc.to_dict()
        return clients
    except Exception as e:
        logging.error(f"Error loading clients: {e}")
        return {}

def save_single_client(name, data):
    """Saves a single client to Firestore."""
    try:
        db.collection('clients').document(name).set(data, merge=True)
    except Exception as e:
        logging.error(f"Error saving client {name}: {e}")

def load_invoices():
    """Fetches all invoices from Firestore 'invoices' collection."""
    try:
        docs = db.collection('invoices').stream()
        invoices = []
        for doc in docs:
            invoices.append(doc.to_dict())
        return invoices
    except Exception as e:
        logging.error(f"Error loading invoices: {e}")
        return []

def save_single_invoice(invoice_data):
    """Saves a single invoice using a sanitized Bill No as ID."""
    try:
        # Create a safe document ID (replace slashes with underscores)
        doc_id = invoice_data['bill_no'].replace('/', '_')
        db.collection('invoices').document(doc_id).set(invoice_data)
    except Exception as e:
        logging.error(f"Error saving invoice: {e}")

def load_particulars():
    """Fetches all particulars (items) from Firestore."""
    try:
        docs = db.collection('particulars').stream()
        particulars = {}
        for doc in docs:
            particulars[doc.id] = doc.to_dict()
        return particulars
    except Exception as e:
        logging.error(f"Error loading particulars: {e}")
        return {}

def save_single_particular(name, data):
    """Saves a single particular item."""
    try:
        db.collection('particulars').document(name).set(data, merge=True)
    except Exception as e:
        logging.error(f"Error saving particular {name}: {e}")

def get_next_counter(is_credit_note=False):
    """Atomically increments and retrieves the invoice counter."""
    doc_ref = db.collection('config').document('counters')
    
    @firestore.transactional
    def update_in_transaction(transaction, doc_ref):
        snapshot = doc_ref.get(transaction=transaction)
        if not snapshot.exists:
            # Initialize if not exists
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

# ------------------ NUMBER TO WORDS ------------------
def convert_to_words(number):
    units = ["","One","Two","Three","Four","Five","Six","Seven","Eight","Nine","Ten",
             "Eleven","Twelve","Thirteen","Fourteen","Fifteen","Sixteen","Seventeen","Eighteen","Nineteen"]
    tens = ["","","Twenty","Thirty","Forty","Fifty","Sixty","Seventy","Eighty","Ninety"]
    def two_digit(n):
        return units[n] if n < 20 else tens[n//10] + (" " + units[n%10] if n%10 else "")
    def three_digit(n):
        s = ""
        if n >= 100:
            s += units[n//100] + " Hundred"
            if n % 100: s += " "
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

# ------------------ PDF GENERATION ------------------
def PDF_Generator(invoice_data, is_credit_note=False):
    pdf = FPDF()
    pdf.add_page()
    pdf.add_font("Calibri", "", CALIBRI_FONT_PATH, uni=True)
    pdf.add_font("Calibri", "B", CALIBRI_FONT_PATH, uni=True)

    margin = 15
    page_width = pdf.w - 2 * margin 
    col_width = (page_width / 2) - 5 
    line_height = 5 
    
    # --- Column widths for item table ---
    particulars_w = 60
    hsn_w = 20
    qty_w = 10
    rate_w = 20
    tax_percent_w = 15
    taxable_amt_w = 20
    tax_amt_w = 20
    total_w = 15

    # --- Header ---
    if os.path.exists(os.path.join(BASE_DIR, "static", "logo.png")):
        pdf.image(os.path.join(BASE_DIR, "static", "logo.png"), x=15, y=8, w=30)
    
    pdf.set_font("Calibri", "B", 22)
    
    # --- Determine Mode (Invoice / Credit Note / Non-GST) ---
    is_non_gst = invoice_data.get('is_non_gst', False)

    if is_credit_note:
        pdf.set_text_color(220, 38, 38) # Red
        header_title = "CREDIT NOTE"
    elif is_non_gst:
        pdf.set_text_color(0, 128, 0) # Green
        header_title = "BILL OF SUPPLY"
    else:
        pdf.set_text_color(255, 165, 0) # Orange
        header_title = "TAX INVOICE"

    pdf.cell(page_width, 10, "THE ELEMENT", ln=True, align='C')
    pdf.set_font("Calibri", "B", 14)
    pdf.set_text_color(0, 0, 0)
    pdf.cell(page_width, 8, header_title, ln=True, align='C')
    pdf.set_font("Calibri", "", 10)
    
    my_gstin = invoice_data.get('my_gstin', '06ABOCS1954R1ZG')
    pdf.multi_cell(page_width, line_height, f"Shop 19, AIPL Boulevard, Sector 70A, Gurugram, Haryana - 122101\nPhone: +91-8826382299 | E-mail: customercare@theelement.skin\nGSTIN: {my_gstin}", align='C')
    pdf.ln(5)
    pdf.line(margin, pdf.get_y(), pdf.w - margin, pdf.get_y())

    # --- Credit Note Reference ---
    if is_credit_note:
        pdf.ln(3)
        pdf.set_font("Calibri", "B", 11)
        pdf.set_text_color(220, 38, 38)
        # Strip CN- prefix for cleaner display of reference
        ref_bill = invoice_data.get('original_invoice_no')
        if not ref_bill:
            ref_bill = invoice_data.get('bill_no', '').replace('CN-', '').replace('TE-CN', 'TE')

        pdf.cell(0, 7, f"This is a credit note against Invoice No: {ref_bill}", ln=True, align='C')
        pdf.set_text_color(0, 0, 0)
    
    pdf.ln(5)

    # --- Info Section (Bill To / Ship To) ---
    bill_to_text = (
        f"{invoice_data.get('client_name','')}\n"
        f"{invoice_data.get('client_address1','')}\n"
        f"{invoice_data.get('client_address2','')}\n"
        f"GSTIN: {invoice_data.get('client_gstin','')}\n"
        f"Email: {invoice_data.get('client_email','')}\n"
        f"Mobile: {invoice_data.get('client_mobile','')}"
    )
    
    ship_to_text = (
        f"{invoice_data.get('shipto_name','')}\n"
        f"{invoice_data.get('shipto_address1','')}\n"
        f"{invoice_data.get('shipto_address2','')}\n"
        f"GSTIN: {invoice_data.get('shipto_gstin','')}\n"
        f"Email: {invoice_data.get('shipto_email','')}\n"
        f"Mobile: {invoice_data.get('shipto_mobile','')}"
    )
    
    invoice_no_text = f"{header_title} No: {invoice_data.get('bill_no','')}"
    invoice_date_text = f"Date: {invoice_data.get('invoice_date','')}"
    po_number_text = f"PO Number: {invoice_data.get('po_number','')}"

    # --- Block 1: Bill To (Left) & Invoice Info (Right) ---
    y_start_block1 = pdf.get_y()
    
    # Left
    pdf.set_font("Calibri", "B", 12)
    pdf.cell(col_width, line_height, "Bill To:", ln=True)
    pdf.set_font("Calibri", "", 10)
    pdf.multi_cell(col_width, line_height, bill_to_text)
    y_after_bill_to = pdf.get_y()

    # Right
    pdf.set_y(y_start_block1)
    pdf.set_x(margin + col_width + 10)
    pdf.set_font("Calibri", "B", 10)
    pdf.multi_cell(col_width, line_height, f"{invoice_no_text}\n{invoice_date_text}")
    y_after_invoice_info = pdf.get_y()

    pdf.set_y(max(y_after_bill_to, y_after_invoice_info))
    pdf.ln(5) 
    
    # --- Block 2: Ship To (Left) & PO Number (Right) ---
    y_start_block2 = pdf.get_y()
    
    # Left
    pdf.set_font("Calibri", "B", 12)
    pdf.cell(col_width, line_height, "Ship To:", ln=True)
    pdf.set_font("Calibri", "", 10)
    pdf.multi_cell(col_width, line_height, ship_to_text)
    y_after_ship_to = pdf.get_y()
    
    # Right
    pdf.set_y(y_start_block2)
    pdf.set_x(margin + col_width + 10)
    pdf.set_font("Calibri", "B", 10)
    pdf.multi_cell(col_width, line_height, po_number_text)
    y_after_po = pdf.get_y()

    pdf.set_y(max(y_after_ship_to, y_after_po))
    pdf.ln(10) 
    
    # --- Table Headers ---
    pdf.set_fill_color(255, 204, 153)
    pdf.set_font("Calibri", "B", 10)
    pdf.cell(particulars_w, 8, "Particulars", border=1, align='L', fill=True)
    pdf.cell(hsn_w, 8, "HSN", border=1, align='C', fill=True)
    pdf.cell(qty_w, 8, "Qty", border=1, align='C', fill=True)
    pdf.cell(rate_w, 8, "Rate", border=1, align='R', fill=True)
    pdf.cell(tax_percent_w, 8, "Tax %", border=1, align='R', fill=True)
    pdf.cell(taxable_amt_w, 8, "Taxable Amt", border=1, align='R', fill=True)
    pdf.cell(tax_amt_w, 8, "Tax Amt", border=1, align='R', fill=True)
    pdf.cell(total_w, 8, "Total", border=1, align='R', fill=True)
    pdf.ln()

    # --- Table Rows ---
    pdf.set_font("Calibri", "", 10)
    particulars = invoice_data.get('particulars', [])
    hsns = invoice_data.get('hsns', [])
    qtys = invoice_data.get('qtys', [])
    rates = invoice_data.get('rates', [])
    taxrates = invoice_data.get('taxrates', [])
    amounts = invoice_data.get('amounts', []) 
    line_tax_amounts = invoice_data.get('line_tax_amounts', [])
    line_total_amounts = invoice_data.get('line_total_amounts', [])
    
    n_rows = len(particulars)
    
    for i in range(n_rows):
        start_y = pdf.get_y()
        start_x = pdf.get_x()

        # MultiCell for Item Name
        pdf.multi_cell(particulars_w, 7, str(particulars[i]), border=0, align='L')
        y_after_multi_cell = pdf.get_y()
        row_height = y_after_multi_cell - start_y
        
        # Reset position for other cells
        pdf.set_xy(start_x + particulars_w, start_y)
        
        # HSN Logic: Blank if Non-GST
        display_hsn = "" if is_non_gst else (str(hsns[i]) if i < len(hsns) else '')
        pdf.cell(hsn_w, row_height, display_hsn, border=1, align='C')
        
        # ABS() Logic: Ensure PDF shows positive numbers even if data is negative (CN)
        qty_val = abs(float(qtys[i])) if i < len(qtys) else 0
        rate_val = abs(float(rates[i])) if i < len(rates) else 0
        taxrate_val = abs(float(taxrates[i])) if i < len(taxrates) else 0
        amt_val = abs(float(amounts[i])) if i < len(amounts) else 0
        tax_val = abs(float(line_tax_amounts[i])) if i < len(line_tax_amounts) else 0
        total_val = abs(float(line_total_amounts[i])) if i < len(line_total_amounts) else 0

        pdf.cell(qty_w, row_height, str(qty_val), border=1, align='C')
        pdf.cell(rate_w, row_height, f"{rate_val:.2f}", border=1, align='R')
        pdf.cell(tax_percent_w, row_height, f"{taxrate_val:.2f}%", border=1, align='R')
        pdf.cell(taxable_amt_w, row_height, f"{amt_val:.2f}", border=1, align='R')
        pdf.cell(tax_amt_w, row_height, f"{tax_val:.2f}", border=1, align='R')
        pdf.cell(total_w, row_height, f"{total_val:.2f}", border=1, align='R')

        # Draw border around Particulars cell
        pdf.rect(start_x, start_y, particulars_w, row_height)
        
        # Move to next line
        pdf.set_y(y_after_multi_cell)

    # --- Totals ---
    pdf.set_font("Calibri", "B", 10)
    pdf.set_fill_color(230, 230, 230)
    
    total_label_w = 150
    total_value_w = 30 

    # ABS() Logic for Totals
    sub_total = abs(invoice_data.get('sub_total',0))
    igst = abs(invoice_data.get('igst',0))
    cgst = abs(invoice_data.get('cgst',0))
    sgst = abs(invoice_data.get('sgst',0))
    grand_total = abs(invoice_data.get('grand_total',0))

    pdf.cell(total_label_w, 7, "Sub Total", border=1, align='R', fill=True)
    pdf.cell(total_value_w, 7, f"{sub_total:.2f}", border=1, align='R', fill=True)
    pdf.ln()
    pdf.cell(total_label_w, 7, "IGST", border=1, align='R', fill=True)
    pdf.cell(total_value_w, 7, f"{igst:.2f}", border=1, align='R', fill=True)
    pdf.ln()
    pdf.cell(total_label_w, 7, "CGST", border=1, align='R', fill=True)
    pdf.cell(total_value_w, 7, f"{cgst:.2f}", border=1, align='R', fill=True)
    pdf.ln()
    pdf.cell(total_label_w, 7, "SGST", border=1, align='R', fill=True)
    pdf.cell(total_value_w, 7, f"{sgst:.2f}", border=1, align='R', fill=True)
    pdf.ln()
    pdf.cell(total_label_w, 7, "Grand Total", border=1, align='R', fill=True)
    pdf.cell(total_value_w, 7, f"{grand_total:.2f}", border=1, align='R', fill=True)
    pdf.ln(15)

    # --- Footer ---
    pdf.set_font("Calibri", "", 10)
    pdf.multi_cell(page_width, line_height, f"Rupees: {convert_to_words(grand_total)}\nBank Name: HDFC Bank\nAccount Holder Name: Shifupro Technologies Pvt Ltd.\nAccount No: 50200103384194\nIFSC Code: HDFC0001414")
    pdf.ln(10)
    pdf.set_font("Calibri", "B", 10)
    pdf.cell(0, 5, "For THE ELEMENT", ln=True, align='R')

    if os.path.exists(SIGNATURE_IMAGE):
        pdf.image(SIGNATURE_IMAGE, x=pdf.w - margin - 40, y=pdf.get_y(), w=40) 
    pdf.ln(20)

    pdf_bytes = pdf.output(dest="S").encode("latin-1")
    return io.BytesIO(pdf_bytes)

# ------------------ ROUTES ------------------

@app.route("/", methods=["GET"])
def root():
    if current_user.is_authenticated:
        return redirect(url_for("home"))
    return redirect(url_for("login"))

# --- GET Clients (for Dropdown) ---
@app.route('/clients', methods=['GET'])
@login_required
def get_clients_route():
    return jsonify(load_clients())

@app.route("/login", methods=["GET","POST"])
def login():
    if current_user.is_authenticated: return redirect(url_for("home"))
    error = None
    if request.method=="POST":
        username = request.form.get("username","")
        password = request.form.get("password","")
        if username==AUTH_USERNAME and password==AUTH_PASSWORD:
            user = User(id=username)
            login_user(user)
            return redirect(url_for("home"))
        else:
            error = "Invalid username or password"
    return render_template("login.html", error=error)

@app.route("/home", methods=["GET"])
@login_required
def home():
    return render_template("index.html")

@app.route("/logout")
@login_required
def logout():
    logout_user()
    return redirect(url_for("login"))

# --- GET Particulars (for Dropdown) ---
@app.route('/particulars', methods=['GET'])
@login_required
def get_particulars_route():
    return jsonify(load_particulars())

# --- GENERATE INVOICE (With Smart Logic) ---
@app.route("/generate-invoice", methods=["POST"])
@login_required
def handle_invoice():
    try:
        data = request.json or {}
        
        # 1. Extract Flags
        is_non_gst = data.get('is_non_gst', False)

        # 2. Extract Data
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

        # 3. SAVE PARTICULAR LOGIC (Updated for Firestore)
        for i, item_name in enumerate(particulars):
            if item_name:
                # Store separately so GST items don't overwrite Non-GST items
                storage_key = f"{item_name}_NONGST" if is_non_gst else item_name
                
                hsn_val = "" if is_non_gst else (hsns[i] if i < len(hsns) else "")
                rate_val = rates[i] if i < len(rates) else 0
                tax_val = 0 if is_non_gst else (taxrates[i] if i < len(taxrates) else 0)

                # Save directly to DB (merge=True handles updates)
                save_single_particular(storage_key, {
                    "hsn": hsn_val,
                    "rate": rate_val,
                    "taxrate": tax_val
                })

        # 4. Save Client (Updated for Firestore)
        if client_name:
            client_data = {
                "address1": data.get('client_address1',''),
                "address2": data.get('client_address2',''),
                "gstin": data.get('client_gstin',''),
                "email": data.get('client_email',''),
                "mobile": data.get('client_mobile','')
            }
            save_single_client(client_name, client_data)

        # 5. Handle Invoice Numbering (Updated for Firestore)
        auto_generate = data.get("auto_generate", True)
        if auto_generate:
            # Use the new transactional counter helper
            counter = get_next_counter(is_credit_note=False)
            bill_no = f"TE/2025-26/{counter:04d}"
            invoice_date_str = date.today().strftime('%d-%b-%Y')
        else:
            bill_no = str(data.get("manual_bill_no","")).strip()
            if not bill_no: return jsonify({"error": "Invoice Number required"}), 400
            
            # Simple dup check via existing invoices
            # (In production with many docs, a direct query is better)
            invoices = load_invoices()
            if any(inv['bill_no']==bill_no for inv in invoices): return jsonify({"error": "Duplicate Invoice"}), 409
            
            manual_date = data.get("manual_invoice_date","")
            if manual_date:
                invoice_date_str = datetime.strptime(manual_date, '%Y-%m-%d').strftime('%d-%b-%Y')
            else:
                invoice_date_str = date.today().strftime('%d-%b-%Y')
        
        # 6. Calculations
        my_gstin = "06ABOCS1954R1ZG" 
        my_state_code = my_gstin[:2]
        
        total_igst = 0.0
        total_cgst = 0.0
        total_sgst = 0.0
        line_taxable = []
        line_tax = []
        line_total = []

        for i in range(len(amounts_inclusive)):
            inclusive = float(amounts_inclusive[i])
            
            # Force Tax 0 if Non-GST
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

        final_sub = round(sum(line_taxable), 2)
        final_igst = round(total_igst, 2)
        final_cgst = round(total_cgst, 2)
        final_sgst = round(total_sgst, 2)
        final_grand = round(final_sub + final_igst + final_cgst + final_sgst, 2)

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
            "sub_total": final_sub,
            "igst": final_igst,
            "cgst": final_cgst,
            "sgst": final_sgst,
            "grand_total": final_grand,
            "line_tax_amounts": line_tax,
            "line_total_amounts": line_total
        }
        
        # Save invoice to Firestore
        save_single_invoice(invoice_data)
        
        pdf_file = PDF_Generator(invoice_data)
        download_name = f"Invoice_{bill_no.replace('/','_')}.pdf"
        
        return send_file(pdf_file, mimetype="application/pdf", as_attachment=True, download_name=download_name)
    except Exception as e:
        logging.error(f"Error generating invoice: {e}", exc_info=True)
        return jsonify({"error":str(e)}),500

# --- INVOICES LIST (With Credit Note Flag) ---
@app.route('/invoices-list', methods=['GET'])
@login_required
def invoices_list_route():
    invoices = load_invoices()
    
    # Identify which invoices ALREADY have a Credit Note
    cn_original_refs = set()
    
    for inv in invoices:
        if inv.get('is_credit_note'):
            # New Style: Check the stored reference field
            if inv.get('original_invoice_no'):
                cn_original_refs.add(inv['original_invoice_no'])
            # Old Style Fallback: Check if bill_no starts with CN-
            elif inv['bill_no'].startswith('CN-'):
                original = inv['bill_no'].replace('CN-', '')
                cn_original_refs.add(original)

    brief = []
    for i in invoices:
        # Skip showing Credit Notes in the main dropdown list
        if i.get('is_credit_note'): 
            continue 
            
        brief.append({
            "bill_no": i["bill_no"],
            "date": i.get("invoice_date"),
            "grand_total": i.get("grand_total"),
            "client_name": i.get("client_name"),
            # Check if this bill_no exists in our set of referenced originals
            "has_credit_note": i["bill_no"] in cn_original_refs 
        })
    return jsonify(brief)

@app.route('/download-invoice/<path:bill_no>', methods=['GET'])
@login_required
def download_invoice(bill_no):
    bill_no = unquote(bill_no)
    invoices = load_invoices()
    invoice_data = next((inv for inv in invoices if inv['bill_no']==bill_no),None)
    if not invoice_data: return jsonify({"error":"Invoice not found"}),404
    pdf_file = PDF_Generator(invoice_data)
    return send_file(pdf_file, mimetype="application/pdf", as_attachment=True, download_name=f"Invoice_{bill_no.replace('/','_')}.pdf")

# --- CREDIT NOTE (Logic to Prevent Duplicates & Save Negative) ---
@app.route('/generate-credit-note/<path:bill_no>', methods=['GET'])
@login_required
def generate_credit_note(bill_no):
    try:
        bill_no = unquote(bill_no) # This is the ORIGINAL Invoice No
        invoices = load_invoices()
        
        # 1. Search if a Credit Note ALREADY exists for this specific invoice
        existing_cn = next((inv for inv in invoices if inv.get('original_invoice_no') == bill_no), None)
        
        # Old Style Check (Fallback):
        if not existing_cn:
            possible_old_cn_id = f"CN-{bill_no}"
            existing_cn = next((inv for inv in invoices if inv['bill_no'] == possible_old_cn_id), None)

        if existing_cn:
            # It exists! Download it.
            pdf_file = PDF_Generator(existing_cn, is_credit_note=True)
            download_name = f"CreditNote_{existing_cn['bill_no'].replace('/','_')}.pdf"
            return send_file(pdf_file, mimetype="application/pdf", as_attachment=True, download_name=download_name)

        # 2. Case: Create NEW Credit Note
        original_inv = next((inv for inv in invoices if inv['bill_no'] == bill_no), None)
        if not original_inv: return jsonify({"error": "Original Invoice not found"}), 404

        # 3. Generate NEW SERIES Number (Updated for Firestore)
        cn_counter = get_next_counter(is_credit_note=True)
        new_cn_bill_no = f"TE-CN/2025-26/{cn_counter:04d}"

        cn_data = original_inv.copy()
        cn_data['bill_no'] = new_cn_bill_no
        cn_data['original_invoice_no'] = bill_no # <--- CRITICAL: Save link to original
        cn_data['invoice_date'] = date.today().strftime('%d-%b-%Y')
        cn_data['is_credit_note'] = True
        
        # Negate values (Store as negative for Excel)
        cn_data['sub_total'] = -abs(original_inv.get('sub_total', 0))
        cn_data['igst'] = -abs(original_inv.get('igst', 0))
        cn_data['cgst'] = -abs(original_inv.get('cgst', 0))
        cn_data['sgst'] = -abs(original_inv.get('sgst', 0))
        cn_data['grand_total'] = -abs(original_inv.get('grand_total', 0))
        cn_data['qtys'] = [-abs(float(q)) for q in original_inv.get('qtys', [])]
        cn_data['amounts'] = [-abs(float(a)) for a in original_inv.get('amounts', [])]
        cn_data['line_tax_amounts'] = [-abs(float(t)) for t in original_inv.get('line_tax_amounts', [])]
        cn_data['line_total_amounts'] = [-abs(float(t)) for t in original_inv.get('line_total_amounts', [])]

        # Save CN to Firestore
        save_single_invoice(cn_data)

        pdf_file = PDF_Generator(cn_data, is_credit_note=True)
        download_name = f"CreditNote_{new_cn_bill_no.replace('/','_')}.pdf"
        
        return send_file(pdf_file, mimetype="application/pdf", as_attachment=True, download_name=download_name)

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

                row = {
                    "Invoice Date": inv.get('invoice_date'),
                    "Bill No": inv.get('bill_no'),
                    "Client Name": inv.get('client_name'),
                    "Client GSTIN": inv.get('client_gstin'),
                    "State": "Interstate" if inv.get('client_gstin','').startswith('06') == False else "Haryana",
                    
                    "Item Name": part_list[i] if i < len(part_list) else "",
                    "HSN": hsn_list[i] if i < len(hsn_list) else "",
                    "Qty": float(qty_list[i]) if i < len(qty_list) else 0,
                    "Rate (Incl Tax)": float(rate_list[i]) if i < len(rate_list) else 0,
                    "GST %": float(tax_rate_list[i]) if i < len(tax_rate_list) else 0,
                    
                    "Taxable Value": float(taxable_list[i]) if i < len(taxable_list) else 0,
                    "Tax Amount": float(tax_amt_list[i]) if i < len(tax_amt_list) else 0,
                    "Line Total": float(total_list[i]) if i < len(total_list) else 0,
                    "Doc Type": doc_type
                }
                report_data.append(row)

        df = pd.DataFrame(report_data)
        output = io.BytesIO()
        with pd.ExcelWriter(output, engine='openpyxl') as writer:
            df.to_excel(writer, index=False, sheet_name='Sales Register')
        output.seek(0)
        
        return send_file(
            output, 
            mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet', 
            as_attachment=True, 
            download_name=f'Sales_Report_{date.today()}.xlsx'
        )

    except Exception as e:
        logging.error(f"Error generating Excel: {e}")
        return f"Error generating report: {str(e)}", 500

if __name__ == "__main__":
    app.run(debug=True, host="0.0.0.0", port=int(os.getenv("PORT",5000)))