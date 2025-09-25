# app.py

import os
import io
import json
import logging
from datetime import date, datetime
from urllib.parse import unquote

from flask import Flask, request, send_file, jsonify, render_template, redirect, url_for
from flask_login import LoginManager, UserMixin, login_user, login_required, logout_user, current_user
from dotenv import load_dotenv
from fpdf import FPDF

# ------------------ BASE DIR ------------------
BASE_DIR = os.path.dirname(os.path.abspath(__file__))

CLIENTS_FILE = os.path.join(BASE_DIR, "clients.json")
INVOICES_FILE = os.path.join(BASE_DIR, "invoices.json")
INVOICE_COUNTER_FILE = os.path.join(BASE_DIR, "invoice_counter.json")
PARTICULARS_FILE = os.path.join(BASE_DIR, "particulars.json")
SIGNATURE_IMAGE = os.path.join(BASE_DIR, "Signatory.jpg")
CALIBRI_FONT_PATH = os.path.join(BASE_DIR, "CALIBRI.TTF")
INVOICE_PDF_FOLDER = os.path.join(BASE_DIR, "generated_invoices")
TEMPLATE_DIR = os.path.join(BASE_DIR, "templates")

os.makedirs(INVOICE_PDF_FOLDER, exist_ok=True)

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

# ------------------ HELPERS ------------------
def load_json(file_path, default):
    if os.path.exists(file_path):
        try:
            with open(file_path, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception as e:
            logging.error(f"Error reading {file_path}: {e}")
            return default
    return default

def save_json(data, file_path):
    with open(file_path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=4, ensure_ascii=False)

def load_clients():
    return load_json(CLIENTS_FILE, {})

def save_clients(clients):
    save_json(clients, CLIENTS_FILE)

def load_invoices():
    return load_json(INVOICES_FILE, [])

def save_invoices(invoices):
    save_json(invoices, INVOICES_FILE)
    
def load_particulars():
    return load_json(PARTICULARS_FILE, {})

def save_particulars(particulars):
    save_json(particulars, PARTICULARS_FILE)


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
def generate_pdf(invoice_data):
    pdf = FPDF()
    pdf.add_page()
    pdf.add_font("Calibri", "", CALIBRI_FONT_PATH, uni=True)
    pdf.add_font("Calibri", "B", CALIBRI_FONT_PATH, uni=True)

    margin = 15
    page_width = pdf.w - 2 * margin

    # Header
    if os.path.exists(os.path.join(BASE_DIR, "static", "mb-logo.png")):
        pdf.image(os.path.join(BASE_DIR, "static", "mb-logo.png"), x=15, y=8, w=30)
    pdf.set_font("Calibri", "B", 22)
    pdf.set_text_color(255, 165, 0)  # Orange
    pdf.cell(page_width, 10, "MB COLLECTION", ln=True, align='C')
    pdf.set_font("Calibri", "B", 14)
    pdf.set_text_color(0, 0, 0)
    pdf.cell(page_width, 8, "Tax Invoice", ln=True, align='C')
    pdf.set_font("Calibri", "", 10)
    pdf.multi_cell(page_width, 5, "H.No 3A Shri Krishana Vatika, Sudamapuri, Vijaynagar, Ghaziabad, Uttar Pradesh - 201001\nPhone: +91-8651537856 | E-mail: skpa.avkashmishra@gmail.com\nGSTIN: 09ENEPM4809Q1Z8", align='C')
    pdf.ln(5)
    pdf.line(margin, pdf.get_y(), pdf.w - margin, pdf.get_y())

    # Bill To
    pdf.ln(5)
    pdf.set_font("Calibri", "B", 12)
    pdf.cell(0, 5, "Bill To:", ln=True)
    pdf.set_font("Calibri", "", 10)
    pdf.multi_cell(0, 5, f"{invoice_data['client_name']}\n{invoice_data['client_address1']}\n{invoice_data['client_address2']}\nGSTIN: {invoice_data['client_gstin']}")
    pdf.set_xy(140, 65)
    pdf.set_font("Calibri", "B", 10)
    pdf.cell(0, 5, f"Invoice No: {invoice_data['bill_no']}", ln=True)
    pdf.set_x(140)
    pdf.cell(0, 5, f"Date: {invoice_data['invoice_date']}", ln=True)

    pdf.ln(10)
    pdf.set_fill_color(255, 204, 153)  # Light orange header
    pdf.set_font("Calibri", "B", 10)
    pdf.cell(130, 8, "Particulars", border=1, align='C', fill=True)
    pdf.cell(30, 8, "HSN", border=1, align='C', fill=True)
    pdf.cell(30, 8, "Amount", border=1, align='C', fill=True)
    pdf.ln()

    # Table rows
    pdf.set_font("Calibri", "", 10)
    particulars = [p.strip() for p in invoice_data['particulars'].split('\n') if p.strip()]
    hsn_list = ["998222"] * len(particulars)
    
    amounts = invoice_data.get('amounts')
    if not amounts or len(amounts) != len(particulars):
        total_amount = invoice_data.get('amount', 0)
        amounts = [total_amount / len(particulars)] * len(particulars) if particulars else []

    line_height = 7 # A consistent height for text lines
    for i in range(len(particulars)):
        if i % 2 == 0:
            pdf.set_fill_color(255, 255, 204)
        else:
            pdf.set_fill_color(255, 255, 230)
            
        # --- FIX START: Correct Vertical Alignment for All Cells ---
        start_x = pdf.get_x()
        start_y = pdf.get_y()
        
        # Draw particulars text (invisibly) to calculate the required row height
        # This is a common FPDF workaround
        pdf.multi_cell(130, line_height, particulars[i], border=0, align='L', fill=False)
        final_y = pdf.get_y()
        row_height = final_y - start_y
        
        # Go back to the starting position to draw the actual cells with borders and fill
        pdf.set_xy(start_x, start_y)
        pdf.cell(130, row_height, "", border=1, fill=True)
        pdf.cell(30, row_height, "", border=1, fill=True)
        pdf.cell(30, row_height, "", border=1, fill=True)
        
        # Go back again to draw the text on top of the colored boxes, ensuring top alignment
        pdf.set_xy(start_x, start_y)
        pdf.multi_cell(130, line_height, particulars[i], align='L', fill=False)
        
        pdf.set_xy(start_x + 130, start_y)
        pdf.multi_cell(30, line_height, hsn_list[i], align='C', fill=False)
        
        pdf.set_xy(start_x + 130 + 30, start_y)
        pdf.multi_cell(30, line_height, f"{amounts[i]:.2f}", align='R', fill=False)
        
        # Move the cursor to the correct position for the next row
        pdf.set_y(final_y)
        # --- FIX END ---

    # Totals
    pdf.set_font("Calibri", "B", 10)
    pdf.set_fill_color(230, 230, 230)
    pdf.cell(160, 7, "Sub Total", border=1, align='R', fill=True)
    pdf.cell(30, 7, f"{invoice_data['sub_total']:.2f}", border=1, align='R', fill=True)
    pdf.ln()
    pdf.cell(160, 7, "IGST @18%", border=1, align='R', fill=True)
    pdf.cell(30, 7, f"{invoice_data['igst']:.2f}", border=1, align='R', fill=True)
    pdf.ln()
    pdf.cell(160, 7, "CGST @9%", border=1, align='R', fill=True)
    pdf.cell(30, 7, f"{invoice_data['cgst']:.2f}", border=1, align='R', fill=True)
    pdf.ln()
    pdf.cell(160, 7, "SGST @9%", border=1, align='R', fill=True)
    pdf.cell(30, 7, f"{invoice_data['sgst']:.2f}", border=1, align='R', fill=True)
    pdf.ln()
    pdf.cell(160, 7, "Grand Total", border=1, align='R', fill=True)
    pdf.cell(30, 7, f"{invoice_data['grand_total']:.2f}", border=1, align='R', fill=True)
    pdf.ln(15)

    # Amount in words & bank details
    pdf.set_font("Calibri", "", 10)
    pdf.multi_cell(0, 5, f"Rupees: {convert_to_words(invoice_data['grand_total'])}\nBank Name: Yes Bank\nAccount Holder Name: MB Collection\nAccount No: 003861900014956\nIFSC Code: YESB0000038")
    pdf.ln(10)
    pdf.set_font("Calibri", "B", 10)
    pdf.cell(0, 5, "For MB COLLECTION", ln=True, align='R')

    if os.path.exists(SIGNATURE_IMAGE):
        pdf.image(SIGNATURE_IMAGE, x=150, y=pdf.get_y(), w=40)
    pdf.ln(20)

    pdf_bytes = pdf.output(dest="S").encode("latin-1")
    return io.BytesIO(pdf_bytes)


# ------------------ ROUTES ------------------

@app.route("/", methods=["GET"])
def root():
    if current_user.is_authenticated:
        return redirect(url_for("home"))
    return redirect(url_for("login"))

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

# ------------------ INVOICE ROUTES ------------------

@app.route("/generate-invoice", methods=["POST"])
@login_required
def handle_invoice():
    try:
        data = request.json or {}
        client_name = data.get('client_name','').strip()
        client_address1 = data.get('client_address1','').strip()
        client_address2 = data.get('client_address2','').strip()
        client_gstin = data.get('client_gstin','').strip()
        particulars = data.get('particulars', [])
        if isinstance(particulars, list):
            particulars = '\n'.join([str(p).strip() for p in particulars])
        else:
            particulars = str(particulars).strip()  

        amounts = data.get("amounts")
        if amounts and isinstance(amounts,list): 
            amount = float(sum([float(x) for x in amounts if x is not None]))
        else: 
            amount = float(data.get("amount",0))

        # Save client
        clients = load_clients()
        if client_name and client_name not in clients:
            clients[client_name] = {"address1":client_address1,"address2":client_address2,"gstin":client_gstin}
            save_clients(clients)

        # Determine auto or manual invoice number/date
        auto_generate = data.get("auto_generate", True)
        if auto_generate:
            counter_data = load_json(INVOICE_COUNTER_FILE, {"counter":0})
            counter = counter_data.get("counter",0)+1
            counter_data['counter']=counter
            save_json(counter_data, INVOICE_COUNTER_FILE)
            bill_no = f"INV/{counter:04d}/25-26"
            invoice_date_str = date.today().strftime('%d-%b-%Y')
        else:
            bill_no = str(data.get("manual_bill_no","")).strip() or f"INV/XXXX/25-26"
            manual_date = str(data.get("manual_invoice_date","")).strip()
            if manual_date:
                # Convert YYYY-MM-DD from input[type=date] to DD-Mon-YYYY
                invoice_date_str = datetime.strptime(manual_date, '%Y-%m-%d').strftime('%d-%b-%Y')
            else:
                invoice_date_str = date.today().strftime('%d-%b-%Y')

        my_gstin = "09ENEPM4809Q1Z8"
        sub_total = round(amount,2)
        igst=cgst=sgst=0.0
        if client_gstin and client_gstin[:2]==my_gstin[:2]: 
            cgst=round(sub_total*0.09,2); sgst=round(sub_total*0.09,2)
        else: 
            igst=round(sub_total*0.18,2)
        grand_total = round(sub_total+igst+cgst+sgst,2)

        invoice_data = {
            "bill_no":bill_no,
            "invoice_date":invoice_date_str,
            "client_name":client_name,
            "client_address1":client_address1,
            "client_address2":client_address2,
            "client_gstin":client_gstin,
            "my_gstin":my_gstin,
            "particulars":particulars,
            "amount":sub_total,
            "amounts":amounts if isinstance(amounts,list) else None,
            "sub_total":sub_total,
            "igst":igst,
            "cgst":cgst,
            "sgst":sgst,
            "grand_total":grand_total
        }

        invoices = load_invoices()
        invoices.append(invoice_data)
        save_invoices(invoices)

        pdf_file = generate_pdf(invoice_data)
        download_name = f"Invoice_{bill_no.replace('/','_')}.pdf"
        
        # Save the file to the generated_invoices folder
        path = os.path.join(INVOICE_PDF_FOLDER, download_name)
        with open(path,"wb") as f: 
            f.write(pdf_file.getbuffer())
        pdf_file.seek(0) # Reset buffer position after writing
            
        return send_file(pdf_file, mimetype="application/pdf", as_attachment=True, download_name=download_name)
    except Exception as e:
        logging.error(f"Error generating invoice: {e}", exc_info=True)
        return jsonify({"error":str(e)}),500

@app.route('/clients', methods=['GET'])
@login_required
def get_clients_route():
    return jsonify(load_clients())

@app.route('/invoices-list', methods=['GET'])
@login_required
def invoices_list_route():
    invoices = load_invoices()
    brief = [{"bill_no":i["bill_no"],"date":i.get("invoice_date"),"grand_total":i.get("grand_total"),"client_name":i.get("client_name")} for i in invoices]
    return jsonify(brief)

@app.route('/download-invoice/<path:bill_no>', methods=['GET'])
@login_required
def download_invoice(bill_no):
    bill_no = unquote(bill_no)
    invoices = load_invoices()
    invoice_data = next((inv for inv in invoices if inv['bill_no']==bill_no),None)
    if not invoice_data: return jsonify({"error":"Invoice not found"}),404
    pdf_file = generate_pdf(invoice_data)
    download_name = f"Invoice_{bill_no.replace('/','_')}.pdf"
    return send_file(pdf_file,mimetype="application/pdf",as_attachment=True,download_name=download_name)

# ------------------ RUN ------------------
if __name__ == "__main__":
    app.run(debug=True, host="0.0.0.0", port=int(os.getenv("PORT",5000)))