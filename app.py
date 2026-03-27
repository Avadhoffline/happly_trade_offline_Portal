from flask import Flask, render_template, request, redirect, send_file, session
import mysql.connector
import pandas as pd
import io
from datetime import datetime

app = Flask(__name__)
app.secret_key = "supersecretkey123"

# -------------------- DATABASE CONFIG --------------------
db_config = {
    'host': '192.168.1.22',
    'port': '3306',
    'user': 'avadh',
    'password': 'Avadh!@#123',
    'database': 'test'
}

# -------------------- LOGIN PAGE --------------------
@app.route('/', methods=['GET', 'POST'])
def login():
    error = None
    if request.method == 'POST':
        email = request.form['email'].strip()
        password = request.form['password'].strip()

        conn = None
        cursor = None
        try:
            conn = mysql.connector.connect(**db_config)
            cursor = conn.cursor(dictionary=True)
            cursor.execute(
                "SELECT DISTINCT Email, Password, HsCode, PortType FROM Users WHERE Email=%s AND Password=%s",
                (email, password)
            )
            users = cursor.fetchall()
        except Exception as e:
            error = f"Database error: {e}"
            users = []
        finally:
            if cursor:
                cursor.close()
            if conn:
                conn.close()

        if users:
            session['users'] = users
            if len(users) == 1:
                session['user'] = users[0]['Email']
                session['port_type'] = users[0]['PortType']
                session['hs_code'] = users[0]['HsCode']
                return redirect('/dashboard')
            else:
                return render_template('choose_port.html', users=users)
        else:
            if not error:
                error = "Invalid Email or Password"

    return render_template('login.html', error=error)

# -------------------- SELECT PORT --------------------
@app.route('/select_port', methods=['POST'])
def select_port():
    try:
        index = int(request.form['port_selection'])
        selected_user = session['users'][index]

        session['user'] = selected_user['Email']
        session['port_type'] = selected_user['PortType']
        session['hs_code'] = selected_user['HsCode']
    except Exception as e:
        return f"Error selecting port: {e}"

    return redirect('/dashboard')

# -------------------- DASHBOARD PAGE --------------------
@app.route('/dashboard', methods=['GET', 'POST'])
def dashboard():
    if 'user' not in session:
        return redirect('/')

    user_hs_code = str(session['hs_code'])
    port_type = session['port_type'].strip().lower().replace(" ", "_")  # normalize

    table_mapping = {
        "import": "Monthly_import_off_1to31th_Jan26",
        "export": "Monthly_Export_Offline_Jan26",
        "sez_import": "SEZ_I_Off_Jan26",
        "sez_export": "Sez_E_Off_jan26"
    }

    if port_type not in table_mapping:
        return f"Access Denied: Invalid PortType ({port_type})"

    table_name = table_mapping[port_type]

    if request.method == 'POST':
        hs_code_input = request.form.get('hs_code', '').strip()
        if hs_code_input and not hs_code_input.startswith(user_hs_code):
            return "Kindly enter a valid HS Code"
        hs_filter = f"{hs_code_input or user_hs_code}%"

        conn = None
        try:
            conn = mysql.connector.connect(**db_config)
            query = f"SELECT * FROM `{table_name}` WHERE `HS Code` LIKE %s"
            df = pd.read_sql_query(query, conn, params=[hs_filter])
        except Exception as e:
            return f"Database error: {e}"
        finally:
            if conn:
                conn.close()

        if df.empty:
            return f"No data found for HS Code: {hs_filter}"

        # -------------------- EXPORT TO EXCEL --------------------
        try:
            output = io.BytesIO()
            df.to_excel(output, index=False, engine='openpyxl')
            output.seek(0)
        except Exception as e:
            return f"Error generating Excel file: {e}"

        # -------------------- DYNAMIC FILE NAME BASED ON TABLE --------------------
        hs_code_for_file = hs_code_input if hs_code_input else user_hs_code
        month_in_table = table_name.split("_")[-1]  # Extract last part of table name for month
        filename = f"{hs_code_for_file}_{port_type}_{month_in_table}.xlsx"

        # -------------------- LOG DOWNLOAD --------------------
        if 'downloads' not in session:
            session['downloads'] = []

        session['downloads'].append({
            'hs_code': hs_code_for_file,
            'port_type': port_type,
            'timestamp': datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            'filename': filename
        })

        return send_file(
            output,
            download_name=filename,
            as_attachment=True
        )

    # GET request: render dashboard
    return render_template(
        'dashboard.html',
        user_port_type=port_type,
        user_hs_code=user_hs_code,
        downloads=session.get('downloads', [])
    )

# -------------------- DOWNLOAD HISTORY ROUTE --------------------
@app.route('/download/<filename>')
def download_file(filename):
    if 'downloads' not in session:
        return redirect('/dashboard')

    # Find the download record
    record = next((d for d in session['downloads'] if d['filename'] == filename), None)
    if not record:
        return "File not found in download history."

    port_type = record['port_type']
    hs_code = record['hs_code']
    table_mapping = {
        "import": "Monthly_import_off_1to31th_Jan26",
        "export": "Monthly_Export_Offline_Jan26",
        "sez_import": "SEZ_I_Off_Jan26",
        "sez_export": "Sez_E_Off_jan26"
    }
    table_name = table_mapping.get(port_type)
    if not table_name:
        return "Invalid PortType."

    hs_filter = f"{hs_code}%"
    conn = mysql.connector.connect(**db_config)
    df = pd.read_sql_query(f"SELECT * FROM `{table_name}` WHERE `HS Code` LIKE %s", conn, params=[hs_filter])
    conn.close()

    output = io.BytesIO()
    df.to_excel(output, index=False, engine='openpyxl')
    output.seek(0)

    return send_file(output, download_name=filename, as_attachment=True)

# -------------------- CHANGE PORT --------------------
@app.route('/change_port')
def change_port():
    if 'users' not in session:
        return redirect('/')
    return render_template('choose_port.html', users=session['users'])

# -------------------- LOGOUT --------------------
@app.route('/logout')
def logout():
    session.clear()
    return redirect('/')

# ==================== RUN APP ====================
if __name__ == '__main__':
    app.run(host="0.0.0.0", port=5000, debug=True)