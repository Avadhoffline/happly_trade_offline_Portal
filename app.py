from flask import Flask, render_template, request, redirect, send_file, session, Response
import mysql.connector
import pandas as pd
import io
from datetime import datetime

app = Flask(__name__)
app.secret_key = "supersecretkey123"

# -------------------- DATABASE CONFIG --------------------
db_config = {
    'host': '122.180.251.28',
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
            cursor.close()
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
    index = int(request.form['port_selection'])
    selected_user = session['users'][index]

    session['user'] = selected_user['Email']
    session['port_type'] = selected_user['PortType']
    session['hs_code'] = selected_user['HsCode']

    return redirect('/dashboard')

# -------------------- DASHBOARD --------------------
@app.route('/dashboard', methods=['GET', 'POST'])
def dashboard():
    if 'user' not in session:
        return redirect('/')

    user_hs_code = str(session['hs_code'])
    port_type = session['port_type'].strip().lower().replace(" ", "_")

    table_mapping = {
        "import": "Monthly_import_off_1to31th_Jan26",
        "export": "Monthly_Export_Offline_Jan26",
        "sez_import": "SEZ_I_Off_Jan26",
        "sez_export": "Sez_E_Off_jan26"
    }

    if port_type not in table_mapping:
        return "Invalid PortType"

    table_name = table_mapping[port_type]

    if request.method == 'POST':
        hs_code_input = request.form.get('hs_code', '').strip()

        if hs_code_input and not hs_code_input.startswith(user_hs_code):
            return "Invalid HS Code"

        hs_filter = f"{hs_code_input or user_hs_code}%"

        conn = mysql.connector.connect(**db_config)
        cursor = conn.cursor(dictionary=True)

        query = f"SELECT * FROM `{table_name}` WHERE `HS Code` LIKE %s"
        cursor.execute(query, (hs_filter,))

        # -------------------- STREAMING CSV --------------------
        def generate():
            # Header
            columns = [col[0] for col in cursor.description]
            yield ','.join(columns) + '\n'

            row_count = 0
            MAX_ROWS = 500000  # safety limit

            for row in cursor:
                row_count += 1

                if row_count > MAX_ROWS:
                    yield "\n--- DATA LIMIT REACHED (500000 rows) ---"
                    break

                yield ','.join(str(v) if v is not None else '' for v in row.values()) + '\n'

        # -------------------- FILE NAME --------------------
        hs_code_for_file = hs_code_input if hs_code_input else user_hs_code
        filename = f"{hs_code_for_file}_{port_type}.csv"

        # -------------------- LOG DOWNLOAD --------------------
        if 'downloads' not in session:
            session['downloads'] = []

        session['downloads'].append({
            'hs_code': hs_code_for_file,
            'port_type': port_type,
            'timestamp': datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            'filename': filename
        })

        return Response(
            generate(),
            mimetype='text/csv',
            headers={"Content-Disposition": f"attachment; filename={filename}"}
        )

    return render_template(
        'dashboard.html',
        user_port_type=port_type,
        user_hs_code=user_hs_code,
        downloads=session.get('downloads', [])
    )

# -------------------- DOWNLOAD HISTORY --------------------
@app.route('/download/<filename>')
def download_file(filename):
    if 'downloads' not in session:
        return redirect('/dashboard')

    record = next((d for d in session['downloads'] if d['filename'] == filename), None)

    if not record:
        return "File not found"

    port_type = record['port_type']
    hs_code = record['hs_code']

    table_mapping = {
        "import": "Monthly_import_off_1to31th_Jan26",
        "export": "Monthly_Export_Offline_Jan26",
        "sez_import": "SEZ_I_Off_Jan26",
        "sez_export": "Sez_E_Off_jan26"
    }

    table_name = table_mapping.get(port_type)

    conn = mysql.connector.connect(**db_config)
    cursor = conn.cursor(dictionary=True)

    query = f"SELECT * FROM `{table_name}` WHERE `HS Code` LIKE %s"
    cursor.execute(query, (f"{hs_code}%",))

    def generate():
        columns = [col[0] for col in cursor.description]
        yield ','.join(columns) + '\n'

        for row in cursor:
            yield ','.join(str(v) if v else '' for v in row.values()) + '\n'

    return Response(
        generate(),
        mimetype='text/csv',
        headers={"Content-Disposition": f"attachment; filename={filename}"}
    )

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

# -------------------- RUN --------------------
if __name__ == '__main__':
    app.run(host="0.0.0.0", port=5000, debug=True)
