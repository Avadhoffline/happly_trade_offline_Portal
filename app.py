from flask import Flask, render_template, request, redirect, session, Response
import mysql.connector
import io
import zipfile
from datetime import datetime
import csv

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

MAX_ROWS_PER_FILE = 600000  # safe under Excel limit

# -------------------- LOGIN --------------------
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

# -------------------- CHANGE PORT --------------------
@app.route('/change_port')
def change_port():
    session.pop('user', None)
    session.pop('port_type', None)
    session.pop('hs_code', None)
    return redirect('/')

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

        # -------------------- CREATE ZIP WITH CSV FILES --------------------
        zip_buffer = io.BytesIO()
        zip_file = zipfile.ZipFile(zip_buffer, mode='w', compression=zipfile.ZIP_DEFLATED)

        file_count = 1
        row_count = 0
        columns = [col[0] for col in cursor.description]
        csv_rows = []

        for row in cursor:
            formatted_row = []
            for col in columns:
                val = row[col]
                if isinstance(val, (int, float)):
                    formatted_row.append(f"{val:,}")
                elif isinstance(val, datetime):
                    formatted_row.append(val.strftime("%Y-%m-%d"))
                else:
                    formatted_row.append(val if val is not None else '')
            csv_rows.append(formatted_row)
            row_count += 1

            # SPLIT FILE
            if row_count >= MAX_ROWS_PER_FILE:
                csv_buffer = io.StringIO()
                writer = csv.writer(csv_buffer)
                writer.writerow(columns)
                writer.writerows(csv_rows)
                zip_file.writestr(f"data_part_{file_count}.csv", csv_buffer.getvalue())
                file_count += 1
                row_count = 0
                csv_rows = []

        # LAST FILE
        if row_count > 0:
            csv_buffer = io.StringIO()
            writer = csv.writer(csv_buffer)
            writer.writerow(columns)
            writer.writerows(csv_rows)
            zip_file.writestr(f"data_part_{file_count}.csv", csv_buffer.getvalue())

        zip_file.close()
        zip_buffer.seek(0)
        filename = f"{user_hs_code}_{port_type}.zip"

        # LOG DOWNLOAD
        if 'downloads' not in session:
            session['downloads'] = []
        session['downloads'].append({
            'filename': filename,
            'timestamp': datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        })

        cursor.close()
        conn.close()

        return Response(
            zip_buffer,
            mimetype='application/zip',
            headers={"Content-Disposition": f"attachment; filename={filename}"}
        )

    return render_template(
        'dashboard.html',
        user_port_type=port_type,
        user_hs_code=user_hs_code,
        downloads=session.get('downloads', [])
    )

# -------------------- LOGOUT --------------------
@app.route('/logout')
def logout():
    session.clear()
    return redirect('/')

# -------------------- RUN --------------------
if __name__ == '__main__':
    app.run(host="0.0.0.0", port=5000, debug=True)
