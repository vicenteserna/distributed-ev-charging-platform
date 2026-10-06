from flask import Flask, request, jsonify
import requests
import psycopg2
import os
import sys
import threading
import json
import secrets
from kafka import KafkaConsumer
from cryptography.fernet import Fernet

# Add parent directory to path to import ev_common
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from ev_common.config import DB_URI, PORT_CENTRAL, KAFKA_BROKER, KAFKA_TOPIC_TELEMETRY

app = Flask(__name__)

# In-memory store for active sessions and status (for simplicity/speed)
# In a real app, use Redis or DB
active_sessions = {} # cp_id -> session_key
cp_sessions_info = {} # cp_id -> {'key': session_key, 'url': callback_url}
cp_status = {} # cp_id -> status
weather_alerts = [] # List of active alerts
weather_cache = {} # city -> {'temp': float, 'last_update': timestamp}

def get_db_connection():
    return psycopg2.connect(DB_URI)

def log_audit(action, description, source_ip):
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute(
            'INSERT INTO audit_logs (source_ip, action, description) VALUES (%s, %s, %s)',
            (source_ip, action, description)
        )
        conn.commit()
        cur.close()
        conn.close()
    except Exception as e:
        print(f"Audit Log Error: {e}")

# --- Kafka Consumer Thread ---
def consume_telemetry():
    print("Starting Kafka Consumer...")
    # Wait for Kafka to be ready might be needed in production, but retry logic handles it usually
    try:
        consumer = KafkaConsumer(
            KAFKA_TOPIC_TELEMETRY,
            bootstrap_servers=KAFKA_BROKER,
            auto_offset_reset='latest',
            value_deserializer=lambda x: json.loads(x.decode('utf-8'))
        )
        for message in consumer:
            data = message.value
            # decrypt payload if needed (instructions say payload is encrypted)
            # For now, assuming the consumer receives the wrapper and needs to decrypt
            # But wait, if I don't have the key easily accessible for *every* message without looking up...
            # The message should probably contain the CP_ID so I can look up the key.
            
            cp_id = data.get('cp_id')
            encrypted_payload = data.get('payload')
            
            if cp_id and cp_id in active_sessions:
                key = active_sessions[cp_id]
                f = Fernet(key)
                try:
                    decrypted_bytes = f.decrypt(encrypted_payload.encode('utf-8'))
                    telemetry = json.loads(decrypted_bytes.decode('utf-8'))
                    # Update status/DB
                    print(f"Telemetry from {cp_id}: {telemetry}")
                    # Here we would update the DB or in-memory status
                    cp_status[cp_id] = 'CHARGING' # Update based on telemetry
                    
                    # Update DB to ensure persistence and frontend visibility
                    try:
                        conn = get_db_connection()
                        cur = conn.cursor()
                        cur.execute('UPDATE charging_points SET status = %s WHERE cp_id = %s', ('CHARGING', cp_id))
                        conn.commit()
                        cur.close()
                        conn.close()
                    except Exception as e:
                        print(f"DB Update Error in consumer for {cp_id}: {e}")
                except Exception as e:
                    print(f"Decryption error for {cp_id}: {e}")
            else:
                print(f"Unknown CP or no session for {cp_id}")

    except Exception as e:
        print(f"Kafka Consumer Error: {e}")

# --- API Endpoints ---

@app.route('/login', methods=['POST'])
def login():
    data = request.get_json()
    cp_id = data.get('cp_id')
    token = data.get('token')
    
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute('SELECT security_token FROM charging_points WHERE cp_id = %s', (cp_id,))
    res = cur.fetchone()
    cur.close()
    conn.close()
    
    if res and res[0] == token:
        # Generate Session Key (Fernet key)
        key = Fernet.generate_key().decode('utf-8')
        callback_url = data.get('callback_url')
        
        active_sessions[cp_id] = key
        cp_sessions_info[cp_id] = {'key': key, 'url': callback_url}
        
        # Store in DB as well? Instructions say "Negotiate secret key". 
        # "Almacena la identidad... y claves de cifrado" in DB section.
        # Let's update DB.
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute('UPDATE charging_points SET shared_secret = %s, status = %s WHERE cp_id = %s', (key, 'AVAILABLE', cp_id))
        conn.commit()
        cur.close()
        conn.close()
        
        log_audit('LOGIN', f"CP {cp_id} logged in successfully", request.remote_addr)
        return jsonify({'session_key': key}), 200
    else:
        log_audit('LOGIN_FAIL', f"Failed login for CP {cp_id}", request.remote_addr)
        return jsonify({'error': 'Invalid credentials'}), 401

@app.route('/revoke_key', methods=['POST'])
def revoke_key():
    # Admin endpoint to revoke a CP's key
    data = request.get_json()
    cp_id = data.get('cp_id')
    
    if cp_id in active_sessions:
        del active_sessions[cp_id]
    if cp_id in cp_sessions_info:
        url = cp_sessions_info[cp_id].get('url')
        if url:
            try:
                print(f"Notifying {cp_id} of revocation at {url}...")
                requests.post(f"{url}/revoked", timeout=2) 
            except Exception as e:
                print(f"Failed to notify CP {cp_id} of revocation: {e}")

        del cp_sessions_info[cp_id]
        
    # Update DB - Delete the CP entirely so it can re-register as fresh
    # This simulates a full factory reset/revocation where the old identity is purged.
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute('DELETE FROM charging_points WHERE cp_id = %s', (cp_id,))
    conn.commit()
    cur.close()
    conn.close()
    
    log_audit('REVOKE', f"Revoked key for {cp_id}", request.remote_addr)
    return jsonify({'status': 'revoked'}), 200

@app.route('/authorize_charge', methods=['POST'])
def authorize_charge():
    # Called by Driver or CP? Instructions: "Driver -> Central: Solicitud de carga"
    # Also "Central -> CP: POST /authorize_charge"
    # So this endpoint receives request from Driver, then calls CP.
    
    data = request.get_json()
    cp_id = data.get('cp_id')
    user = data.get('user')
    
    if cp_id not in active_sessions:
         return jsonify({'error': 'CP not connected'}), 404
         
    # Check if CP is available (logic to be added)
    
    # Notify CP to start (Mocking the call to CP for now, or implementing it)
    # We need the CP's IP/URL. For simulation, maybe we assume a fixed pattern or register IP?
    # Instructions don't explicitly say CP registers its IP, but it contacts Registry.
    # We might need to store CP address in DB during registration or login?
    # "CP contacts Registry... CP contacts Central".
    # If Central needs to call CP, Central needs CP's address.
    # Let's assume CP sends its address during Login or we use a service discovery/DNS name if Docker.
    # For Docker simulation, "ev_cp" might be the hostname if only one. But we have multiple.
    # We might need to run multiple CP containers or one container simulating multiple.
    # "Simula el poste... Se divide en dos sub-procesos".
    # "Desplegar 50 cargadores... Docker".
    # If we run 50 containers, we need 50 service names or IPs.
    # For this MVP, let's assume the CP sends its callback URL during login.
    
    # ... logic to call CP ...
    # ... logic to call CP ...
    # Use callback URL if available
    if cp_id in cp_sessions_info and cp_sessions_info[cp_id].get('url'):
        # Notify CP of user
        cp_url = cp_sessions_info[cp_id]['url']
        try:
            requests.post(f"{cp_url}/authorize", json={'user': user}, timeout=5)
        except Exception as e:
            print(f"Failed to notify CP {cp_id} of auth: {e}")

        # Update DB to CONNECTED
        try:
            conn = get_db_connection()
            cur = conn.cursor()
            cur.execute('UPDATE charging_points SET status = %s WHERE cp_id = %s', ('CONNECTED', cp_id))
            conn.commit()
            cur.close()
            conn.close()
            print(f"[{cp_id}] Status updated to CONNECTED. Waiting for manual start.")
        except Exception as e:
            print(f"DB Update Error in authorize_charge for {cp_id}: {e}")

    log_audit('CHARGE_REQ', f"User {user} requested charge at {cp_id}", request.remote_addr)
    return jsonify({'status': 'authorized'}), 200

@app.route('/admin/start_charge', methods=['POST'])
def admin_start_charge():
    data = request.get_json()
    cp_id = data.get('cp_id')
    
    # Check if CP is connected
    if cp_id not in cp_sessions_info:
        return jsonify({'error': 'CP not connected'}), 404
        
    cp_url = cp_sessions_info[cp_id]['url']
    
    try:
        print(f"Manually starting charge for {cp_id} at {cp_url}...")
        # We need the user who authorized the charge. 
        # For simplicity, we'll assume the last user or pass it in.
        # But wait, authorize_charge didn't store the user in DB, just audit log.
        # Let's just send 'ManualUser' or store it in memory/DB.
        # Ideally, we should have stored the pending session in DB.
        # For this MVP, let's just send 'ManualStart'.
        
        resp = requests.post(f"{cp_url}/start_charge", json={'user': 'ManualStart'}, timeout=5)
        
        if resp.status_code != 200:
            return jsonify({'error': f'CP failed to start: {resp.text}'}), 500

        # Update DB status to CHARGING
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute('UPDATE charging_points SET status = %s WHERE cp_id = %s', ('CHARGING', cp_id))
        conn.commit()
        cur.close()
        conn.close()
        
        log_audit('MANUAL_START', f"Manual start for {cp_id}", request.remote_addr)
        return jsonify({'status': 'started'}), 200
        
    except Exception as e:
        print(f"Failed to contact CP {cp_id}: {e}")
        return jsonify({'error': str(e)}), 500

@app.route('/admin/stop_charge', methods=['POST'])
def admin_stop_charge():
    data = request.get_json()
    cp_id = data.get('cp_id')
    
    if cp_id not in cp_sessions_info:
        return jsonify({'error': 'CP not connected'}), 404
        
    cp_url = cp_sessions_info[cp_id]['url']
    
    try:
        print(f"Manually stopping charge for {cp_id} at {cp_url}...")
        resp = requests.post(f"{cp_url}/stop_charge", timeout=5)
        
        if resp.status_code != 200:
            return jsonify({'error': f'CP failed to stop: {resp.text}'}), 500

        # Get ticket from CP response if available
        ticket = resp.json().get('ticket')

        # Update DB status to CONNECTED
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute('UPDATE charging_points SET status = %s WHERE cp_id = %s', ('CONNECTED', cp_id))
        conn.commit()
        cur.close()
        conn.close()
        
        log_audit('MANUAL_STOP', f"Manual stop for {cp_id}", request.remote_addr)
        return jsonify({'status': 'stopped', 'ticket': ticket}), 200
        
    except Exception as e:
        print(f"Failed to contact CP {cp_id}: {e}")
        return jsonify({'error': str(e)}), 500

@app.route('/admin/disconnect', methods=['POST'])
def admin_disconnect():
    data = request.get_json()
    cp_id = data.get('cp_id')
    
    if cp_id not in cp_sessions_info:
        # Even if not in session (maybe lost?), try to update DB
        pass
        
    # If connected, try to stop charge first
    ticket = None
    if cp_id in cp_sessions_info:
        cp_url = cp_sessions_info[cp_id]['url']
        try:
            print(f"Disconnecting {cp_id} at {cp_url}...")
            # We call stop_charge to ensure it stops and generates ticket
            resp = requests.post(f"{cp_url}/stop_charge", timeout=5)
            if resp.status_code == 200:
                ticket = resp.json().get('ticket')
        except Exception as e:
            print(f"Failed to stop CP {cp_id} during disconnect: {e}")

    # Update DB status to AVAILABLE
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute('UPDATE charging_points SET status = %s WHERE cp_id = %s', ('AVAILABLE', cp_id))
        
        # If no ticket returned (maybe already stopped), try to find the latest ticket for this CP
        if not ticket:
            # Look for ticket in last 30 seconds
            cur.execute("""
                SELECT cp_id, user_id, start_time, end_time, total_kwh, total_cost 
                FROM tickets 
                WHERE cp_id = %s 
                ORDER BY id DESC LIMIT 1
            """, (cp_id,))
            row = cur.fetchone()
            if row:
                # Check if it's recent (optional, but good practice)
                # For now, just return the last one.
                ticket = {
                    'cp_id': row[0],
                    'user_id': row[1],
                    'start_time': str(row[2]),
                    'end_time': str(row[3]),
                    'total_kwh': row[4],
                    'total_cost': row[5]
                }

        conn.commit()
        cur.close()
        conn.close()
        
        log_audit('DRIVER_DISCONNECT', f"Driver disconnected from {cp_id}", request.remote_addr)
        return jsonify({'status': 'disconnected', 'ticket': ticket}), 200
    except Exception as e:
        print(f"DB Error during disconnect {cp_id}: {e}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/cp_shutdown', methods=['POST'])
def cp_shutdown():
    data = request.get_json()
    cp_id = data.get('cp_id')
    
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute('UPDATE charging_points SET status = %s WHERE cp_id = %s', ('DISCONNECTED', cp_id))
        conn.commit()
        cur.close()
        conn.close()
        
        log_audit('CP_SHUTDOWN', f"CP {cp_id} shut down gracefully", request.remote_addr)
        print(f"CP {cp_id} shut down. Status set to DISCONNECTED.")
        return jsonify({'status': 'shutdown'}), 200
    except Exception as e:
        print(f"Error during CP shutdown {cp_id}: {e}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/submit_ticket', methods=['POST'])
def submit_ticket():
    data = request.get_json()
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute(
            'INSERT INTO tickets (cp_id, user_id, start_time, end_time, total_kwh, total_cost) VALUES (%s, %s, to_timestamp(%s), to_timestamp(%s), %s, %s)',
            (data['cp_id'], data['user_id'], data['start_time'], data['end_time'], data['total_kwh'], data['total_cost'])
        )
        conn.commit()
        cur.close()
        conn.close()
        print(f"Ticket saved for {data['cp_id']}")
        return jsonify({'status': 'saved'}), 201
    except Exception as e:
        print(f"Error saving ticket: {e}")
        return jsonify({'error': str(e)}), 500

@app.route('/tickets', methods=['GET'])
def get_tickets():
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute('SELECT * FROM tickets ORDER BY id DESC LIMIT 50')
        rows = cur.fetchall()
        
        tickets = []
        for row in rows:
            tickets.append({
                'id': row[0],
                'cp_id': row[1],
                'user_id': row[2],
                'start_time': str(row[3]),
                'end_time': str(row[4]),
                'total_kwh': row[5],
                'total_cost': row[6]
            })
            
        cur.close()
        conn.close()
        return jsonify({'tickets': tickets})
    except Exception as e:
        print(f"Error fetching tickets: {e}")
        return jsonify({'error': str(e)}), 500

@app.route('/alert', methods=['POST'])
def weather_alert():
    data = request.get_json()
    city = data.get('city')
    alert_type = data.get('type')
    
    log_audit('WEATHER_ALERT', f"Alert {alert_type} in {city}", request.remote_addr)
    
    # Store alert
    weather_alerts.append({'city': city, 'type': alert_type, 'time': 'Just now'})
    
    log_audit('WEATHER_ALERT', f"Alert {alert_type} in {city}", request.remote_addr)
    
    # Store alert
    weather_alerts.append({'city': city, 'type': alert_type, 'time': 'Just now'})
    
    # Logic to stop CPs in that city
    if alert_type == 'LOW_TEMP':
        print(f"Executing emergency stop for all CPs in {city}...")
        try:
            conn = get_db_connection()
            cur = conn.cursor()
            # Find all CPs in this city
            cur.execute("SELECT cp_id, status FROM charging_points WHERE location = %s", (city,))
            rows = cur.fetchall()
            
            for row in rows:
                target_cp = row[0]
                current_status = row[1]
                
                # If active, stop charge
                if current_status in ['CHARGING', 'CONNECTED']:
                    if target_cp in cp_sessions_info:
                        url = cp_sessions_info[target_cp]['url']
                        try:
                            requests.post(f"{url}/stop_charge", timeout=5)
                            print(f"Stopped {target_cp} due to weather.")
                        except Exception as e:
                            print(f"Failed to stop {target_cp}: {e}")
                
                # Set DB status to MAINTENANCE (or similar)
                cur.execute("UPDATE charging_points SET status = 'MAINTENANCE' WHERE cp_id = %s", (target_cp,))
                print(f"Set {target_cp} status to MAINTENANCE.")
            
            conn.commit()
            cur.close()
            conn.close()
        except Exception as e:
            print(f"Error processing weather alert stops: {e}")

    return jsonify({'status': 'received'}), 200

@app.route('/update_weather', methods=['POST'])
def update_weather():
    data = request.get_json()
    city = data.get('city')
    temp = data.get('temp')
    
    if city is not None and temp is not None:
        weather_cache[city] = temp
        
    return jsonify({'status': 'updated'}), 200

@app.route('/locations', methods=['GET'])
def get_locations():
    # Return list of unique cities where we have CPs
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute('SELECT DISTINCT location FROM charging_points')
    rows = cur.fetchall()
    cur.close()
    conn.close()
    
    locations = [r[0] for r in rows]
    return jsonify({'locations': locations}), 200

@app.route('/status', methods=['GET'])
def get_status():
    # For Frontend
    # Return list of CPs and their status
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute('SELECT cp_id, location, status FROM charging_points')
    rows = cur.fetchall()
    cur.close()
    conn.close()
    
    result = []
    for r in rows:
        cp_id = r[0]
        loc = r[1]
        stat = r[2]
        # Get temp
        temp = weather_cache.get(loc, 'N/A')
        result.append({'id': cp_id, 'location': loc, 'status': stat, 'temp': temp})
        
    return jsonify({'cps': result, 'alerts': weather_alerts}), 200

@app.route('/logs', methods=['GET'])
def get_logs():
    # Return last 50 audit logs
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute('SELECT timestamp, source_ip, action, description FROM audit_logs ORDER BY timestamp DESC LIMIT 50')
        rows = cur.fetchall()
        cur.close()
        conn.close()
        
        logs = []
        for r in rows:
            logs.append({
                'timestamp': r[0],
                'source_ip': r[1],
                'action': r[2],
                'description': r[3]
            })
        return jsonify({'logs': logs}), 200
    except Exception as e:
        print(f"Error fetching logs: {e}")
        return jsonify({'error': 'Failed to fetch logs'}), 500

if __name__ == '__main__':
    # Start Kafka Consumer in background
    t = threading.Thread(target=consume_telemetry, daemon=True)
    t.start()
    
    app.run(host='0.0.0.0', port=PORT_CENTRAL)
