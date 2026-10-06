from flask import Flask, request, jsonify
import requests
import time
import threading
import json
import os
import sys
from kafka import KafkaProducer
from cryptography.fernet import Fernet

import argparse
import socket
import signal

# Add parent directory to path to import ev_common
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from ev_common.config import PORT_REGISTRY, PORT_CENTRAL, KAFKA_BROKER, KAFKA_TOPIC_TELEMETRY

# Parse Args
parser = argparse.ArgumentParser()
parser.add_argument('--id', default=os.getenv('CP_ID', 'MADRID-01'))
parser.add_argument('--port', type=int, default=5000)
parser.add_argument('--city', default='Madrid')
parser.add_argument('--server-ip', default='127.0.0.1', help='IP address of the Central/Registry server')
parser.add_argument('--price', type=float, default=0.30, help='Price per kWh in EUR')
args = parser.parse_args()

app = Flask(__name__)

# Configuration
CP_ID = args.id
PORT = args.port
CITY = args.city
REGISTRY_URL = f"http://ev_registry:{PORT_REGISTRY}"
CENTRAL_URL = f"http://ev_central:{PORT_CENTRAL}"

# If running locally (not docker), localhost might be needed
# If running locally (not docker), localhost might be needed
if os.getenv('DB_HOST') is None: # Simple check if we are in docker
    SERVER_IP = args.server_ip
    REGISTRY_URL = f"http://{SERVER_IP}:{PORT_REGISTRY}"
    CENTRAL_URL = f"http://{SERVER_IP}:{PORT_CENTRAL}"
    KAFKA_BROKER = f'{SERVER_IP}:9092' # Force IPv4 localhost if not in docker

# Get own hostname/IP
hostname = socket.gethostname()

def get_local_ip():
    try:
        # Connect to an external IP (Google DNS) to get the local interface IP
        # This does not send any data, just determines the route
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.connect(("8.8.8.8", 80))
        ip = s.getsockname()[0]
        s.close()
        return ip
    except Exception:
        return '127.0.0.1'

# For callback, if local, use localhost
CALLBACK_URL = f"http://{hostname}:{PORT}"
if os.getenv('DB_HOST') is None:
    # We are running locally (not in docker), but Central is likely in Docker (or remote)
    # We need to send an IP that Central can reach.
    # If Central is on localhost (mapped port), we can't send 'localhost' because that means Central itself.
    # We need our LAN IP.
    local_ip = get_local_ip()
    CALLBACK_URL = f"http://{local_ip}:{PORT}"
    print(f"[{CP_ID}] Detected local IP: {local_ip}. Callback URL: {CALLBACK_URL}")

# State
state = {
    'status': 'DISCONNECTED', # DISCONNECTED, READY, CHARGING, ERROR
    'token': None,
    'session_key': None,
    'current_charge': 0.0, # kWh
    'current_cost': 0.0, # EUR
    'user': None,
    'start_time': None
}

# --- Monitor Thread (Registration & Auth) ---
def monitor_loop():
    print(f"[{CP_ID}] Starting Monitor Loop...")
    token_file = f"token_{CP_ID}.json"

    while True: # Main Reconnection Loop
        
        if state['token'] is None:
            # 1. Try load from file
            if os.path.exists(token_file):
                try:
                    with open(token_file, 'r') as f:
                        data = json.load(f)
                        state['token'] = data.get('token')
                        print(f"[{CP_ID}] Loaded token from file")
                except Exception as e:
                    print(f"[{CP_ID}] Failed to load token file: {e}")

        # 2. Always verify registration status with Registry
        # This handles the case where DB was reset (we have token, but DB doesn't know us)
        # or where we are new.
        try:
            print(f"[{CP_ID}] Verifying registration in {CITY}...")
            resp = requests.post(f"{REGISTRY_URL}/register", json={'station_id': CP_ID, 'location': CITY})
            
            if resp.status_code == 201:
                # Registered successfully (New DB or New CP)
                data = resp.json()
                new_token = data['token']
                if state['token'] != new_token:
                    print(f"[{CP_ID}] Re-registered with a new token")
                    state['token'] = new_token
                    # Save new token
                    try:
                        with open(token_file, 'w') as f:
                            json.dump({'token': state['token']}, f)
                    except Exception as e:
                        print(f"[{CP_ID}] Failed to save token file: {e}")
            elif resp.status_code == 409:
                print(f"[{CP_ID}] Already registered on server.")
                if state['token'] is None:
                     print(f"[{CP_ID}] CRITICAL: Server has us registered, but we have no token. Waiting for manual fix or timeout...")
                     time.sleep(5)
                     continue # Retry loop
            else:
                 print(f"[{CP_ID}] Registration check failed: {resp.text}")
                 time.sleep(5)
                 continue
                 
        except Exception as e:
            print(f"[{CP_ID}] Registration check error: {e}")
            time.sleep(5)
            continue
            
        if state['token'] is None:
            continue

        # --- PHASE 2: LOGIN & MAINTAIN SESSION ---
        # We have a token. Try to login.
        while state['token']:
            if state['session_key'] is None:
                try:
                    print(f"[{CP_ID}] Attempting login...")
                    resp = requests.post(f"{CENTRAL_URL}/login", json={
                        'cp_id': CP_ID, 
                        'token': state['token'],
                        'callback_url': CALLBACK_URL
                    })
                    if resp.status_code == 200:
                        data = resp.json()
                        state['session_key'] = data['session_key']
                        state['status'] = 'READY'
                        print(f"[{CP_ID}] Logged in. Session Key: {state['session_key']}")
                    elif resp.status_code == 401:
                        print(f"[{CP_ID}] Login failed: Invalid Token. Deleting cached token and re-registering.")
                        state['token'] = None
                        state['session_key'] = None
                        if os.path.exists(token_file):
                            try:
                                os.remove(token_file)
                            except Exception as e:
                                print(f"[{CP_ID}] Failed to delete token file: {e}")
                        break # Break inner loop, go back to Phase 1
                    else:
                        print(f"[{CP_ID}] Login failed: {resp.text}")
                        time.sleep(5)
                except Exception as e:
                    print(f"[{CP_ID}] Login error: {e}")
                    time.sleep(5)
            else:
                # Session exists, verify it or wait
                time.sleep(2)

# --- Engine Thread (Simulation & Streaming) ---
def engine_loop():
    print(f"[{CP_ID}] Starting Engine Loop...")
    producer = None
    while producer is None:
        try:
            print(f"[{CP_ID}] Connecting to Kafka at {KAFKA_BROKER}...")
            producer = KafkaProducer(bootstrap_servers=KAFKA_BROKER)
            print(f"[{CP_ID}] Kafka Connected!")
        except Exception as e:
            print(f"[{CP_ID}] Waiting for Kafka ({KAFKA_BROKER}): {e}")
            time.sleep(2)

    while True:
        if state['status'] == 'CHARGING' and state['session_key']:
            # Simulate charging
            state['current_charge'] += 0.1 # +0.1 kWh per second
            state['current_cost'] = state['current_charge'] * args.price
            
            # Create payload
            telemetry = {
                'cp_id': CP_ID,
                'consumo': round(state['current_charge'], 2),
                'coste': round(state['current_cost'], 2),
                'user': state['user']
            }
            
            # Encrypt
            try:
                f = Fernet(state['session_key'])
                payload_json = json.dumps(telemetry)
                encrypted_payload = f.encrypt(payload_json.encode('utf-8')).decode('utf-8')
                
                # Send to Kafka
                msg = {'cp_id': CP_ID, 'payload': encrypted_payload}
                producer.send(KAFKA_TOPIC_TELEMETRY, json.dumps(msg).encode('utf-8'))
                # print(f"[{CP_ID}] Sent telemetry: {telemetry}")
            except Exception as e:
                print(f"[{CP_ID}] Encryption/Send error: {e}")
                
        time.sleep(1)

# --- API for Central ---
@app.route('/authorize', methods=['POST'])
def authorize():
    print(f"[{CP_ID}] Received /authorize request")
    data = request.get_json()
    state['user'] = data.get('user')
    state['status'] = 'CONNECTED'
    state['current_charge'] = 0.0
    state['current_cost'] = 0.0
    state['start_time'] = None # Not charging yet
    print(f"[{CP_ID}] Authorized user {state['user']}. Status set to CONNECTED.")
    return jsonify({'status': 'authorized'}), 200

@app.route('/start_charge', methods=['POST'])
def start_charge():
    # Central orders start
    # Check if we are ready
    if state['status'] not in ['READY', 'CHARGING', 'CONNECTED']: 
        pass

    data = request.get_json()
    state['status'] = 'CHARGING'
    
    new_user = data.get('user', 'unknown')
    # If we already have a user (from authorize) and the new user is 'ManualStart', keep the existing user.
    if state['user'] and new_user == 'ManualStart':
        pass # Keep existing user
    else:
        state['user'] = new_user
        
    state['current_charge'] = 0.0
    state['current_cost'] = 0.0
    state['start_time'] = time.time()
    print(f"[{CP_ID}] Started charging for {state['user']} at {args.price} EUR/kWh")
    return jsonify({'status': 'started'}), 200

@app.route('/stop_charge', methods=['POST'])
def stop_charge():
    ticket = None
    if state['status'] in ['CHARGING', 'CONNECTED']:
        # Generate Ticket
        end_time = time.time()
        start_t = state['start_time'] or end_time
        
        ticket = {
            'cp_id': CP_ID,
            'user_id': state['user'],
            'start_time': start_t,
            'end_time': end_time,
            'total_kwh': round(state['current_charge'], 2),
            'total_cost': round(state['current_cost'], 2)
        }
        
        print("\n" + "="*30)
        print(f" TICKET GENERATED - {CP_ID}")
        print(f" User: {ticket['user_id']}")
        print(f" kWh: {ticket['total_kwh']}")
        print(f" Cost: {ticket['total_cost']} EUR")
        print("="*30 + "\n")
        
        # Send ticket to Central
        try:
            requests.post(f"{CENTRAL_URL}/api/submit_ticket", json=ticket)
        except Exception as e:
            print(f"Failed to submit ticket to Central: {e}")

    state['status'] = 'READY'
    state['user'] = None
    print(f"[{CP_ID}] Stopped charging")
    if ticket:
        return jsonify({'status': 'stopped', 'ticket': ticket}), 200
    else:
        return jsonify({'status': 'stopped'}), 200

@app.route('/revoked', methods=['POST'])
def revoked():
    print(f"\n[{CP_ID}] !!! REVOCATION RECEIVED FROM CENTRAL !!!")
    print(f"[{CP_ID}] Disconnecting and clearing credentials...")
    
    # 1. Clear State
    state['status'] = 'DISCONNECTED'
    state['token'] = None
    state['session_key'] = None
    state['user'] = None
    state['start_time'] = None
    state['current_charge'] = 0.0
    state['current_cost'] = 0.0
    
    # 2. Delete Token File
    token_file = f"token_{CP_ID}.json"
    if os.path.exists(token_file):
        try:
            os.remove(token_file)
            print(f"[{CP_ID}] Token file deleted.")
        except Exception as e:
            print(f"[{CP_ID}] Failed to delete token file: {e}")
            
    print(f"[{CP_ID}] Reset complete. Monitor loop will restart registration shortly.\n")
    return jsonify({'status': 'acknowledged'}), 200

@app.route('/health', methods=['GET'])
def health():
    # Check if session is still valid with Central (Heartbeat)
    # If Central restarted, our key is invalid.
    # We can try a dummy request or just rely on failures.
    # Let's add a proactive check or handle failures in engine.
    return jsonify({'status': state['status']}), 200

# Background thread to check connection health
    while True:
        if state['session_key']:
            # Verify with Central if we are still authenticated?
            # Or just wait for operations to fail.
            # Let's try to re-login if we suspect issues?
            pass
        time.sleep(10)

def signal_handler(sig, frame):
    print(f"\n[{CP_ID}] Graceful shutdown requested...")
    try:
        if state['token']: # Only if we registered/logged in
            print(f"[{CP_ID}] Notifying Central of shutdown...")
            requests.post(f"{CENTRAL_URL}/api/cp_shutdown", json={'cp_id': CP_ID})
    except Exception as e:
        print(f"[{CP_ID}] Shutdown notification failed: {e}")
    sys.exit(0)

if __name__ == '__main__':
    signal.signal(signal.SIGINT, signal_handler)

    # Start background threads
    t_monitor = threading.Thread(target=monitor_loop, daemon=True)
    t_monitor.start()
    
    t_engine = threading.Thread(target=engine_loop, daemon=True)
    t_engine.start()
    
    # Run Flask
    app.run(host='0.0.0.0', port=PORT)
