import requests
import time
import os
import sys
import random
import signal
import argparse

# Global state to track current session
current_session = {'cp_id': None, 'user': None}

def signal_handler(sig, frame):
    print("\nGraceful disconnect requested...")
    if current_session['cp_id']:
        print(f"Disconnecting {current_session['user']} from {current_session['cp_id']}...")
        try:
            # Call disconnect endpoint
            resp = requests.post(f"{CENTRAL_URL}/admin/disconnect", json={'cp_id': current_session['cp_id']})
            if resp.status_code == 200:
                print("Disconnected successfully.")
                data = resp.json()
                if data.get('ticket'):
                    t = data['ticket']
                    print("\n" + "="*30)
                    print(f" TICKET RECEIVED")
                    print(f" CP: {t['cp_id']}")
                    print(f" User: {t['user_id']}")
                    print(f" kWh: {t['total_kwh']}")
                    print(f" Cost: {t['total_cost']} EUR")
                    print("="*30 + "\n")
                else:
                    print("No ticket received in disconnect response.")
                    print(f"Full response: {data}")
            else:
                print(f"Disconnect failed: {resp.text}")
        except Exception as e:
            print(f"Failed to disconnect: {e}")
    sys.exit(0)

if __name__ == '__main__':
    signal.signal(signal.SIGINT, signal_handler)
    
    parser = argparse.ArgumentParser()
    # ...

# Add parent directory to path to import ev_common
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from ev_common.config import PORT_CENTRAL

CENTRAL_URL = f"http://ev_central:{PORT_CENTRAL}"
# We will update CENTRAL_URL in main if server-ip is provided or if running locally


# Simulation parameters
USERS = ['Alice', 'Bob', 'Charlie', 'Dave']
CPS = ['MADRID-01'] 

def request_charge(user, cp_id):
    print(f"User {user} requesting charge at {cp_id}...")
    try:
        payload = {'cp_id': cp_id, 'user': user}
        resp = requests.post(f"{CENTRAL_URL}/authorize_charge", json=payload)
        
        if resp.status_code == 200:
            print(f"Charge authorized for {user} at {cp_id}")
            # Automatically start charge for simulation?
            # The user said "al hacer ctr + c ... este se desconecte".
            # If we just authorize, we are CONNECTED.
            # If we want to simulate a full session, we should probably START it too.
            # But the driver script just requests auth.
            # Let's assume if we authorized, we are "connected".
            current_session['cp_id'] = cp_id
            current_session['user'] = user
            
            # If manual mode, we might want to wait here until Ctrl+C
            if len(sys.argv) > 1 and '--user' in sys.argv:
                 print("Connected. Press Ctrl+C to disconnect.")
                 while True:
                     time.sleep(1)
        else:
            print(f"Charge denied for {user} at {cp_id}: {resp.text}")
    except Exception as e:
        print(f"Error requesting charge: {e}")

def simulate_driver():
    print("Starting EV_Driver Simulation...")
    while True:
        try:
            # Randomly pick a user and CP
            user = random.choice(USERS)
            cp_id = random.choice(CPS)
            
            request_charge(user, cp_id)
            
            # Wait random time before next request
            sleep_time = random.randint(10, 30)
            print(f"Sleeping for {sleep_time}s...")
            time.sleep(sleep_time)
            
        except Exception as e:
            print(f"Driver simulation error: {e}")
            time.sleep(5)

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--user', help='User name for manual request')
    parser.add_argument('--cp', help='CP ID for manual request')
    parser.add_argument('--server-ip', default='127.0.0.1', help='IP address of the Central server')
    args = parser.parse_args()

    if os.getenv('DB_HOST') is None:
        CENTRAL_URL = f"http://{args.server_ip}:{PORT_CENTRAL}"

    if args.user and args.cp:
        request_charge(args.user, args.cp)
    else:
        simulate_driver()
