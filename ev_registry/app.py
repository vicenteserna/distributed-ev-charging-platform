from flask import Flask, request, jsonify
import secrets
import psycopg2
import os
import sys

# Add parent directory to path to import ev_common if running locally
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from ev_common.config import DB_URI, PORT_REGISTRY

app = Flask(__name__)

def get_db_connection():
    conn = psycopg2.connect(DB_URI)
    return conn

@app.route('/register', methods=['POST'])
def register_cp():
    data = request.get_json()
    station_id = data.get('station_id')
    location = data.get('location')

    if not station_id or not location:
        return jsonify({'error': 'Missing station_id or location'}), 400

    # Generate a secure token
    token = secrets.token_hex(32)

    try:
        conn = get_db_connection()
        cur = conn.cursor()
        
        # Check if already exists
        cur.execute('SELECT id FROM charging_points WHERE cp_id = %s', (station_id,))
        if cur.fetchone():
            cur.close()
            conn.close()
            return jsonify({'error': 'Station ID already registered'}), 409

        # Insert new CP
        cur.execute(
            'INSERT INTO charging_points (cp_id, location, security_token, status) VALUES (%s, %s, %s, %s)',
            (station_id, location, token, 'DISCONNECTED')
        )
        conn.commit()
        cur.close()
        conn.close()

        print(f"Registered CP: {station_id}")
        return jsonify({'token': token, 'message': 'Registration successful'}), 201

    except Exception as e:
        print(f"Error registering CP: {e}")
        return jsonify({'error': str(e)}), 500

@app.route('/unregister', methods=['POST'])
def unregister_cp():
    data = request.get_json()
    station_id = data.get('station_id')

    if not station_id:
        return jsonify({'error': 'Missing station_id'}), 400

    try:
        conn = get_db_connection()
        cur = conn.cursor()
        
        # Delete CP
        cur.execute('DELETE FROM charging_points WHERE cp_id = %s', (station_id,))
        if cur.rowcount == 0:
            cur.close()
            conn.close()
            return jsonify({'error': 'Station ID not found'}), 404

        conn.commit()
        cur.close()
        conn.close()

        print(f"Unregistered CP: {station_id}")
        return jsonify({'message': 'Unregistration successful'}), 200

    except Exception as e:
        print(f"Error unregistering CP: {e}")
        return jsonify({'error': str(e)}), 500

@app.route('/health', methods=['GET'])
def health():
    return jsonify({'status': 'ok'}), 200

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=PORT_REGISTRY)
