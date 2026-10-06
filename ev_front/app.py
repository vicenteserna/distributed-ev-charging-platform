from flask import Flask, render_template, jsonify, request
import requests
import os
import sys

# Add parent directory to path to import ev_common
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from ev_common.config import PORT_CENTRAL, PORT_FRONT

app = Flask(__name__)
CENTRAL_URL = f"http://ev_central:{PORT_CENTRAL}"

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/api/status')
def get_status():
    try:
        resp = requests.get(f"{CENTRAL_URL}/status")
        if resp.status_code == 200:
            return jsonify(resp.json())
        else:
            return jsonify({'error': 'Failed to fetch status'}), 500
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/logs')
def get_logs():
    try:
        resp = requests.get(f"{CENTRAL_URL}/logs")
        if resp.status_code == 200:
            return jsonify(resp.json())
        else:
            return jsonify({'error': 'Failed to fetch logs'}), 500
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/start_charge', methods=['POST'])
def start_charge():
    try:
        data = request.get_json()
        resp = requests.post(f"{CENTRAL_URL}/admin/start_charge", json=data)
        if resp.status_code == 200:
            return jsonify(resp.json())
        else:
            # Try to get error from central
            try:
                err_msg = resp.json().get('error', 'Unknown error from Central')
            except:
                err_msg = resp.text
            return jsonify({'error': err_msg}), resp.status_code
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/stop_charge', methods=['POST'])
def stop_charge():
    try:
        data = request.get_json()
        resp = requests.post(f"{CENTRAL_URL}/admin/stop_charge", json=data)
        if resp.status_code == 200:
            return jsonify(resp.json())
        else:
            # Try to get error from central
            try:
                err_msg = resp.json().get('error', 'Unknown error from Central')
            except:
                err_msg = resp.text
            return jsonify({'error': err_msg}), resp.status_code
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/revoke_key', methods=['POST'])
def revoke_key():
    try:
        data = request.get_json()
        resp = requests.post(f"{CENTRAL_URL}/revoke_key", json=data)
        if resp.status_code == 200:
            return jsonify(resp.json())
        else:
            return jsonify({'error': 'Failed to revoke key'}), resp.status_code
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/tickets')
def get_tickets():
    try:
        resp = requests.get(f"{CENTRAL_URL}/tickets")
        if resp.status_code == 200:
            return jsonify(resp.json())
        else:
            return jsonify({'error': 'Failed to fetch tickets'}), 500
    except Exception as e:
        return jsonify({'error': str(e)}), 500

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=PORT_FRONT)
