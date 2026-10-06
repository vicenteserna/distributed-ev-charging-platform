import os

# Database Config
DB_HOST = os.getenv('DB_HOST', 'localhost')
DB_PORT = os.getenv('DB_PORT', '5432')
DB_NAME = os.getenv('DB_NAME', 'ev_charging_db')
DB_USER = os.getenv('DB_USER', 'admin')
DB_PASSWORD = os.getenv('DB_PASSWORD')
DB_URI = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

# Kafka Config
KAFKA_BROKER = os.getenv('KAFKA_BROKER', 'localhost:9092')
KAFKA_TOPIC_TELEMETRY = 'ev_telemetry'

# Service Ports
PORT_REGISTRY = 5001
PORT_CENTRAL = 5002
PORT_FRONT = 5000

# Security
SECRET_KEY_DEFAULT = os.getenv('SECRET_KEY')
