-- Users/Drivers
CREATE TABLE drivers (
    id SERIAL PRIMARY KEY,
    username VARCHAR(50) UNIQUE NOT NULL,
    password VARCHAR(100) NOT NULL -- In production, this should be hashed
);

-- Charging Points (CPs)
CREATE TABLE charging_points (
    id SERIAL PRIMARY KEY,
    cp_id VARCHAR(50) UNIQUE NOT NULL, -- e.g., "MADRID-01"
    location VARCHAR(100) NOT NULL,
    status VARCHAR(20) DEFAULT 'DISCONNECTED', -- DISCONNECTED, AVAILABLE, CHARGING, ERROR, MAINTENANCE
    last_heartbeat TIMESTAMP,
    security_token VARCHAR(100), -- Token for initial auth
    shared_secret VARCHAR(100) -- Negotiated session key
);

-- Audit Logs
CREATE TABLE audit_logs (
    id SERIAL PRIMARY KEY,
    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    source_ip VARCHAR(50),
    action VARCHAR(50),
    description TEXT
);

-- Tickets
CREATE TABLE tickets (
    id SERIAL PRIMARY KEY,
    cp_id VARCHAR(50),
    user_id VARCHAR(50),
    start_time TIMESTAMP,
    end_time TIMESTAMP,
    total_kwh FLOAT,
    total_cost FLOAT
);

