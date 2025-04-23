#!/bin/bash

# Setup script for Meshtastic MQTT Bridge
# This script installs and configures all the necessary components

set -e

# Print colored messages
print_info() {
    echo -e "\e[1;34m[INFO]\e[0m $1"
}

print_success() {
    echo -e "\e[1;32m[SUCCESS]\e[0m $1"
}

print_error() {
    echo -e "\e[1;31m[ERROR]\e[0m $1"
}

# Check if running as root
if [ "$EUID" -ne 0 ]; then
    print_error "Please run as root"
    exit 1
fi

# Update system packages
print_info "Updating system packages..."
apt-get update
apt-get upgrade -y

# Install required packages
print_info "Installing required packages..."
apt-get install -y \
    mosquitto \
    mosquitto-clients \
    python3 \
    python3-pip \
    python3-venv \
    nodejs \
    npm \
    curl

# Create directories for logs
print_info "Creating log directories..."
mkdir -p /var/log/mosquitto
chown mosquitto:mosquitto /var/log/mosquitto

# Create directory for mosquitto persistence
print_info "Creating persistence directory..."
mkdir -p /var/lib/mosquitto
chown mosquitto:mosquitto /var/lib/mosquitto

# Copy Mosquitto configuration
print_info "Configuring Mosquitto..."
cp /mosquitto/mosquitto.conf /etc/mosquitto/conf.d/meshtastic.conf
chown root:root /etc/mosquitto/conf.d/meshtastic.conf
chmod 644 /etc/mosquitto/conf.d/meshtastic.conf

# Restart Mosquitto service
print_info "Restarting Mosquitto service..."
systemctl restart mosquitto
systemctl enable mosquitto

# Set up Python virtual environment for the backend
print_info "Setting up Python virtual environment for backend..."
mkdir -p /opt/meshtastic-mqtt-bridge
cd /opt/meshtastic-mqtt-bridge

python3 -m venv venv
source venv/bin/activate

# Install Python packages
pip install fastapi uvicorn paho-mqtt

# Copy backend files
print_info "Copying backend files..."
mkdir -p /opt/meshtastic-mqtt-bridge/backend
cp /backend/* /opt/meshtastic-mqtt-bridge/backend/

# Create a systemd service for the FastAPI backend
print_info "Creating systemd service for backend..."
cat > /etc/systemd/system/meshtastic-mqtt-backend.service << EOF
[Unit]
Description=Meshtastic MQTT Bridge Backend
After=network.target mosquitto.service

[Service]
User=root
WorkingDirectory=/opt/meshtastic-mqtt-bridge
ExecStart=/opt/meshtastic-mqtt-bridge/venv/bin/python3 /opt/meshtastic-mqtt-bridge/backend/app.py
Restart=always
Environment="PYTHONPATH=/opt/meshtastic-mqtt-bridge"

[Install]
WantedBy=multi-user.target
EOF

# Set up Node.js frontend
print_info "Setting up Node.js frontend..."
mkdir -p /opt/meshtastic-mqtt-bridge/frontend
cp -r /frontend/* /opt/meshtastic-mqtt-bridge/frontend/

# Install Node.js dependencies
cd /opt/meshtastic-mqtt-bridge/frontend
npm install express

# Create a systemd service for the Node.js frontend
print_info "Creating systemd service for frontend..."
cat > /etc/systemd/system/meshtastic-mqtt-frontend.service << EOF
[Unit]
Description=Meshtastic MQTT Bridge Frontend
After=network.target

[Service]
User=root
WorkingDirectory=/opt/meshtastic-mqtt-bridge/frontend
ExecStart=/usr/bin/node /opt/meshtastic-mqtt-bridge/frontend/server.js
Restart=always
Environment="PORT=5000"

[Install]
WantedBy=multi-user.target
EOF

# Enable and start services
print_info "Enabling and starting services..."
systemctl daemon-reload
systemctl enable meshtastic-mqtt-backend.service
systemctl enable meshtastic-mqtt-frontend.service
systemctl start meshtastic-mqtt-backend.service
systemctl start meshtastic-mqtt-frontend.service

# Check if services are running
print_info "Checking service status..."
if systemctl is-active --quiet mosquitto && \
   systemctl is-active --quiet meshtastic-mqtt-backend.service && \
   systemctl is-active --quiet meshtastic-mqtt-frontend.service; then
    print_success "All services are running!"
    echo ""
    echo "Meshtastic MQTT Bridge is now installed and running."
    echo "Frontend: http://localhost:5000"
    echo "Backend API: http://localhost:8000"
    echo "MQTT Broker: localhost:1883"
else
    print_error "Some services failed to start. Please check the logs with:"
    echo "journalctl -u mosquitto.service"
    echo "journalctl -u meshtastic-mqtt-backend.service"
    echo "journalctl -u meshtastic-mqtt-frontend.service"
fi
