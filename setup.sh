#!/bin/bash

# Setup script for Meshtastic MQTT Bridge
# This script installs and configures all the necessary components for Windows 11

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

# Check if running with administrator privileges
if ! net session &>/dev/null; then
    print_error "Please run as administrator (right-click, Run as administrator)"
    exit 1
fi

# Install Chocolatey package manager if not already installed
print_info "Checking for Chocolatey package manager..."
if ! command -v choco &>/dev/null; then
    print_info "Installing Chocolatey package manager..."
    powershell -Command "Set-ExecutionPolicy Bypass -Scope Process -Force; [System.Net.ServicePointManager]::SecurityProtocol = [System.Net.ServicePointManager]::SecurityProtocol -bor 3072; iex ((New-Object System.Net.WebClient).DownloadString('https://chocolatey.org/install.ps1'))"
fi

# Install required packages using Chocolatey
print_info "Installing required packages..."
choco install -y mosquitto python nodejs npm

# Add Python and npm to PATH for this session
export PATH="$PATH:/c/Program Files/Python310:/c/Program Files/Python310/Scripts:/c/Program Files/nodejs"

# Create directories for logs
print_info "Creating log directories..."
mkdir -p "/c/ProgramData/mosquitto/log"

# Create directory for mosquitto persistence
print_info "Creating persistence directory..."
mkdir -p "/c/ProgramData/mosquitto/data"

# Copy Mosquitto configuration
print_info "Configuring Mosquitto..."
cp "/mosquitto/mosquitto.conf" "/c/Program Files/mosquitto/mosquitto.conf"

# Start Mosquitto service
print_info "Starting Mosquitto service..."
net start mosquitto
sc config mosquitto start= auto

# Set up Python virtual environment for the backend
print_info "Setting up Python virtual environment for backend..."
mkdir -p "/c/Program Files/meshtastic-mqtt-bridge"
cd "/c/Program Files/meshtastic-mqtt-bridge"

python -m venv venv
source venv/Scripts/activate

# Install Python packages
pip install fastapi uvicorn paho-mqtt

# Copy backend files
print_info "Copying backend files..."
mkdir -p "/c/Program Files/meshtastic-mqtt-bridge/backend"
cp /backend/* "/c/Program Files/meshtastic-mqtt-bridge/backend/"

# Create a Windows service for the FastAPI backend using NSSM
print_info "Creating Windows service for backend..."
choco install -y nssm
nssm install MeshtasticMQTTBackend "/c/Program Files/meshtastic-mqtt-bridge/venv/Scripts/python.exe" "/c/Program Files/meshtastic-mqtt-bridge/backend/app.py"
nssm set MeshtasticMQTTBackend AppDirectory "/c/Program Files/meshtastic-mqtt-bridge"
nssm set MeshtasticMQTTBackend AppEnvironmentExtra "PYTHONPATH=/c/Program Files/meshtastic-mqtt-bridge"
nssm set MeshtasticMQTTBackend Start SERVICE_AUTO_START
nssm set MeshtasticMQTTBackend ObjectName LocalSystem

# Set up Node.js frontend
print_info "Setting up Node.js frontend..."
mkdir -p "/c/Program Files/meshtastic-mqtt-bridge/frontend"
cp -r /frontend/* "/c/Program Files/meshtastic-mqtt-bridge/frontend/"

# Install Node.js dependencies
cd "/c/Program Files/meshtastic-mqtt-bridge/frontend"
npm install express

# Create a Windows service for the Node.js frontend using NSSM
print_info "Creating Windows service for frontend..."
nssm install MeshtasticMQTTFrontend "C:\Program Files\nodejs\node.exe" "/c/Program Files/meshtastic-mqtt-bridge/frontend/server.js"
nssm set MeshtasticMQTTFrontend AppDirectory "/c/Program Files/meshtastic-mqtt-bridge/frontend"
nssm set MeshtasticMQTTFrontend AppEnvironmentExtra "PORT=5000"
nssm set MeshtasticMQTTFrontend Start SERVICE_AUTO_START
nssm set MeshtasticMQTTFrontend ObjectName LocalSystem

# Start services
print_info "Starting services..."
nssm start MeshtasticMQTTBackend
nssm start MeshtasticMQTTFrontend

# Check if services are running
print_info "Checking service status..."
backend_status=$(sc query MeshtasticMQTTBackend | grep STATE | grep RUNNING)
frontend_status=$(sc query MeshtasticMQTTFrontend | grep STATE | grep RUNNING)
mosquitto_status=$(sc query mosquitto | grep STATE | grep RUNNING)

if [ -n "$backend_status" ] && [ -n "$frontend_status" ] && [ -n "$mosquitto_status" ]; then
    print_success "All services are running!"
    echo ""
    echo "Meshtastic MQTT Bridge is now installed and running on Windows 11."
    echo "Frontend: http://localhost:5000"
    echo "Backend API: http://localhost:8000"
    echo "MQTT Broker: localhost:1883"
else
    print_error "Some services failed to start. Please check the Windows Event Viewer for details."
    echo "You can also check service status with: sc query MeshtasticMQTTBackend"
    echo "sc query MeshtasticMQTTFrontend"
    echo "sc query mosquitto"
fi
