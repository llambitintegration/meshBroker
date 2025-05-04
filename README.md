# MeshBroker: Meshtastic MQTT Bridge

A powerful bridge application connecting Meshtastic mesh networks with MQTT, providing real-time visualization, monitoring, and management capabilities.

## Overview

MeshBroker serves as an intelligent bridge between Meshtastic mesh networks and MQTT, enabling seamless communication, data visualization, and network management. The system includes:

- Robust backend with Python FastAPI
- MQTT integration with full protocol support
- WebSocket API for real-time updates
- Comprehensive frontend dashboard
- Map visualization for node positioning
- Message analytics and management

## Project Structure

```
meshBroker/
├── backend/                 # FastAPI backend application
│   ├── mqtt/                # MQTT handler and broker integration
│   ├── ws/                  # WebSocket implementation
│   ├── auth/                # Authentication and authorization
│   ├── models/              # Data models
│   ├── message_handlers/    # Message processing logic
│   └── monitoring/          # System monitoring
├── frontend/                # Frontend web application
│   ├── public/              # Static web assets
│   │   ├── js/              # JavaScript files
│   │   └── css/             # CSS styles
├── mosquitto/               # MQTT broker configuration
├── mosqDev/                 # CLI and development scripts
└── mqtt_data/               # MQTT data storage
```

## Features

### Backend
- MQTT message handling with QoS levels 0, 1, and 2
- Meshtastic protocol integration
- Message persistence and queue management
- Authentication and authorization
- Topic-based WebSocket channels
- Comprehensive system monitoring
- RESTful API for management

### Frontend
- Real-time dashboard with message statistics
- Interactive node management interface
- Message visualization and history
- MQTT topic explorer with subscription management
- Real-time map visualization of node positions
- Responsive, modern UI design

### CLI Utility (backend/meshtastic_mqtt_cli.py)
- Send and receive MQTT messages from the command line
- Supports broker/port/topic selection, QoS, retain, and logging
- Message decryption and protocol buffer parsing for Meshtastic messages
- Message relay to different topics with transformations
- Various output formats (text, JSON, raw)
- Output redirection to file

#### Basic Usage:

```bash
# Send a message
python backend/meshtastic_mqtt_cli.py send --topic test/topic --message "Hello Mesh!" --broker localhost --port 1883

# Receive messages
python backend/meshtastic_mqtt_cli.py receive --topic test/topic --broker localhost --port 1883
```

#### Advanced Usage (Phase 3 Features):

```bash
# Receive messages with decryption enabled and channel key
python backend/meshtastic_mqtt_cli.py receive --topic "msh/#" --decrypt --channel-key "your-channel-key" 

# Receive and decode messages, output as JSON, and save to file
python backend/meshtastic_mqtt_cli.py receive --topic "msh/#" --decrypt --keyfile "psk.key" --output-format json --output-file "messages.log"

# Receive, decode, and relay messages to another topic
python backend/meshtastic_mqtt_cli.py receive --topic "msh/#" --decrypt --channel-key "your-channel-key" --relay --relay-topic "relay/{node_id}/message"
```

## Getting Started

### Prerequisites
- Python 3.8+
- Node.js 14+
- Mosquitto MQTT Broker
- Meshtastic device(s)

### Installation

1. Clone the repository:
```bash
git clone https://github.com/yourusername/meshBroker.git
cd meshBroker
```

2. Set up the backend:
```bash
cd backend
pip install -r requirements.txt
```

3. Start the backend server:
```bash
python run.py
```

4. Start the frontend server:
```bash
cd ../frontend
npm install
node server.js
```

5. Access the dashboard at `http://localhost:5000`

## Progress

The project is actively being developed according to the checklist in `backend/checklist.md` and the Meshtastic MQTT CLI roadmap. Key milestones achieved:

- ✅ Core MQTT functionality with message persistence
- ✅ Meshtastic integration with node management
- ✅ API and WebSocket enhancements
- ✅ Testing and monitoring functionality
- ✅ Initial frontend integration with WebSocket connectivity
- ✅ Map visualization with Leaflet integration
- ✅ CLI utility for MQTT send/receive (Phase 1)
- ✅ CLI utility: Message decryption and relay (Phase 3)

## Next Steps

The following features are currently in development:

- Enhanced message visualization components
- Advanced node management interface
- Geofencing and path visualization
- Topic hierarchy visualization
- User authentication UI
- Mobile responsiveness and cross-browser compatibility
- Progressive Web App capabilities
- CLI utility: Daemonization and advanced features (Phase 4)

## License

This project is licensed under the MIT License - see the LICENSE file for details.

## Acknowledgments

- [Meshtastic](https://meshtastic.org/) for the amazing mesh networking platform
- [Mosquitto](https://mosquitto.org/) for the reliable MQTT broker
- [FastAPI](https://fastapi.tiangolo.com/) for the efficient backend framework
- [Leaflet](https://leafletjs.com/) for the interactive mapping library
