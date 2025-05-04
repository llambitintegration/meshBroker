# Meshtastic MQTT CLI Utility Roadmap (Checklist)

## Overview
This document outlines the phased implementation plan for a Python CLI utility that leverages the Meshtastic Python packages (`meshtastic`, `meshtastic.cli`, and `json-mqtt`) to communicate with Meshtastic nodes via a local Mosquitto MQTT bridge. The utility will support sending and receiving (pub/sub) messages, with the ability to decrypt messages as they are relayed through the localhost bridge. The focus is on backend and broker interaction, not frontend/UI.

---

## Phase 1: CLI Argument Parsing & Basic MQTT Pub/Sub
### Summary
- [x] Implement a Python CLI that can:
  - [x] Connect to a local MQTT broker (Mosquitto)
  - [x] Publish messages to a specified topic
  - [x] Subscribe to one or more topics and print received messages
- [x] Use the `paho-mqtt` client (as in `MQTTHandler`) for broker communication.

### Required Modules/Classes
- [x] `argparse` or `click` (for CLI parsing)
- [x] `paho.mqtt.client` (for MQTT pub/sub)
- [x] Logging utilities

### Implementation Steps
- [x] Parse CLI arguments: mode (send/receive), broker address, topic(s), message, QoS, etc.
- [x] Initialize MQTT client and connect to broker.
- [x] For send mode: publish message to topic.
- [x] For receive mode: subscribe to topic(s), print/decrypt incoming messages.
- [x] Add basic logging and error handling.
- [x] Test send/receive modes and error handling. Add usage examples to README.

---

## Phase 2: Meshtastic Device Management Integration
### Summary
- [ ] Integrate with the Meshtastic Python API for direct device communication (serial, TCP, BLE).
- [ ] Support hybrid operation: direct device and MQTT bridge.
- [ ] Allow CLI to discover, connect, and manage Meshtastic nodes using `DeviceManager` from `direct_meshtastic.py`.

### Required Modules/Classes
- [ ] `meshtastic`, `meshtastic.serial_interface`, `meshtastic.tcp_interface`, `meshtastic.ble_interface`
- [ ] `DeviceManager` and `DeviceInterface` (from backend)
- [ ] Asyncio for device discovery/connection

### Implementation Steps
- [ ] Add CLI options for connection type and parameters.
- [ ] Use `DeviceManager` to discover and connect to devices.
- [ ] Support sending/receiving messages via direct device connection.
- [ ] Bridge messages between device and MQTT broker as needed.

---

## Phase 3: Message Decryption & Relay Logic
### Summary
- [ ] Implement logic to decrypt messages received from the Meshtastic node via the MQTT bridge.
- [ ] Ensure messages are relayed between the device and broker, with decryption applied as needed.

### Required Modules/Classes
- [ ] Meshtastic message decoding utilities
- [ ] Existing message handler logic (see `meshtastic_integration.py`)

### Implementation Steps
- [ ] Subscribe to Meshtastic topics (e.g., `msh/...`) on the broker.
- [ ] On message receipt, use Meshtastic libraries to decode/decrypt payloads.
- [ ] Print/store decrypted messages.
- [ ] Optionally, relay messages to other systems or files.

---

## Phase 4: Daemonization & Advanced Features
### Summary
- [ ] Support running the utility as a background process (daemon/service).
- [ ] Add advanced features: config files, robust logging, error handling, reconnection logic, etc.

### Required Modules/Classes
- [ ] `daemonize` or `python-daemon` (optional)
- [ ] Config file parser (YAML, TOML, or JSON)
- [ ] Enhanced logging and monitoring

### Implementation Steps
- [ ] Add option to run as a daemon/service.
- [ ] Support config file for persistent settings.
- [ ] Implement reconnection and error recovery logic.
- [ ] Add monitoring endpoints or status reporting if needed.

---

## References
- [ ] [direct_meshtastic.py](../backend/direct_meshtastic.py)
- [ ] [meshtastic_integration.py](../backend/meshtastic_integration.py)
- [ ] [mqtt_handler.py](../backend/mqtt_handler.py)
- [ ] [Mosquitto MQTT Broker](../mosquitto/mosquitto.conf)
- [ ] [Meshtastic Python API](https://github.com/meshtastic/Meshtastic-python)

---

### Status Update (2024-06-09)
- Initial CLI skeleton created as `meshtastic_mqtt_cli.py` using `argparse`.
- `argparse` and `paho-mqtt` noted in requirements.txt.
- MQTT client logic, send/receive, and logging implemented in CLI script.
- CLI tested for send/receive and error handling. Usage examples added to README.
- Phase 1 complete. Ready for review or next phase.

This checklist should be used as a living document to guide the incremental development and testing of the Meshtastic MQTT CLI utility. Each phase can be developed and validated independently before moving to the next.
