# Phase 1: CLI Argument Parsing & Basic MQTT Pub/Sub (Granular Checklist)

This document provides a detailed, step-by-step breakdown for implementing Phase 1 of the Meshtastic MQTT CLI utility.

---

## Project Setup
- [x] Create a new Python script/module for the CLI utility (e.g., `meshtastic_mqtt_cli.py`).
- [x] Add required dependencies to `requirements.txt` (e.g., `paho-mqtt`, `click` or `argparse`).
- [ ] Set up a basic project structure if needed.

## CLI Argument Parsing
- [x] Choose a CLI parsing library (`argparse` or `click`).
- [x] Define CLI arguments:
  - [x] Mode: `send` or `receive`
  - [x] MQTT broker address (default: `localhost`)
  - [x] MQTT broker port (default: `1883`)
  - [x] Topic(s) to publish/subscribe
  - [x] Message content (for send mode)
  - [x] QoS level (optional)
  - [x] Retain flag (optional)
  - [x] Verbosity/logging level
  - [x] Help/version info
- [x] Implement argument validation and help messages.

## MQTT Client Initialization
- [x] Import and configure `paho.mqtt.client`.
- [x] Set up MQTT client callbacks (on_connect, on_message, on_disconnect, etc.).
- [x] Add logging for connection status and errors.

## Connect to MQTT Broker
- [x] Use parsed arguments to connect to the specified broker and port.
- [x] Handle connection errors and retries.
- [x] Log successful connection.

## Send Mode Implementation
- [x] Validate required arguments for send mode (topic, message).
- [x] Publish the message to the specified topic with selected QoS/retain.
- [x] Log the result of the publish operation.
- [x] Exit after publishing (unless in a loop/batch mode).

## Receive Mode Implementation
- [x] Validate required arguments for receive mode (topic(s)).
- [x] Subscribe to the specified topic(s) with selected QoS.
- [x] Print/log all received messages (raw payload for now).
- [x] Optionally, print message metadata (topic, timestamp, QoS, etc.).
- [x] Support graceful shutdown (e.g., Ctrl+C handler).

## Logging and Error Handling
- [x] Set up logging configuration (level, format).
- [x] Log all major actions, errors, and received messages.
- [x] Handle exceptions and provide user-friendly error messages.

## Testing and Validation
- [x] Test send mode with various topics and messages.
- [x] Test receive mode with multiple topics and message types.
- [x] Validate error handling for invalid arguments and broker issues.
- [x] Document usage examples in the README or CLI help.

---

### Status Update (2024-06-09)
- Initial CLI skeleton created as `meshtastic_mqtt_cli.py` using `argparse`.
- `argparse` and `paho-mqtt` noted in requirements.txt.
- MQTT client logic, send/receive, and logging implemented in CLI script.
- CLI tested for send/receive and error handling. Usage examples added to README.
- Phase 1 complete. Ready for review or next phase.
