# Phase 2: Meshtastic Device Management Integration (Granular Checklist)

This document provides a detailed, step-by-step breakdown for implementing Phase 2 of the Meshtastic MQTT CLI utility: integrating Meshtastic device management and supporting hybrid operation.

---

## Project Preparation
- [ ] Review and understand the Meshtastic Python API (`meshtastic`, `meshtastic.serial_interface`, `meshtastic.tcp_interface`, `meshtastic.ble_interface`).
- [ ] Review existing `DeviceManager` and `DeviceInterface` implementations in `direct_meshtastic.py`.
- [ ] Ensure all dependencies are listed in `requirements.txt`.

## CLI Enhancements for Device Management
- [ ] Add CLI options for:
  - [ ] Connection type (`serial`, `tcp`, `ble`)
  - [ ] Connection parameters (e.g., serial port, TCP host, BLE address/name)
  - [ ] Device discovery
  - [ ] Device selection (by ID or connection params)
  - [ ] Hybrid mode toggle (direct + MQTT bridge)
- [ ] Update help messages and argument validation for new options.

## Device Discovery & Connection
- [ ] Implement device discovery logic:
  - [ ] Serial device discovery (list available ports)
  - [ ] BLE device scanning (list available BLE devices)
  - [ ] TCP device discovery (if applicable)
- [ ] Present discovered devices to the user for selection.
- [ ] Use `DeviceManager` to connect to the selected device.
- [ ] Handle connection errors and provide user feedback.

## Device Management Operations
- [ ] Support listing all connected devices.
- [ ] Allow setting a default device for operations.
- [ ] Implement device disconnection logic.
- [ ] Provide status and info for each connected device (ID, type, params, connection state).

## Message Send/Receive via Device
- [ ] Support sending messages directly to a connected device (using Meshtastic API).
- [ ] Support receiving messages from the device and printing/logging them.
- [ ] Bridge messages between device and MQTT broker if in hybrid mode.
- [ ] Ensure message format compatibility between direct and MQTT modes.

## Async & Threading Considerations
- [ ] Use `asyncio` for device discovery and connection where required.
- [ ] Ensure thread safety when accessing shared device state.
- [ ] Handle graceful shutdown and cleanup of device connections.

## Logging and Error Handling
- [ ] Log all device management actions, errors, and state changes.
- [ ] Provide clear error messages for device connection/discovery issues.
- [ ] Handle exceptions from the Meshtastic API gracefully.

## Testing and Validation
- [ ] Test device discovery and connection for all supported types (serial, TCP, BLE).
- [ ] Test sending and receiving messages via direct device connection.
- [ ] Test hybrid mode (device + MQTT bridge) for message relay.
- [ ] Validate error handling for invalid device parameters and connection failures.
- [ ] Document usage examples for device management features in the README or CLI help.

---
