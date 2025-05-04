# Phase 3: Message Decryption & Relay Logic (Granular Checklist)

This document provides a detailed, step-by-step breakdown for implementing Phase 3 of the Meshtastic MQTT CLI utility: message decryption and relay logic.

---

## Preparation & Research
- [ ] Review Meshtastic message formats and encryption/decryption mechanisms.
- [ ] Identify or implement utilities for decoding/decrypting Meshtastic messages (using the Python API or custom logic).
- [ ] Review existing message handler logic (see `meshtastic_integration.py`).

## CLI Enhancements for Decryption/Relay
- [ ] Add CLI options for:
  - [ ] Enabling/disabling message decryption
  - [ ] Output format (raw, decoded, JSON, etc.)
  - [ ] Output destination (stdout, file, etc.)
  - [ ] Relay toggle (whether to forward messages to other systems)
- [ ] Update help messages and argument validation for new options.

## MQTT Subscription & Message Handling
- [ ] Subscribe to relevant Meshtastic topics (e.g., `msh/...`) on the broker.
- [ ] Implement callback for handling incoming MQTT messages.
- [ ] Parse and extract payloads from received messages.

## Message Decryption & Decoding
- [ ] Use Meshtastic libraries/utilities to decode/decrypt message payloads.
- [ ] Handle different message types (text, telemetry, position, binary, etc.).
- [ ] Log or print decrypted/decoded messages in the selected format.
- [ ] Handle decryption errors gracefully and provide feedback.

## Message Relay Logic
- [ ] If relay is enabled, forward decrypted/decoded messages to:
  - [ ] Other MQTT topics
  - [ ] Files or external systems (if required)
- [ ] Ensure relayed messages maintain correct format and metadata.

## Logging and Error Handling
- [ ] Log all message processing actions, errors, and decryption attempts.
- [ ] Provide clear error messages for decryption/decoding failures.
- [ ] Handle exceptions from Meshtastic libraries gracefully.

## Testing and Validation
- [ ] Test decryption/decoding for all supported message types.
- [ ] Test message relay to MQTT and/or files.
- [ ] Validate error handling for malformed or encrypted messages.
- [ ] Document usage examples for decryption and relay features in the README or CLI help.

---
