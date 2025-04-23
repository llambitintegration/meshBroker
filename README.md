# Meshtastic MQTT Bridge

A complete solution for hosting a Mosquitto MQTT broker on Debian/Linux with a FastAPI backend and Node.js frontend for integration with Meshtastic nodes.

## Overview

This project provides a complete infrastructure for:

1. Running an MQTT broker (Mosquitto)
2. Processing and routing MQTT messages with a FastAPI backend
3. Visualizing and interacting with Meshtastic nodes via a web interface
4. Supporting the Sparkplug MQTT message format for industrial IoT

## Architecture

The system consists of three main components:

1. **Mosquitto MQTT Broker**: A lightweight message broker that implements the MQTT protocol, ideal for IoT devices with constrained resources.

2. **FastAPI Backend**: A Python-based API server that:
   - Connects to the MQTT broker
   - Processes messages
   - Provides REST API endpoints for frontend interaction
   - Manages Meshtastic node integration

3. **Node.js Frontend**: A web interface that:
   - Displays connected Meshtastic nodes
   - Shows message history
   - Provides visualizations of node activity
   - Allows sending messages to nodes
   - Includes an MQTT explorer for debugging

## Features

- Real-time message streaming via WebSocket
- Meshtastic node status monitoring
- Direct messaging to Meshtastic nodes
- Message history and visualization
- Topic-based subscription management
- Comprehensive MQTT explorer

## Installation

### Prerequisites

- Debian/Ubuntu Linux system
- Root access

### Automatic Installation

1. Clone the repository:
```bash
git clone https://github.com/yourusername/meshtastic-mqtt-bridge.git
cd meshtastic-mqtt-bridge
