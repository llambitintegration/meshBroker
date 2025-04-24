# Meshtastic Integration Documentation

## Overview

The Meshtastic Integration component provides a bridge between Meshtastic mesh networks and the MQTT broker. It allows for:

- Persistent storage of Meshtastic node information
- Handling different types of Meshtastic messages
- Categorization and grouping of nodes
- Advanced message routing and processing
- Support for binary data
- Protocol conversion between JSON and Meshtastic binary formats

## Configuration

### Environment Variables

The following environment variables can be used to configure the Meshtastic integration:

| Variable | Description | Default |
|----------|-------------|---------|
| `MESHTASTIC_DB_PATH` | Path to the SQLite database for Meshtastic nodes | `mqtt_data/meshtastic_nodes.db` |
| `NODE_EXPIRATION_SECONDS` | Time in seconds after which a node is considered inactive | `3600` (1 hour) |

### MQTT Topics

The integration subscribes to the following Meshtastic MQTT topics:

- `msh/+/json/nodeid` - Node identity information
- `msh/+/json/position` - GPS position updates
- `msh/+/json/text` - Text messages
- `msh/+/json/telemetry` - Telemetry data (battery, environmental)
- `msh/+/json/heartbeat` - Periodic heartbeat messages
- `msh/+/binary` - Binary data messages

### Topic Structure

Meshtastic MQTT topics follow this structure:

```
msh/[node_id]/[format]/[message_type]
```

- `msh` - The Meshtastic prefix
- `node_id` - The ID of the node (or "broadcast" for all nodes)
- `format` - The message format (e.g., "json" or "binary")
- `message_type` - The type of message (e.g., "text", "position", "telemetry")

## API Endpoints

### Node Listing and Filtering

#### `GET /meshtastic/nodes`

Get a list of all known Meshtastic nodes with optional filtering.

Query Parameters:
- `active_only` (boolean, default: true) - Only return active nodes
- `group` (string, optional) - Filter by group name
- `category` (string, optional) - Filter by category

Response:
```json
{
  "nodes": [
    {
      "node_id": "!abc123",
      "name": "Node 1",
      "short_name": "N1",
      "hardware": "TBEAM",
      "group": "default",
      "category": "fixed",
      "last_seen": 1635123456.789,
      "message_count": 42,
      "is_active": true,
      "position": {
        "latitude": 45.123,
        "longitude": -122.456,
        "altitude": 100,
        "timestamp": 1635123450
      }
    }
  ]
}
```

### Node Details

#### `GET /meshtastic/nodes/{node_id}`

Get detailed information about a specific node.

Response:
```json
{
  "node_id": "!abc123",
  "name": "Node 1",
  "short_name": "N1",
  "hardware": "TBEAM",
  "group": "default",
  "category": "fixed",
  "last_seen": 1635123456.789,
  "last_heartbeat_time": 1635123456.0,
  "message_count": 42,
  "is_active": true,
  "position": {
    "latitude": 45.123,
    "longitude": -122.456,
    "altitude": 100,
    "timestamp": 1635123450
  },
  "telemetry": {
    "battery": 75,
    "voltage": 3.8,
    "temperature": 22.5
  },
  "messages": [
    {
      "text": "Hello world",
      "from_id": "!abc123",
      "to_id": "!def456",
      "timestamp": 1635123400
    }
  ]
}
```

### Node Grouping and Categorization

#### `PUT /meshtastic/nodes/{node_id}/group`

Update the group of a node.

Request:
```json
{
  "group": "mobile"
}
```

Response:
```json
{
  "status": "success",
  "message": "Updated group for node !abc123"
}
```

#### `PUT /meshtastic/nodes/{node_id}/category`

Update the category of a node.

Request:
```json
{
  "category": "vehicle"
}
```

Response:
```json
{
  "status": "success",
  "message": "Updated category for node !abc123"
}
```

### Messaging

#### `POST /meshtastic/nodes/{node_id}/message`

Send a text message to a specific node.

Request:
```json
{
  "text": "Hello from the server!",
  "source_id": "!server"
}
```

Response:
```json
{
  "status": "success",
  "message": "Message sent to node !abc123"
}
```

#### `POST /meshtastic/broadcast`

Broadcast a message to all nodes.

Request:
```json
{
  "text": "Attention all nodes!",
  "source_id": "!server"
}
```

Response:
```json
{
  "status": "success",
  "message": "Message broadcasted to all nodes"
}
```

### Statistics

#### `GET /meshtastic/stats`

Get statistics about Meshtastic nodes.

Response:
```json
{
  "nodes": {
    "total": 10,
    "active": 8,
    "inactive": 2
  },
  "last_update": 1635123456.789
}
```

## WebSocket API

The WebSocket endpoint (`/ws`) supports the following Meshtastic-related actions:

### Get All Nodes

```json
{
  "action": "get_nodes",
  "active_only": true,
  "group": "default",
  "category": "fixed"
}
```

### Get Node Details

```json
{
  "action": "get_node_details",
  "node_id": "!abc123"
}
```

### Send Message

```json
{
  "action": "send_message",
  "node_id": "!abc123",
  "text": "Hello from WebSocket",
  "source_id": "!server"
}
```

## Message Type Reference

### Node Info

Node identity information includes:
- Long name (user-friendly name)
- Short name (abbreviated name)
- Hardware model
- Other device-specific information

### Position

Position messages include:
- Latitude
- Longitude
- Altitude
- Timestamp

### Text Messages

Text messages include:
- Message text
- Sender ID
- Recipient ID (or "^all" for broadcast)
- Timestamp

### Telemetry

Telemetry data includes:
- Battery level
- Voltage
- Environmental data (temperature, humidity, etc.)
- Channel utilization statistics

### Heartbeat

Heartbeat messages indicate that a node is online and functioning. They are sent periodically and can include basic status information.

### Binary Data

Binary data messages can contain arbitrary binary payloads, typically using the Meshtastic ProtoBuf format for encoding complex data structures.

## Database Schema

The integration uses SQLite for data persistence with the following schema:

### Nodes Table

```sql
CREATE TABLE nodes (
    node_id TEXT PRIMARY KEY,
    last_seen REAL NOT NULL,
    message_count INTEGER NOT NULL,
    name TEXT NOT NULL,
    short_name TEXT NOT NULL,
    hardware TEXT NOT NULL,
    group_name TEXT NOT NULL,
    category TEXT NOT NULL,
    position_json TEXT,
    telemetry_json TEXT,
    heartbeat_json TEXT,
    last_heartbeat_time REAL NOT NULL,
    attributes_json TEXT,
    is_active INTEGER NOT NULL
)
```

### Messages Table

```sql
CREATE TABLE node_messages (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    node_id TEXT NOT NULL,
    text TEXT NOT NULL,
    from_id TEXT NOT NULL,
    to_id TEXT NOT NULL,
    timestamp INTEGER NOT NULL,
    FOREIGN KEY (node_id) REFERENCES nodes (node_id)
)
```

## Protocol Conversion

The `protocol_converter.py` module provides utilities for converting between different message formats:

- Binary to JSON conversion for Meshtastic protocol buffer messages
- JSON to binary conversion for creating Meshtastic-compatible messages
- Message format detection for automatic handling

## Troubleshooting

### Common Issues

1. **Node not appearing in the list**
   - Ensure the node is actively sending messages to the MQTT broker
   - Check the MQTT topic format (should be `msh/{node_id}/json/{message_type}`)
   - Verify that the broker is properly configured for Meshtastic topics

2. **Node marked as inactive**
   - Nodes are automatically marked as inactive after `NODE_EXPIRATION_SECONDS`
   - Check if the node is still online and sending heartbeats
   - Verify the node's connection to the mesh network

3. **Binary messages not properly decoded**
   - Ensure the Meshtastic protocol buffer modules are installed
   - Check that the message is using the correct binary format
   - Verify the topic structure matches the expected pattern 