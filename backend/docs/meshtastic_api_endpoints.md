# Meshtastic API Endpoints

This document catalogs all Meshtastic-related API endpoints in the current implementation. During code consolidation, refer to this document to ensure no functionality is lost.

## Endpoints in routers/meshtastic.py

| Endpoint | Method | Description | Request Body | Response |
|----------|--------|-------------|--------------|----------|
| `/meshtastic/nodes` | GET | Get all Meshtastic nodes with optional filtering | None | List of node objects |
| `/meshtastic/nodes/{node_id}` | GET | Get a specific Meshtastic node by ID | None | Node details object |
| `/meshtastic/discover` | POST | Discover available Meshtastic devices | `connection_type` | Task status object |
| `/meshtastic/discover/{task_id}` | GET | Get the status of a device discovery task | None | Task status object |
| `/meshtastic/devices` | GET | Get all connected Meshtastic devices | None | List of device objects |
| `/meshtastic/connect` | POST | Connect to a Meshtastic device | Connection request object | Connection status object |
| `/meshtastic/devices/{device_id}` | DELETE | Disconnect from a Meshtastic device | None | Status object |

## Endpoints in app.py

| Endpoint | Method | Description | Request Body | Response |
|----------|--------|-------------|--------------|----------|
| `/meshtastic/nodes` | GET | Get Meshtastic nodes with optional filtering | None | List of node objects |
| `/meshtastic/nodes/{node_id}` | GET | Get a specific Meshtastic node by ID | None | Node details object |
| `/meshtastic/nodes/{node_id}/group` | PUT | Update the group of a node | `group` | Status object |
| `/meshtastic/nodes/{node_id}/category` | PUT | Update the category of a node | `category` | Status object |
| `/meshtastic/nodes/{node_id}/message` | POST | Send a message to a specific node | Message object | Status object |
| `/meshtastic/broadcast` | POST | Broadcast a message to all nodes | Message object | Status object |
| `/meshtastic/stats` | GET | Get statistics about Meshtastic nodes | None | Stats object |

## Models and Objects

### NodeGroupUpdate
```json
{
  "group": "string"
}
```

### NodeCategoryUpdate
```json
{
  "category": "string"
}
```

### MeshMessage
```json
{
  "text": "string",
  "source_id": "string" // Optional
}
```

### ConnectionRequest
```json
{
  "connection_type": "string", // "serial", "tcp", "ble"
  "connection_params": {
    // Connection parameters depend on the connection type
  },
  "device_id": "string" // Optional
}
```

## Consolidation Strategy

When consolidating these endpoints, follow these guidelines:

1. **Endpoints with the same path and method**: Keep the more feature-complete implementation and ensure it provides all functionality of the other version.

2. **Unique endpoints**: Move these to the appropriate router modules without changes.

3. **Authentication and rate limiting**: Ensure all consolidated endpoints maintain their authentication requirements and rate limits.

4. **WebSocket endpoints**: Ensure all WebSocket functionality related to Meshtastic is properly implemented in the ws router.

5. **Dependency injection**: Update all endpoints to use dependency injection for the MQTT handler and other shared resources.

## Validation Checklist

After consolidation, verify that:

- [ ] All endpoints are accessible and return the expected responses
- [ ] Authentication and rate limiting work as expected
- [ ] WebSocket functionality for Meshtastic works correctly
- [ ] All response formats are consistent
- [ ] All error handling is consistent
- [ ] Documentation is updated to reflect the new structure 