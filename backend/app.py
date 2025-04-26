from fastapi import FastAPI, HTTPException, WebSocket, WebSocketDisconnect, Query, BackgroundTasks, Request, status
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
import json
import asyncio
from typing import Dict, List, Optional, Any
import logging
from pydantic import BaseModel
import time

from backend.mqtt.mqtt_handler import MQTTHandler
import backend.meshtastic_integration as meshtastic_integration
from backend.config import settings, configure_logging
from backend.messaging.message_processor import MessageFilter, TopicFilter, PayloadFilter, FilterChain, FilterAction
from backend.messaging.message_queue import MessageQueue, FlowController

# Authentication and new components
from backend.models.user import User, Role
from backend.auth import JWTBearer, AdminRequired, APIKeyAuth, api_key_required
from backend.auth.routes import router as auth_router
from backend.mqtt.settings_routes import router as mqtt_settings_router
from backend.mqtt.settings_routes import set_mqtt_handler
from backend.ws.routes import router as ws_router
from backend.ws.connection_manager import ConnectionManager as EnhancedConnectionManager
from backend.ws.channel_manager import ChannelManager
from backend.ws.connection_models import WSMessage, WSMessageType
from backend.auth.rate_limiter import RateLimiter, get_limiter
from slowapi.errors import RateLimitExceeded
from slowapi import Limiter
from slowapi.util import get_remote_address

# Add monitoring imports - updated to import directly from routes to avoid circular imports
from backend.monitoring.routes import router as monitoring_router
from backend.monitoring.middleware import add_metrics_middleware
from backend.monitoring.monitoring import get_metrics_manager

# Add status router
from backend.routers.status import router as status_router
from backend.routers.status import set_mqtt_handler as set_status_mqtt_handler

# Configure logging
log_level = configure_logging()
logger = logging.getLogger(__name__)

# Initialize app and limiter
app = FastAPI(title="Meshtastic MQTT Bridge")
limiter = get_limiter()

# Configure CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.CORS_ORIGINS,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Add metrics middleware
add_metrics_middleware(app)

# Initialize MQTT Handler
mqtt_handler = MQTTHandler(
    broker_host=settings.MQTT_BROKER_HOST,
    broker_port=settings.MQTT_BROKER_PORT,
    client_id=settings.MQTT_CLIENT_ID,
    username=settings.MQTT_USERNAME if settings.MQTT_USERNAME else None,
    password=settings.MQTT_PASSWORD if settings.MQTT_PASSWORD else None,
    use_tls=settings.MQTT_USE_TLS,
    keepalive=settings.MQTT_KEEPALIVE,
    default_qos=settings.MQTT_QOS,
    persistence_enabled=settings.PERSISTENCE_ENABLED,
    persistence_path=settings.PERSISTENCE_PATH,
    max_queue_size=settings.MAX_QUEUE_SIZE,
    worker_count=settings.QUEUE_WORKER_COUNT,
    max_retries=settings.MAX_RETRIES
)

# Set MQTT handler reference in the settings router
set_mqtt_handler(mqtt_handler)

# Set MQTT handler reference in the status router
set_status_mqtt_handler(mqtt_handler)

# Legacy WebSocket connection manager (kept for backward compatibility)
class ConnectionManager:
    def __init__(self):
        self.active_connections: List[WebSocket] = []
        self.ping_interval = settings.WS_PING_INTERVAL
        self.ping_task = None

    async def connect(self, websocket: WebSocket):
        await websocket.accept()
        self.active_connections.append(websocket)
        logger.info(f"New WebSocket connection established. Total active connections: {len(self.active_connections)}")
        
        # Start ping task if not already running
        if self.ping_task is None or self.ping_task.done():
            self.ping_task = asyncio.create_task(self._ping_connections())

    def disconnect(self, websocket: WebSocket):
        if websocket in self.active_connections:
            self.active_connections.remove(websocket)
            logger.info(f"WebSocket connection closed. Remaining active connections: {len(self.active_connections)}")
        
        # Cancel ping task if no more connections
        if not self.active_connections and self.ping_task is not None:
            self.ping_task.cancel()

    async def broadcast(self, message: str):
        if not self.active_connections:
            logger.debug("No active connections for broadcast")
            return
            
        logger.debug(f"Broadcasting message to {len(self.active_connections)} connections")
        
        # Use a copy of the connections list to avoid modification during iteration
        connections_to_remove = []
        
        for connection in self.active_connections:
            try:
                await connection.send_text(message)
            except WebSocketDisconnect:
                logger.debug("Connection already closed during broadcast")
                connections_to_remove.append(connection)
            except Exception as e:
                logger.error(f"Error broadcasting message: {e}")
                connections_to_remove.append(connection)
        
        # Clean up any disconnected connections
        for connection in connections_to_remove:
            if connection in self.active_connections:
                self.disconnect(connection)
    
    async def _ping_connections(self):
        """Send periodic pings to keep connections alive"""
        try:
            while self.active_connections:
                await asyncio.sleep(self.ping_interval)
                
                if not self.active_connections:
                    break
                
                ping_message = json.dumps({"type": "ping", "timestamp": int(time.time())})
                await self.broadcast(ping_message)
                
        except asyncio.CancelledError:
            logger.debug("Ping task cancelled")
        except Exception as e:
            logger.error(f"Error in ping task: {e}")


# Legacy connection manager for backward compatibility
manager = ConnectionManager()

# Get the enhanced connection manager for forwarding messages
from backend.ws.routes import connection_manager as enhanced_manager
from backend.ws.routes import channel_manager

# Models
class MQTTMessage(BaseModel):
    topic: str
    payload: str
    qos: Optional[int] = None
    retain: Optional[bool] = False


class TopicSubscription(BaseModel):
    topic: str
    qos: Optional[int] = None


class FilterConfig(BaseModel):
    name: str
    topic_pattern: Optional[str] = None
    payload_path: Optional[str] = None
    value_pattern: Optional[str] = None
    action: str = "ACCEPT"  # "ACCEPT", "REJECT", "TRANSFORM"


class FilterChainConfig(BaseModel):
    name: str
    topic_pattern: str
    filters: List[FilterConfig]


class BrokerConfig(BaseModel):
    name: str
    host: str
    port: int


# Rate limit middleware
@app.exception_handler(RateLimitExceeded)
async def rate_limit_handler(request, exc):
    return JSONResponse(
        status_code=status.HTTP_429_TOO_MANY_REQUESTS,
        content={"detail": "Rate limit exceeded"}
    )

# Include routers
app.include_router(auth_router)
app.include_router(mqtt_settings_router)
app.include_router(ws_router)
app.include_router(monitoring_router)
app.include_router(status_router)  # Add the status router

# Add global variable to track MQTT connection status
mqtt_status = {"status": "disconnected"}

# Routes
@app.on_event("startup")
async def startup_event():
    """Connect to MQTT broker on startup"""
    logger.info("Starting Meshtastic MQTT Bridge application")
    
    # Initialize user database
    User.init_db()
    
    # Get the current event loop and set it in the MQTT handler
    current_loop = asyncio.get_running_loop()
    mqtt_handler.set_event_loop(current_loop)
    
    # Connect to MQTT broker
    mqtt_handler.connect()
    
    # Set callback for received messages
    mqtt_handler.set_message_callback(on_mqtt_message)
    
    # Set callback for connection status changes
    mqtt_handler.set_connection_callback(on_mqtt_connection_change)
    
    # Start the Meshtastic integration
    await meshtastic_integration.initialize(mqtt_handler)
    
    # Initialize metrics manager
    metrics_manager = get_metrics_manager()
    
    logger.info("Application startup complete")


@app.on_event("shutdown")
async def shutdown_event():
    """Disconnect from MQTT broker on shutdown"""
    logger.info("Shutting down Meshtastic MQTT Bridge application")
    
    # Disconnect all WebSocket connections
    await enhanced_manager.disconnect_all()
    
    # Disconnect from MQTT broker
    mqtt_handler.disconnect()
    
    # Stop metrics collection
    metrics_manager = get_metrics_manager()
    metrics_manager.stop()


async def on_mqtt_message(topic, payload):
    """Callback for MQTT messages to broadcast to WebSocket clients"""
    try:
        payload_str = payload.decode("utf-8") if isinstance(payload, bytes) else payload
        
        # Legacy broadcast
        message = {
            "topic": topic,
            "payload": payload_str,
            "timestamp": asyncio.get_event_loop().time()
        }
        await manager.broadcast(json.dumps(message))
        
        # Enhanced broadcast via channel manager
        ws_message = WSMessage(
            type=WSMessageType.MESSAGE,
            topic=topic,
            payload={"topic": topic, "payload": payload_str}
        )
        
        # Get subscribers
        subscribers = channel_manager.get_subscribers(topic)
        
        # Send to all subscribers
        for connection_id in subscribers:
            await enhanced_manager.send_personal_message(ws_message, connection_id)
            
    except Exception as e:
        logger.error(f"Error processing MQTT message for broadcast: {e}")


@app.post("/publish", status_code=200)
@limiter.limit("20/minute")
async def publish_message(message: MQTTMessage, request: Request):
    """Publish a message to an MQTT topic"""
    try:
        logger.info(f"Publishing message to topic {message.topic}")
        result = mqtt_handler.publish(
            message.topic, message.payload, message.qos, message.retain
        )
        if result is None:
            error_msg = "Failed to publish message"
            logger.error(error_msg)
            raise HTTPException(status_code=500, detail=error_msg)
        return {"status": "success", "message": "Message published successfully", "message_id": result}
    except Exception as e:
        logger.error(f"Error publishing message: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/subscribe", status_code=200)
@limiter.limit("10/minute")
async def subscribe_topic(subscription: TopicSubscription, request: Request):
    """Subscribe to an MQTT topic"""
    try:
        logger.info(f"Subscribing to topic {subscription.topic}")
        result = mqtt_handler.subscribe(subscription.topic, subscription.qos)
        if not result or result[0] != 0:
            error_msg = f"Failed to subscribe: {result[0] if result else 'Not connected'}"
            logger.error(error_msg)
            raise HTTPException(status_code=500, detail=error_msg)
        return {"status": "success", "message": f"Subscribed to {subscription.topic}"}
    except Exception as e:
        logger.error(f"Error subscribing to topic: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/unsubscribe", status_code=200)
@limiter.limit("10/minute")
async def unsubscribe_topic(subscription: TopicSubscription, request: Request):
    """Unsubscribe from an MQTT topic"""
    try:
        logger.info(f"Unsubscribing from topic {subscription.topic}")
        result = mqtt_handler.unsubscribe(subscription.topic)
        if not result or result[0] != 0:
            error_msg = f"Failed to unsubscribe: {result[0] if result else 'Not connected'}"
            logger.error(error_msg)
            raise HTTPException(status_code=500, detail=error_msg)
        return {"status": "success", "message": f"Unsubscribed from {subscription.topic}"}
    except Exception as e:
        logger.error(f"Error unsubscribing from topic: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/topics")
@limiter.limit("30/minute")
async def get_topics(request: Request):
    """Get list of currently subscribed topics"""
    topics = mqtt_handler.get_subscribed_topics()
    logger.debug(f"Retrieved {len(topics)} subscribed topics")
    return {"topics": topics}


@app.get("/broker_status")
@limiter.limit("30/minute")
async def get_broker_status(request: Request):
    """Get the current status of the MQTT broker connection"""
    global mqtt_status
    
    # Add timestamp and API status to the status info
    status_info = {
        **mqtt_status,
        "timestamp": int(time.time()),
        "api": {
            "status": "connected",
            "timestamp": int(time.time())
        }
    }
    
    return status_info


@app.get("/queue_stats")
@limiter.limit("30/minute")
async def get_queue_stats(request: Request):
    """Get message queue statistics"""
    queue_stats = mqtt_handler.get_queue_stats()
    delivery_stats = mqtt_handler.get_delivery_stats()
    return {
        "queue": queue_stats,
        "delivery": delivery_stats
    }


@app.post("/process_offline_messages", status_code=200)
@limiter.limit("5/minute")
async def process_offline_messages(background_tasks: BackgroundTasks, request: Request):
    """Process messages stored for offline operation"""
    if not mqtt_handler.is_connected():
        raise HTTPException(status_code=503, detail="Not connected to MQTT broker")
    
    # Run processing in background to avoid blocking the API
    background_tasks.add_task(mqtt_handler.process_offline_messages)
    return {"status": "success", "message": "Processing offline messages in background"}


@app.post("/filters/add", status_code=200)
@limiter.limit("10/minute")
async def add_filter_chain(config: FilterChainConfig, request: Request):
    """Add a filter chain for a specific topic pattern"""
    try:
        # Create filter chain
        filter_chain = FilterChain(config.name)
        
        # Add filters to chain
        for filter_config in config.filters:
            if filter_config.topic_pattern:
                filter_obj = TopicFilter(
                    name=filter_config.name,
                    pattern=filter_config.topic_pattern,
                    action=FilterAction(filter_config.action)
                )
                filter_chain.add_filter(filter_obj)
            
            elif filter_config.payload_path and filter_config.value_pattern:
                filter_obj = PayloadFilter(
                    name=filter_config.name,
                    path=filter_config.payload_path,
                    pattern=filter_config.value_pattern,
                    action=FilterAction(filter_config.action)
                )
                filter_chain.add_filter(filter_obj)
            
            else:
                logger.warning(f"Invalid filter configuration: {filter_config}")
        
        # Register filter chain
        mqtt_handler.register_filter_chain(config.topic_pattern, filter_chain)
        
        logger.info(f"Added filter chain '{config.name}' for topic pattern '{config.topic_pattern}' with {len(config.filters)} filters")
        
        return {"status": "success", "message": f"Added filter chain '{config.name}' with {len(config.filters)} filters"}
    except Exception as e:
        logger.error(f"Error adding filter chain: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/meshtastic/nodes")
@limiter.limit("30/minute")
async def get_meshtastic_nodes(
    request: Request,
    active_only: bool = True, 
    group: Optional[str] = None, 
    category: Optional[str] = None
):
    """Get all Meshtastic nodes"""
    try:
        nodes = meshtastic_integration.get_nodes(active_only, group, category)
        
        # Convert nodes to dict representation
        node_list = []
        for node in nodes:
            node_dict = {
                "node_id": node.node_id,
                "name": node.name,
                "short_name": node.short_name,
                "hardware": node.hardware,
                "group": node.group,
                "category": node.category,
                "is_active": node.is_active,
                "last_seen": node.last_seen,
                "message_count": node.message_count
            }
            
            # Add position if available
            if node.position:
                node_dict["position"] = {
                    "latitude": node.position.latitude,
                    "longitude": node.position.longitude,
                    "altitude": node.position.altitude,
                    "timestamp": node.position.timestamp
                }
            
            node_list.append(node_dict)
        
        return {"nodes": node_list}
    except Exception as e:
        logger.error(f"Error retrieving Meshtastic nodes: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/meshtastic/nodes/{node_id}")
@limiter.limit("30/minute")
async def get_meshtastic_node(node_id: str, request: Request):
    """Get a specific Meshtastic node by ID"""
    try:
        node = meshtastic_integration.get_node(node_id)
        if not node:
            raise HTTPException(status_code=404, detail=f"Node {node_id} not found")
        
        # Convert node to dict representation
        node_dict = {
            "node_id": node.node_id,
            "name": node.name,
            "short_name": node.short_name,
            "hardware": node.hardware,
            "group": node.group,
            "category": node.category,
            "is_active": node.is_active,
            "last_seen": node.last_seen,
            "message_count": node.message_count
        }
        
        # Add position if available
        if node.position:
            node_dict["position"] = {
                "latitude": node.position.latitude,
                "longitude": node.position.longitude,
                "altitude": node.position.altitude,
                "timestamp": node.position.timestamp
            }
        
        # Add telemetry if available
        if node.telemetry:
            node_dict["telemetry"] = node.telemetry
        
        # Add heartbeat info if available
        if node.heartbeat:
            node_dict["heartbeat"] = node.heartbeat
            node_dict["last_heartbeat_time"] = node.last_heartbeat_time
        
        # Add recent messages
        node_dict["messages"] = [
            {
                "text": msg.text,
                "from_id": msg.from_id,
                "to_id": msg.to_id,
                "timestamp": msg.timestamp
            }
            for msg in node.messages
        ]
        
        return node_dict
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error retrieving Meshtastic node {node_id}: {e}")
        raise HTTPException(status_code=500, detail=str(e))


class NodeGroupUpdate(BaseModel):
    group: str


class NodeCategoryUpdate(BaseModel):
    category: str


@app.put("/meshtastic/nodes/{node_id}/group")
@limiter.limit("10/minute")
async def update_node_group(node_id: str, update: NodeGroupUpdate, request: Request):
    """Update a node's group"""
    try:
        result = meshtastic_integration.update_node_group(node_id, update.group)
        if not result:
            raise HTTPException(status_code=404, detail=f"Node {node_id} not found")
        
        return {"status": "success", "message": f"Updated group for node {node_id}"}
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error updating group for node {node_id}: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.put("/meshtastic/nodes/{node_id}/category")
@limiter.limit("10/minute")
async def update_node_category(node_id: str, update: NodeCategoryUpdate, request: Request):
    """Update a node's category"""
    try:
        result = meshtastic_integration.update_node_category(node_id, update.category)
        if not result:
            raise HTTPException(status_code=404, detail=f"Node {node_id} not found")
        
        return {"status": "success", "message": f"Updated category for node {node_id}"}
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error updating category for node {node_id}: {e}")
        raise HTTPException(status_code=500, detail=str(e))


class MeshMessage(BaseModel):
    text: str
    source_id: Optional[str] = None


@app.post("/meshtastic/nodes/{node_id}/message")
@limiter.limit("20/minute")
async def send_node_message(node_id: str, message: MeshMessage, request: Request):
    """Send a message to a specific node"""
    try:
        result = meshtastic_integration.send_message_to_node(mqtt_handler, node_id, message.text, message.source_id)
        if not result:
            raise HTTPException(status_code=500, detail="Failed to send message")
        
        return {"status": "success", "message": f"Message sent to node {node_id}"}
    except Exception as e:
        logger.error(f"Error sending message to node {node_id}: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/meshtastic/broadcast")
@limiter.limit("10/minute")
async def broadcast_node_message(message: MeshMessage, request: Request):
    """Broadcast a message to all nodes"""
    try:
        result = meshtastic_integration.broadcast_message(mqtt_handler, message.text, message.source_id)
        if not result:
            raise HTTPException(status_code=500, detail="Failed to broadcast message")
        
        return {"status": "success", "message": "Message broadcast to all nodes"}
    except Exception as e:
        logger.error(f"Error broadcasting message: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/meshtastic/stats")
@api_key_required
async def get_meshtastic_stats(request: Request):
    """Get Meshtastic integration statistics"""
    try:
        stats = meshtastic_integration.get_node_count()
        return stats
    except Exception as e:
        logger.error(f"Error retrieving Meshtastic stats: {e}")
        raise HTTPException(status_code=500, detail="Error retrieving Meshtastic statistics")


# New endpoints for direct device management

@app.get("/meshtastic/devices")
@api_key_required
async def get_meshtastic_devices(request: Request):
    """Get all connected Meshtastic devices"""
    try:
        devices = meshtastic_integration.get_connected_devices()
        return devices
    except Exception as e:
        logger.error(f"Error retrieving Meshtastic devices: {e}")
        raise HTTPException(status_code=500, detail="Error retrieving Meshtastic devices")

@app.get("/meshtastic/discover")
@api_key_required
async def discover_meshtastic_devices(
    request: Request,
    connection_type: str = "all"
):
    """Discover available Meshtastic devices"""
    try:
        if connection_type not in ["serial", "ble", "all"]:
            raise HTTPException(status_code=400, detail="Invalid connection type. Must be 'serial', 'ble', or 'all'")
        
        devices = await meshtastic_integration.discover_devices(connection_type)
        return devices
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error discovering Meshtastic devices: {e}")
        raise HTTPException(status_code=500, detail="Error discovering Meshtastic devices")

class DeviceConnectionRequest(BaseModel):
    connection_type: str
    connection_params: Dict[str, Any]
    device_id: Optional[str] = None

@app.post("/meshtastic/devices")
@api_key_required
async def connect_meshtastic_device(
    request: Request,
    connection_request: DeviceConnectionRequest
):
    """Connect to a Meshtastic device"""
    try:
        if connection_request.connection_type not in ["serial", "tcp", "ble"]:
            raise HTTPException(status_code=400, detail="Invalid connection type. Must be 'serial', 'tcp', or 'ble'")
        
        device_id = await meshtastic_integration.connect_device(
            connection_request.connection_type,
            connection_request.connection_params,
            connection_request.device_id
        )
        
        if not device_id:
            raise HTTPException(status_code=500, detail="Failed to connect to device")
        
        return {"device_id": device_id, "status": "connected"}
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error connecting to Meshtastic device: {e}")
        raise HTTPException(status_code=500, detail="Error connecting to Meshtastic device")

@app.delete("/meshtastic/devices/{device_id}")
@api_key_required
async def disconnect_meshtastic_device(
    request: Request,
    device_id: str
):
    """Disconnect from a Meshtastic device"""
    try:
        success = await meshtastic_integration.disconnect_device(device_id)
        
        if not success:
            raise HTTPException(status_code=404, detail=f"Device {device_id} not found or already disconnected")
        
        return {"device_id": device_id, "status": "disconnected"}
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error disconnecting from Meshtastic device: {e}")
        raise HTTPException(status_code=500, detail="Error disconnecting from Meshtastic device")

@app.get("/meshtastic/devices/{device_id}/config")
@api_key_required
async def get_meshtastic_device_config(
    request: Request,
    device_id: Optional[str] = None
):
    """Get device configuration"""
    try:
        config = meshtastic_integration.get_device_config(device_id)
        
        if not config:
            raise HTTPException(status_code=404, detail=f"Device {device_id} not found or has no configuration")
        
        return config
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error retrieving Meshtastic device configuration: {e}")
        raise HTTPException(status_code=500, detail="Error retrieving Meshtastic device configuration")

class ConfigUpdateRequest(BaseModel):
    key: str
    value: Any

@app.post("/meshtastic/devices/{device_id}/config")
@api_key_required
async def set_meshtastic_device_config(
    request: Request,
    config_update: ConfigUpdateRequest,
    device_id: Optional[str] = None
):
    """Set device configuration"""
    try:
        success = meshtastic_integration.set_device_config(
            config_update.key,
            config_update.value,
            device_id
        )
        
        if not success:
            raise HTTPException(status_code=404, detail=f"Device {device_id} not found or configuration update failed")
        
        return {"status": "success", "message": f"Configuration {config_update.key} updated successfully"}
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error updating Meshtastic device configuration: {e}")
        raise HTTPException(status_code=500, detail="Error updating Meshtastic device configuration")

@app.get("/meshtastic/devices/{device_id}/channels")
@api_key_required
async def get_meshtastic_device_channels(
    request: Request,
    device_id: Optional[str] = None
):
    """Get device channel settings"""
    try:
        channels = meshtastic_integration.get_device_channels(device_id)
        
        if not channels:
            raise HTTPException(status_code=404, detail=f"Device {device_id} not found or has no channels")
        
        return channels
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error retrieving Meshtastic device channels: {e}")
        raise HTTPException(status_code=500, detail="Error retrieving Meshtastic device channels")

class ChannelUpdateRequest(BaseModel):
    settings: Dict[str, Any]
    channel_index: int = 0

@app.post("/meshtastic/devices/{device_id}/channels")
@api_key_required
async def set_meshtastic_device_channel(
    request: Request,
    channel_update: ChannelUpdateRequest,
    device_id: Optional[str] = None
):
    """Set device channel settings"""
    try:
        success = meshtastic_integration.set_device_channel(
            channel_update.settings,
            channel_update.channel_index,
            device_id
        )
        
        if not success:
            raise HTTPException(status_code=404, detail=f"Device {device_id} not found or channel update failed")
        
        return {"status": "success", "message": f"Channel {channel_update.channel_index} updated successfully"}
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error updating Meshtastic device channel: {e}")
        raise HTTPException(status_code=500, detail="Error updating Meshtastic device channel")

# Legacy WebSocket endpoint (kept for backward compatibility)
@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    """WebSocket endpoint for real-time updates"""
    await manager.connect(websocket)
    
    # Send initial status information
    initial_status = {
        "type": "status",
        "data": {
            "api": {
                "status": "connected",
                "timestamp": int(time.time())
            },
            "mqtt": {
                "status": "connected" if mqtt_handler.is_connected() else "disconnected",
                "timestamp": int(time.time())
            },
            "broker": mqtt_handler.get_broker_status() if mqtt_handler else {"status": "UNKNOWN"}
        }
    }
    
    try:
        await websocket.send_text(json.dumps(initial_status))
        while True:
            # Keep the connection alive - wait for messages
            data = await asyncio.wait_for(websocket.receive_text(), timeout=settings.WS_PING_TIMEOUT)
            message = json.loads(data)
            
            # Handle different message types
            if message.get("type") == "ping":
                await websocket.send_text(json.dumps({"type": "pong", "timestamp": int(time.time())}))
            elif message.get("type") == "request_status":
                # Send updated status if requested
                status_update = {
                    "type": "status",
                    "data": {
                        "api": {
                            "status": "connected",
                            "timestamp": int(time.time())
                        },
                        "mqtt": {
                            "status": "connected" if mqtt_handler.is_connected() else "disconnected",
                            "timestamp": int(time.time())
                        },
                        "broker": mqtt_handler.get_broker_status() if mqtt_handler else {"status": "UNKNOWN"}
                    }
                }
                await websocket.send_text(json.dumps(status_update))
    except WebSocketDisconnect:
        logger.info("WebSocket client disconnected normally")
    except asyncio.TimeoutError:
        logger.info("WebSocket connection timed out")
    except json.JSONDecodeError:
        logger.warning("Received invalid JSON data on WebSocket")
    except Exception as e:
        logger.error(f"Error in WebSocket connection: {e}")
    finally:
        manager.disconnect(websocket)


# Add a new function to handle MQTT connection status changes
async def on_mqtt_connection_change(connected: bool):
    """Callback for MQTT connection status changes"""
    global mqtt_status
    
    if connected:
        mqtt_status["status"] = "connected"
        logger.info("MQTT broker connection established")
    else:
        mqtt_status["status"] = "disconnected"
        logger.warning("MQTT broker connection lost")
    
    # Broadcast connection status to all websocket clients
    status_message = {
        "type": "mqtt_status",
        "data": mqtt_status
    }
    
    # Serialize the JSON before broadcasting
    json_message = json.dumps(status_message)
    
    try:
        # Broadcast to enhanced manager connections
        await enhanced_manager.broadcast(json_message)
    except Exception as e:
        logger.error(f"Error broadcasting MQTT status: {e}")


if __name__ == "__main__":
    import uvicorn
    uvicorn.run("app:app", host=settings.API_HOST, port=settings.API_PORT, reload=True)
