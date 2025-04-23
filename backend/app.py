from fastapi import FastAPI, HTTPException, WebSocket, WebSocketDisconnect, Query, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
import json
import asyncio
from typing import Dict, List, Optional, Any
import logging
from pydantic import BaseModel

from mqtt_handler import MQTTHandler
import meshtastic_integration
from config import settings, configure_logging
from message_processor import MessageFilter, TopicFilter, PayloadFilter, FilterChain, FilterAction
from message_queue import MessageQueue, FlowController

# Configure logging
log_level = configure_logging()
logger = logging.getLogger(__name__)

app = FastAPI(title="Meshtastic MQTT Bridge")

# Configure CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.CORS_ORIGINS,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

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

# WebSocket connection manager
class ConnectionManager:
    def __init__(self):
        self.active_connections: List[WebSocket] = []

    async def connect(self, websocket: WebSocket):
        await websocket.accept()
        self.active_connections.append(websocket)
        logger.info(f"New WebSocket connection established. Total active connections: {len(self.active_connections)}")

    def disconnect(self, websocket: WebSocket):
        self.active_connections.remove(websocket)
        logger.info(f"WebSocket connection closed. Remaining active connections: {len(self.active_connections)}")

    async def broadcast(self, message: str):
        if not self.active_connections:
            logger.debug("No active connections for broadcast")
            return
            
        logger.debug(f"Broadcasting message to {len(self.active_connections)} connections")
        for connection in self.active_connections:
            try:
                await connection.send_text(message)
            except Exception as e:
                logger.error(f"Error broadcasting message: {e}")


manager = ConnectionManager()

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


# Routes
@app.on_event("startup")
async def startup_event():
    """Connect to MQTT broker on startup"""
    logger.info("Starting Meshtastic MQTT Bridge application")
    
    # Get the current event loop and set it in the MQTT handler
    current_loop = asyncio.get_running_loop()
    mqtt_handler.set_event_loop(current_loop)
    
    # Connect to MQTT broker
    mqtt_handler.connect()
    
    # Set callback for received messages
    mqtt_handler.set_message_callback(on_mqtt_message)
    
    # Start the Meshtastic integration
    await meshtastic_integration.initialize(mqtt_handler)
    
    logger.info("Application startup complete")


@app.on_event("shutdown")
async def shutdown_event():
    """Disconnect from MQTT broker on shutdown"""
    logger.info("Shutting down Meshtastic MQTT Bridge application")
    mqtt_handler.disconnect()


async def on_mqtt_message(topic, payload):
    """Callback for MQTT messages to broadcast to WebSocket clients"""
    try:
        payload_str = payload.decode("utf-8") if isinstance(payload, bytes) else payload
        message = {
            "topic": topic,
            "payload": payload_str,
            "timestamp": asyncio.get_event_loop().time()
        }
        await manager.broadcast(json.dumps(message))
    except Exception as e:
        logger.error(f"Error processing MQTT message for broadcast: {e}")


@app.post("/publish", status_code=200)
async def publish_message(message: MQTTMessage):
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
async def subscribe_topic(subscription: TopicSubscription):
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
async def unsubscribe_topic(subscription: TopicSubscription):
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
async def get_topics():
    """Get list of currently subscribed topics"""
    topics = mqtt_handler.get_subscribed_topics()
    logger.debug(f"Retrieved {len(topics)} subscribed topics")
    return {"topics": topics}


@app.get("/broker_status")
async def get_broker_status():
    """Check MQTT broker connection status"""
    is_connected = mqtt_handler.is_connected()
    broker_status = mqtt_handler.get_broker_status()
    logger.debug(f"MQTT broker status: {'connected' if is_connected else 'disconnected'}")
    return {
        "connection": "connected" if is_connected else "disconnected",
        "broker_status": broker_status
    }


@app.get("/queue_stats")
async def get_queue_stats():
    """Get message queue statistics"""
    queue_stats = mqtt_handler.get_queue_stats()
    delivery_stats = mqtt_handler.get_delivery_stats()
    return {
        "queue": queue_stats,
        "delivery": delivery_stats
    }


@app.post("/process_offline_messages", status_code=200)
async def process_offline_messages(background_tasks: BackgroundTasks):
    """Process messages stored for offline operation"""
    if not mqtt_handler.is_connected():
        raise HTTPException(status_code=503, detail="Not connected to MQTT broker")
    
    # Run processing in background to avoid blocking the API
    background_tasks.add_task(mqtt_handler.process_offline_messages)
    return {"status": "success", "message": "Processing offline messages in background"}


@app.post("/filters/add", status_code=200)
async def add_filter_chain(config: FilterChainConfig):
    """Add a filter chain for a specific topic pattern"""
    try:
        # Create filter chain
        filter_chain = FilterChain(config.name)
        
        # Add filters to chain
        for filter_config in config.filters:
            action = FilterAction[filter_config.action]
            
            if filter_config.topic_pattern:
                # Topic filter
                filter = TopicFilter(
                    name=filter_config.name,
                    topic_pattern=filter_config.topic_pattern,
                    action=action
                )
                filter_chain.add_filter(filter)
            
            elif filter_config.payload_path and filter_config.value_pattern:
                # Payload filter
                filter = PayloadFilter(
                    name=filter_config.name,
                    json_path=filter_config.payload_path,
                    value_pattern=filter_config.value_pattern,
                    action=action
                )
                filter_chain.add_filter(filter)
        
        # Add filter chain to handler
        mqtt_handler.add_message_filter(config.topic_pattern, filter_chain)
        
        return {
            "status": "success", 
            "message": f"Added filter chain '{config.name}' for topic pattern '{config.topic_pattern}'"
        }
    except Exception as e:
        logger.error(f"Error adding filter chain: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/meshtastic/nodes")
async def get_meshtastic_nodes():
    """Get list of known Meshtastic nodes"""
    nodes = meshtastic_integration.get_nodes()
    logger.debug(f"Retrieved {len(nodes)} Meshtastic nodes")
    return {"nodes": nodes}


@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    """WebSocket endpoint for real-time MQTT messages"""
    await manager.connect(websocket)
    try:
        while True:
            data = await websocket.receive_text()
            # Handle client messages if needed
            try:
                message = json.loads(data)
                if "action" in message:
                    if message["action"] == "get_topics":
                        await websocket.send_json({"topics": mqtt_handler.get_subscribed_topics()})
                    elif message["action"] == "get_status":
                        broker_status = mqtt_handler.get_broker_status()
                        await websocket.send_json({
                            "connection": "connected" if mqtt_handler.is_connected() else "disconnected",
                            "broker_status": broker_status
                        })
                    elif message["action"] == "get_nodes":
                        await websocket.send_json({"nodes": meshtastic_integration.get_nodes()})
                    elif message["action"] == "get_queue_stats":
                        queue_stats = mqtt_handler.get_queue_stats()
                        delivery_stats = mqtt_handler.get_delivery_stats()
                        await websocket.send_json({
                            "queue": queue_stats,
                            "delivery": delivery_stats
                        })
            except json.JSONDecodeError as e:
                logger.warning(f"Received invalid JSON from WebSocket client: {e}")
            except Exception as e:
                logger.error(f"Error processing WebSocket message: {e}")
    except WebSocketDisconnect:
        manager.disconnect(websocket)


if __name__ == "__main__":
    import uvicorn
    uvicorn.run("app:app", host=settings.API_HOST, port=settings.API_PORT, reload=True)
