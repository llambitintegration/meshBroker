from fastapi import FastAPI, HTTPException, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
import json
import asyncio
from typing import Dict, List, Optional
import logging
from pydantic import BaseModel

from mqtt_handler import MQTTHandler
import meshtastic_integration
from config import settings

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

app = FastAPI(title="Meshtastic MQTT Bridge")

# Configure CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Wide open for local development
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Initialize MQTT Handler
mqtt_handler = MQTTHandler(
    broker_host=settings.MQTT_BROKER_HOST,
    broker_port=settings.MQTT_BROKER_PORT,
    client_id=settings.MQTT_CLIENT_ID,
    username=settings.MQTT_USERNAME,
    password=settings.MQTT_PASSWORD,
)

# WebSocket connection manager
class ConnectionManager:
    def __init__(self):
        self.active_connections: List[WebSocket] = []

    async def connect(self, websocket: WebSocket):
        await websocket.accept()
        self.active_connections.append(websocket)

    def disconnect(self, websocket: WebSocket):
        self.active_connections.remove(websocket)

    async def broadcast(self, message: str):
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
    qos: Optional[int] = 0
    retain: Optional[bool] = False


class TopicSubscription(BaseModel):
    topic: str


# Routes
@app.on_event("startup")
async def startup_event():
    """Connect to MQTT broker on startup"""
    mqtt_handler.connect()
    # Set callback for received messages
    mqtt_handler.set_message_callback(on_mqtt_message)
    # Start the Meshtastic integration
    await meshtastic_integration.initialize(mqtt_handler)


@app.on_event("shutdown")
async def shutdown_event():
    """Disconnect from MQTT broker on shutdown"""
    mqtt_handler.disconnect()


async def on_mqtt_message(topic, payload):
    """Callback for MQTT messages to broadcast to WebSocket clients"""
    message = {
        "topic": topic,
        "payload": payload.decode("utf-8") if isinstance(payload, bytes) else payload,
        "timestamp": asyncio.get_event_loop().time()
    }
    await manager.broadcast(json.dumps(message))


@app.post("/publish", status_code=200)
async def publish_message(message: MQTTMessage):
    """Publish a message to an MQTT topic"""
    try:
        result = mqtt_handler.publish(
            message.topic, message.payload, message.qos, message.retain
        )
        if result.rc != 0:
            raise HTTPException(status_code=500, detail=f"Failed to publish: {result.rc}")
        return {"status": "success", "message": "Message published successfully"}
    except Exception as e:
        logger.error(f"Error publishing message: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/subscribe", status_code=200)
async def subscribe_topic(subscription: TopicSubscription):
    """Subscribe to an MQTT topic"""
    try:
        result = mqtt_handler.subscribe(subscription.topic)
        if result.rc != 0:
            raise HTTPException(status_code=500, detail=f"Failed to subscribe: {result.rc}")
        return {"status": "success", "message": f"Subscribed to {subscription.topic}"}
    except Exception as e:
        logger.error(f"Error subscribing to topic: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/unsubscribe", status_code=200)
async def unsubscribe_topic(subscription: TopicSubscription):
    """Unsubscribe from an MQTT topic"""
    try:
        result = mqtt_handler.unsubscribe(subscription.topic)
        if result.rc != 0:
            raise HTTPException(status_code=500, detail=f"Failed to unsubscribe: {result.rc}")
        return {"status": "success", "message": f"Unsubscribed from {subscription.topic}"}
    except Exception as e:
        logger.error(f"Error unsubscribing from topic: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/topics")
async def get_topics():
    """Get list of currently subscribed topics"""
    return {"topics": mqtt_handler.get_subscribed_topics()}


@app.get("/broker_status")
async def get_broker_status():
    """Check MQTT broker connection status"""
    return {"status": "connected" if mqtt_handler.is_connected() else "disconnected"}


@app.get("/meshtastic/nodes")
async def get_meshtastic_nodes():
    """Get list of known Meshtastic nodes"""
    return {"nodes": meshtastic_integration.get_nodes()}


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
                        await websocket.send_json({"status": "connected" if mqtt_handler.is_connected() else "disconnected"})
            except json.JSONDecodeError:
                pass
    except WebSocketDisconnect:
        manager.disconnect(websocket)


if __name__ == "__main__":
    import uvicorn
    uvicorn.run("app:app", host="0.0.0.0", port=8000, reload=True)
