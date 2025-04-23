import logging
import json
import asyncio
import time
from typing import Dict, List, Optional, Any

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

# Store known Meshtastic nodes
meshtastic_nodes = {}

# MQTT topics for Meshtastic
MESHTASTIC_TOPIC_PREFIX = "msh"
MESHTASTIC_TOPIC_ALL = f"{MESHTASTIC_TOPIC_PREFIX}/#"
NODE_INFO_TOPIC = f"{MESHTASTIC_TOPIC_PREFIX}/+/json/nodeid"
POSITION_TOPIC = f"{MESHTASTIC_TOPIC_PREFIX}/+/json/position"
TEXT_MESSAGE_TOPIC = f"{MESHTASTIC_TOPIC_PREFIX}/+/json/text"
TELEMETRY_TOPIC = f"{MESHTASTIC_TOPIC_PREFIX}/+/json/telemetry"
HEARTBEAT_TOPIC = f"{MESHTASTIC_TOPIC_PREFIX}/+/json/heartbeat"

# List of topics to subscribe to
MESHTASTIC_TOPICS = [
    NODE_INFO_TOPIC,
    POSITION_TOPIC,
    TEXT_MESSAGE_TOPIC,
    TELEMETRY_TOPIC,
    HEARTBEAT_TOPIC,
]

async def initialize(mqtt_handler):
    """Initialize Meshtastic integration"""
    logger.info("Initializing Meshtastic integration")
    
    # Subscribe to Meshtastic topics
    for topic in MESHTASTIC_TOPICS:
        mqtt_handler.subscribe(topic)
    
    # Set up message handling
    mqtt_handler.set_message_callback(process_meshtastic_message)
    
    logger.info("Meshtastic integration initialized")
    return True

async def process_meshtastic_message(topic: str, payload: Any):
    """Process incoming Meshtastic MQTT messages"""
    try:
        if not topic.startswith(MESHTASTIC_TOPIC_PREFIX):
            return  # Not a Meshtastic message
        
        if isinstance(payload, bytes):
            payload = payload.decode('utf-8')
        
        # Parse the payload
        try:
            data = json.loads(payload)
        except json.JSONDecodeError:
            logger.warning(f"Failed to parse JSON from topic {topic}: {payload}")
            return
        
        # Get the current time safely
        try:
            current_time = asyncio.get_running_loop().time()
        except RuntimeError:
            # Fallback if no running loop
            current_time = time.time()
        
        # Extract node ID from topic
        # Topic format: msh/{node_id}/json/{message_type}
        parts = topic.split('/')
        if len(parts) >= 4:
            node_id = parts[1]
            message_type = parts[3]
            
            # Update or create node info
            if node_id not in meshtastic_nodes:
                meshtastic_nodes[node_id] = {
                    "node_id": node_id,
                    "last_seen": current_time,
                    "message_count": 0,
                }
            
            # Update node data based on message type
            node = meshtastic_nodes[node_id]
            node["last_seen"] = current_time
            node["message_count"] += 1
            
            if message_type == "nodeid" and "user" in data:
                node["name"] = data.get("user", {}).get("longName", "Unknown")
                node["short_name"] = data.get("user", {}).get("shortName", "")
                node["hardware"] = data.get("user", {}).get("hwModel", "")
                logger.info(f"Updated node info for {node_id}: {node['name']}")
            
            elif message_type == "position" and "position" in data:
                node["position"] = {
                    "latitude": data["position"].get("latitude", 0),
                    "longitude": data["position"].get("longitude", 0),
                    "altitude": data["position"].get("altitude", 0),
                    "timestamp": data["position"].get("time", 0),
                }
                logger.info(f"Updated position for node {node_id}")
            
            elif message_type == "text" and "text" in data:
                if "messages" not in node:
                    node["messages"] = []
                node["messages"].append({
                    "text": data["text"].get("text", ""),
                    "from": data["text"].get("from", ""),
                    "to": data["text"].get("to", ""),
                    "timestamp": data["text"].get("time", 0),
                })
                # Limit stored messages
                if len(node["messages"]) > 100:
                    node["messages"] = node["messages"][-100:]
                logger.info(f"Received text message from node {node_id}")
            
            elif message_type == "telemetry" and "telemetry" in data:
                node["telemetry"] = data["telemetry"]
                logger.info(f"Updated telemetry for node {node_id}")
            
            elif message_type == "heartbeat":
                node["heartbeat"] = data
                logger.info(f"Received heartbeat from node {node_id}")
    
    except Exception as e:
        logger.error(f"Error processing Meshtastic message: {e}")

def get_nodes():
    """Get list of known Meshtastic nodes"""
    return meshtastic_nodes

def send_message_to_node(mqtt_handler, target_node_id, message, source_node_id=None):
    """Send a text message to a Meshtastic node"""
    topic = f"{MESHTASTIC_TOPIC_PREFIX}/{target_node_id}/json/text"
    payload = {
        "text": {
            "text": message,
            "from": source_node_id or "mqtt-bridge",
            "to": target_node_id,
            "time": int(time.time())
        }
    }
    return mqtt_handler.publish(topic, json.dumps(payload))

def broadcast_message(mqtt_handler, message, source_node_id=None):
    """Broadcast a text message to all Meshtastic nodes"""
    topic = f"{MESHTASTIC_TOPIC_PREFIX}/broadcast/json/text"
    payload = {
        "text": {
            "text": message,
            "from": source_node_id or "mqtt-bridge",
            "to": "^all",
            "time": int(time.time())
        }
    }
    return mqtt_handler.publish(topic, json.dumps(payload))
