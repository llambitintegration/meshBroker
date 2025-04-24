import logging
import json
import asyncio
import time
import os
from typing import Dict, List, Optional, Any

from backend.models.meshtastic_node import MeshtasticNode, Message, Position
from backend.message_router import MessageRouter

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

# Message router instance
message_router = None

# Database configuration
DB_PATH = os.environ.get("MESHTASTIC_DB_PATH", "mqtt_data/meshtastic_nodes.db")

# Expiration configuration
NODE_EXPIRATION_SECONDS = int(os.environ.get("NODE_EXPIRATION_SECONDS", "3600"))  # 1 hour default

# MQTT topics for Meshtastic
MESHTASTIC_TOPIC_PREFIX = "msh"

async def initialize(mqtt_handler):
    """Initialize Meshtastic integration"""
    global message_router
    
    logger.info("Initializing Meshtastic integration")
    
    # Initialize the database
    try:
        # Ensure the data directory exists
        db_dir = os.path.dirname(DB_PATH)
        if not os.path.exists(db_dir):
            logger.info(f"Creating database directory: {db_dir}")
            os.makedirs(db_dir, exist_ok=True)
            
        # Set the database path and initialize it
        MeshtasticNode.set_db_path(DB_PATH)
        db_initialized = MeshtasticNode.init_db()
        
        if not db_initialized:
            logger.error("Failed to initialize Meshtastic nodes database")
            return False
            
    except Exception as e:
        logger.error(f"Error during database initialization: {e}")
        return False
    
    # Create the message router
    message_router = MessageRouter()
    
    # Subscribe to Meshtastic topics
    for topic in message_router.meshtastic_topics:
        mqtt_handler.subscribe(topic)
    
    # Set up message handling
    mqtt_handler.set_message_callback(process_meshtastic_message)
    
    # Set up periodic node expiration check
    asyncio.create_task(periodic_node_expiration())
    
    logger.info("Meshtastic integration initialized")
    return True

async def process_meshtastic_message(topic: str, payload: Any):
    """Process incoming Meshtastic MQTT messages"""
    global message_router
    
    try:
        if not message_router:
            logger.error("Message router not initialized")
            return
        
        # Route the message to the appropriate handler
        await message_router.route_message(topic, payload)
    
    except Exception as e:
        logger.error(f"Error processing Meshtastic message: {e}")

async def periodic_node_expiration():
    """Periodically check for and expire inactive nodes"""
    while True:
        try:
            # Wait for one hour
            await asyncio.sleep(3600)
            
            # Expire inactive nodes
            expired_count = MeshtasticNode.expire_inactive_nodes(NODE_EXPIRATION_SECONDS)
            logger.info(f"Expired {expired_count} inactive nodes")
        
        except asyncio.CancelledError:
            break
        except Exception as e:
            logger.error(f"Error in node expiration task: {e}")
            # Wait a bit before retrying
            await asyncio.sleep(60)

def get_nodes(active_only=True, group=None, category=None):
    """Get list of known Meshtastic nodes with optional filtering"""
    return MeshtasticNode.get_all(active_only=active_only, group=group, category=category)

def get_node(node_id):
    """Get a specific node by ID"""
    return MeshtasticNode.get(node_id)

def update_node_group(node_id, group):
    """Update a node's group"""
    node = MeshtasticNode.get(node_id)
    if node:
        node.group = group
        node.save()
        logger.info(f"Updated node {node_id} group to {group}")
        return True
    return False

def update_node_category(node_id, category):
    """Update a node's category"""
    node = MeshtasticNode.get(node_id)
    if node:
        node.category = category
        node.save()
        logger.info(f"Updated node {node_id} category to {category}")
        return True
    return False

def send_message_to_node(mqtt_handler, target_node_id, message_text, source_node_id=None):
    """Send a text message to a Meshtastic node"""
    topic = f"{MESHTASTIC_TOPIC_PREFIX}/{target_node_id}/json/text"
    payload = {
        "text": {
            "text": message_text,
            "from": source_node_id or "mqtt-bridge",
            "to": target_node_id,
            "time": int(time.time())
        }
    }
    return mqtt_handler.publish(topic, json.dumps(payload))

def broadcast_message(mqtt_handler, message_text, source_node_id=None):
    """Broadcast a text message to all Meshtastic nodes"""
    topic = f"{MESHTASTIC_TOPIC_PREFIX}/broadcast/json/text"
    payload = {
        "text": {
            "text": message_text,
            "from": source_node_id or "mqtt-bridge",
            "to": "^all",
            "time": int(time.time())
        }
    }
    return mqtt_handler.publish(topic, json.dumps(payload))

def send_binary_data(mqtt_handler, target_node_id, binary_data, source_node_id=None):
    """Send binary data to a Meshtastic node"""
    topic = f"{MESHTASTIC_TOPIC_PREFIX}/{target_node_id}/binary"
    
    # Ensure binary_data is bytes
    if isinstance(binary_data, str):
        binary_data = binary_data.encode('utf-8')
    
    return mqtt_handler.publish(topic, binary_data)

def get_node_count():
    """Get count of nodes by activity status"""
    all_nodes = MeshtasticNode.get_all()
    active_nodes = [node for node in all_nodes if node.is_active]
    
    return {
        "total": len(all_nodes),
        "active": len(active_nodes),
        "inactive": len(all_nodes) - len(active_nodes)
    }
