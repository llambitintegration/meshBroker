import logging
import json
import asyncio
import time
import os
from typing import Dict, List, Optional, Any
import re

from backend.models.meshtastic_node import MeshtasticNode, Message, Position
from backend.messaging.message_router import MessageRouter
from backend.message_handlers.meshtastic_handlers import (
    NodeInfoHandler, PositionHandler, TextMessageHandler,
    TelemetryHandler, HeartbeatHandler, BinaryMessageHandler
)
import backend.direct_meshtastic as direct_meshtastic

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
MESHTASTIC_TOPIC_PREFIX = "msh/Bob"

# Connection mode
# Can be "mqtt", "direct", or "hybrid"
CONNECTION_MODE = os.environ.get("MESHTASTIC_CONNECTION_MODE", "hybrid")

async def initialize(mqtt_handler):
    """Initialize Meshtastic integration"""
    global message_router
    
    logger.info(f"Initializing Meshtastic integration in {CONNECTION_MODE} mode")
    
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
    
    # Register handlers for different message types
    register_message_handlers(message_router)
    
    # Register a default handler
    message_router.set_default_handler(default_message_handler)
    
    if CONNECTION_MODE in ["mqtt", "hybrid"]:
        # Set up MQTT message handling
        logger.info("Setting up MQTT message handling")
        mqtt_handler.set_message_callback(process_meshtastic_message)
        
        # Subscribe to Meshtastic topics
        for topic in message_router.meshtastic_topics:
            mqtt_handler.subscribe(topic)
    
    if CONNECTION_MODE in ["direct", "hybrid"]:
        # Initialize direct device communication
        logger.info("Initializing direct device communication")
        direct_initialized = await direct_meshtastic.initialize(mqtt_handler if CONNECTION_MODE == "hybrid" else None)
        
        if not direct_initialized:
            logger.error("Failed to initialize direct Meshtastic communication")
            if CONNECTION_MODE == "direct":
                return False
    
    # Set up periodic node expiration check
    asyncio.create_task(periodic_node_expiration())
    
    # Set up periodic node synchronization for direct connections
    if CONNECTION_MODE in ["direct", "hybrid"]:
        asyncio.create_task(periodic_node_sync())
    
    logger.info("Meshtastic integration initialized")
    return True

def register_message_handlers(router):
    """Register handlers for different message types with the router"""
    # Create handler instances
    node_info_handler = NodeInfoHandler()
    position_handler = PositionHandler()
    text_message_handler = TextMessageHandler()
    telemetry_handler = TelemetryHandler()
    heartbeat_handler = HeartbeatHandler()
    binary_handler = BinaryMessageHandler()
    
    # Register handlers with the router
    router.routes[re.compile(r"msh/Bob/\+/json/nodeid")] = node_info_handler.handle
    router.routes[re.compile(r"msh/Bob/\+/json/position")] = position_handler.handle
    router.routes[re.compile(r"msh/Bob/\+/json/text")] = text_message_handler.handle
    router.routes[re.compile(r"msh/Bob/\+/json/telemetry")] = telemetry_handler.handle
    router.routes[re.compile(r"msh/Bob/\+/json/heartbeat")] = heartbeat_handler.handle
    router.routes[re.compile(r"msh/Bob/\+/binary")] = binary_handler.handle
    
    logger.info("Registered message handlers with the router")

async def default_message_handler(topic: str, payload: Any) -> bool:
    """Default handler for messages that don't match any specific pattern"""
    try:
        logger.debug(f"Processing unhandled message with topic: {topic}")
        
        # Just log the message for now - could add additional processing later
        if isinstance(payload, bytes):
            payload_str = f"<binary data of length {len(payload)}>"
        elif isinstance(payload, (dict, list)):
            payload_str = json.dumps(payload)
        else:
            payload_str = str(payload)
            
        logger.debug(f"Unhandled message content: {payload_str[:200]}...")
        
        # Return True to indicate we "handled" it (even if just by logging)
        return True
    except Exception as e:
        logger.error(f"Error in default message handler: {e}")
        return False

async def process_meshtastic_message(topic: str, payload: Any):
    """Process incoming Meshtastic MQTT messages"""
    global message_router
    
    try:
        if not message_router:
            logger.error("Message router not initialized")
            # Try to recover by re-initializing
            message_router = MessageRouter()
            register_message_handlers(message_router)
            logger.info("Re-initialized message router after finding it was None")
        
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

async def periodic_node_sync():
    """Periodically synchronize nodes from direct connections"""
    if CONNECTION_MODE not in ["direct", "hybrid"]:
        return
        
    while True:
        try:
            # Wait for 5 minutes
            await asyncio.sleep(300)
            
            # Sync nodes from all connected devices
            devices = direct_meshtastic.device_manager.get_all_devices()
            for device in devices:
                if device["connected"]:
                    await direct_meshtastic.sync_nodes_to_database(device["id"])
        
        except asyncio.CancelledError:
            break
        except Exception as e:
            logger.error(f"Error in node sync task: {e}")
            # Wait a bit before retrying
            await asyncio.sleep(60)

def get_nodes(active_only=True, group=None, category=None):
    """Get list of known Meshtastic nodes with optional filtering"""
    nodes = MeshtasticNode.get_all(active_only=active_only, group=group, category=category)
    # Convert node objects to dictionaries for API responses
    return [node.to_dict() for node in nodes]

def get_node(node_id):
    """Get a specific node by ID"""
    node = MeshtasticNode.get(node_id)
    # Convert node object to dictionary for API response
    return node.to_dict() if node else None

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
    # Try direct connection first if in direct or hybrid mode
    if CONNECTION_MODE in ["direct", "hybrid"]:
        device = direct_meshtastic.device_manager.get_device()
        if device and device.connected:
            success = device.send_text(message_text, destination_id=target_node_id)
            if success:
                logger.info(f"Sent message to node {target_node_id} via direct connection")
                return True
    
    # Fall back to MQTT if direct connection failed or not available
    if CONNECTION_MODE in ["mqtt", "hybrid"]:
        topic = f"{MESHTASTIC_TOPIC_PREFIX}/{target_node_id}/json/text"
        payload = {
            "text": {
                "text": message_text,
                "from": source_node_id or "mqtt-bridge",
                "to": target_node_id,
                "time": int(time.time())
            }
        }
        success = mqtt_handler.publish(topic, json.dumps(payload))
        if success:
            logger.info(f"Sent message to node {target_node_id} via MQTT")
            return True
    
    logger.error(f"Failed to send message to node {target_node_id}")
    return False

def broadcast_message(mqtt_handler, message_text, source_node_id=None):
    """Broadcast a text message to all Meshtastic nodes"""
    # Try direct connection first if in direct or hybrid mode
    if CONNECTION_MODE in ["direct", "hybrid"]:
        device = direct_meshtastic.device_manager.get_device()
        if device and device.connected:
            success = device.send_text(message_text)  # No destination_id means broadcast
            if success:
                logger.info("Broadcast message via direct connection")
                return True
    
    # Fall back to MQTT if direct connection failed or not available
    if CONNECTION_MODE in ["mqtt", "hybrid"]:
        topic = f"{MESHTASTIC_TOPIC_PREFIX}/broadcast/json/text"
        payload = {
            "text": {
                "text": message_text,
                "from": source_node_id or "mqtt-bridge",
                "to": "^all",
                "time": int(time.time())
            }
        }
        success = mqtt_handler.publish(topic, json.dumps(payload))
        if success:
            logger.info("Broadcast message via MQTT")
            return True
    
    logger.error("Failed to broadcast message")
    return False

def send_binary_data(mqtt_handler, target_node_id, binary_data, source_node_id=None):
    """Send binary data to a Meshtastic node"""
    # For now, binary data is only supported via MQTT
    # Direct binary data transmission would need additional implementation
    
    if CONNECTION_MODE in ["mqtt", "hybrid"]:
        topic = f"{MESHTASTIC_TOPIC_PREFIX}/{target_node_id}/binary"
        
        # Ensure binary_data is bytes
        if isinstance(binary_data, str):
            binary_data = binary_data.encode('utf-8')
        
        return mqtt_handler.publish(topic, binary_data)
    
    logger.error(f"Failed to send binary data to node {target_node_id}")
    return False

def get_node_count():
    """Get count of nodes by activity status"""
    all_nodes = MeshtasticNode.get_all()
    active_nodes = [node for node in all_nodes if node.is_active]
    
    return {
        "total": len(all_nodes),
        "active": len(active_nodes),
        "inactive": len(all_nodes) - len(active_nodes)
    }

# Direct device management functions

async def discover_devices(connection_type="all"):
    """
    Discover available Meshtastic devices
    
    Args:
        connection_type: Type of connection to discover ('serial', 'ble', or 'all')
        
    Returns:
        Dict containing available devices by connection type
    """
    if CONNECTION_MODE not in ["direct", "hybrid"]:
        logger.warning("Device discovery only available in direct or hybrid mode")
        return {"success": False, "error": "Device discovery requires direct mode or hybrid mode"}
    
    result = {"success": True}
    error_messages = []
    
    try:
        if connection_type in ["serial", "all"]:
            try:
                serial_ports = await direct_meshtastic.device_manager.discover_serial_devices()
                result["serial"] = [{"port": port} for port in serial_ports]
                
                # If no devices found but no error occurred, provide a helpful message
                if not serial_ports:
                    logger.info("No serial devices were found during discovery")
                    if "serial_message" not in result:
                        result["serial_message"] = "No Meshtastic serial devices found. Please check connections and permissions."
                
            except PermissionError as e:
                error_msg = str(e)
                logger.error(f"Permission error during serial discovery: {error_msg}")
                error_messages.append(f"Serial port permission error: {error_msg}")
                result["serial"] = []
                result["serial_error"] = error_msg
                
            except FileNotFoundError as e:
                error_msg = str(e)
                logger.error(f"Device not found during serial discovery: {error_msg}")
                error_messages.append(f"Serial device not found: {error_msg}")
                result["serial"] = []
                result["serial_error"] = error_msg
                
            except Exception as e:
                error_msg = str(e)
                logger.error(f"Error discovering serial devices: {error_msg}")
                error_messages.append(f"Serial discovery error: {error_msg}")
                result["serial"] = []
                result["serial_error"] = error_msg
        
        if connection_type in ["ble", "all"]:
            try:
                ble_devices = await direct_meshtastic.device_manager.discover_ble_devices()
                result["ble"] = ble_devices
                
                # If no devices found but no error occurred, provide a helpful message
                if not ble_devices:
                    logger.info("No BLE devices were found during discovery")
                    if "ble_message" not in result:
                        result["ble_message"] = "No Meshtastic BLE devices found. Please check that Bluetooth is enabled."
                
            except Exception as e:
                error_msg = str(e)
                logger.error(f"Error discovering BLE devices: {error_msg}")
                error_messages.append(f"BLE discovery error: {error_msg}")
                result["ble"] = []
                result["ble_error"] = error_msg
        
        # If both types had errors, set overall error message
        if error_messages and len(error_messages) == (
            (1 if connection_type in ["serial", "all"] else 0) +
            (1 if connection_type in ["ble", "all"] else 0)
        ):
            result["success"] = False
            result["error"] = " | ".join(error_messages)
        
        # If no devices were found at all, include a helpful overall message
        if (connection_type == "serial" and "serial" in result and not result["serial"]) or \
           (connection_type == "ble" and "ble" in result and not result["ble"]) or \
           (connection_type == "all" and 
            "serial" in result and not result["serial"] and 
            "ble" in result and not result["ble"]):
            if "success" in result and result["success"]:
                # Only add this message if we didn't already mark it as unsuccessful
                result["message"] = "No devices found. Please check your connections and try again."
        
        return result
        
    except Exception as e:
        logger.error(f"Unexpected error in discover_devices: {e}")
        return {"success": False, "error": f"Device discovery failed: {str(e)}"}

async def connect_device(connection_type, connection_params, device_id=None):
    """
    Connect to a Meshtastic device
    
    Args:
        connection_type: Type of connection ('serial', 'tcp', 'ble')
        connection_params: Parameters for the connection
        device_id: Optional ID for the device
        
    Returns:
        Device ID if successful, None otherwise
    """
    if CONNECTION_MODE not in ["direct", "hybrid"]:
        logger.warning("Device connection only available in direct or hybrid mode")
        return None
    
    device_id = await direct_meshtastic.device_manager.connect_device(
        connection_type, connection_params, device_id
    )
    
    if device_id:
        # Sync nodes from the newly connected device
        await direct_meshtastic.sync_nodes_to_database(device_id)
    
    return device_id

async def disconnect_device(device_id):
    """
    Disconnect from a Meshtastic device
    
    Args:
        device_id: Device ID to disconnect
        
    Returns:
        True if successful, False otherwise
    """
    if CONNECTION_MODE not in ["direct", "hybrid"]:
        logger.warning("Device disconnection only available in direct or hybrid mode")
        return False
    
    return await direct_meshtastic.device_manager.disconnect_device(device_id)

def get_connected_devices():
    """
    Get information about all connected devices
    
    Returns:
        List of connected devices with their information
    """
    if CONNECTION_MODE not in ["direct", "hybrid"]:
        logger.warning("Device information only available in direct or hybrid mode")
        return []
    
    return direct_meshtastic.device_manager.get_all_devices()

def get_device_config(device_id=None):
    """
    Get device configuration
    
    Args:
        device_id: Device ID (uses default if None)
        
    Returns:
        Dict with device configuration
    """
    if CONNECTION_MODE not in ["direct", "hybrid"]:
        logger.warning("Device configuration only available in direct or hybrid mode")
        return {}
    
    device = direct_meshtastic.device_manager.get_device(device_id)
    if not device:
        logger.warning(f"Device {device_id} not found")
        return {}
    
    return device.get_config()

def set_device_config(key, value, device_id=None):
    """
    Set device configuration
    
    Args:
        key: Configuration key (e.g., 'device.role')
        value: Value to set
        device_id: Device ID (uses default if None)
        
    Returns:
        True if successful, False otherwise
    """
    if CONNECTION_MODE not in ["direct", "hybrid"]:
        logger.warning("Device configuration only available in direct or hybrid mode")
        return False
    
    device = direct_meshtastic.device_manager.get_device(device_id)
    if not device:
        logger.warning(f"Device {device_id} not found")
        return False
    
    return device.set_config(key, value)

def get_device_channels(device_id=None):
    """
    Get device channel settings
    
    Args:
        device_id: Device ID (uses default if None)
        
    Returns:
        List of channel settings
    """
    if CONNECTION_MODE not in ["direct", "hybrid"]:
        logger.warning("Device channel settings only available in direct or hybrid mode")
        return []
    
    device = direct_meshtastic.device_manager.get_device(device_id)
    if not device:
        logger.warning(f"Device {device_id} not found")
        return []
    
    return device.get_channels()

def set_device_channel(settings, channel_index=0, device_id=None):
    """
    Set device channel settings
    
    Args:
        settings: Channel settings to update
        channel_index: Channel index (default: 0 for primary channel)
        device_id: Device ID (uses default if None)
        
    Returns:
        True if successful, False otherwise
    """
    if CONNECTION_MODE not in ["direct", "hybrid"]:
        logger.warning("Device channel settings only available in direct or hybrid mode")
        return False
    
    device = direct_meshtastic.device_manager.get_device(device_id)
    if not device:
        logger.warning(f"Device {device_id} not found")
        return False
    
    return device.set_channel_settings(settings, channel_index)
