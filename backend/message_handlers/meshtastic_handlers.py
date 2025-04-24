"""
Handlers for different types of Meshtastic messages
"""
import json
import time
import logging
import base64
from abc import ABC, abstractmethod
from typing import Dict, Any, Optional, Tuple

from backend.models.meshtastic_node import MeshtasticNode, Position, Message

# Configure logger
logger = logging.getLogger(__name__)

class MeshtasticMessageHandler(ABC):
    """Base class for Meshtastic message handlers"""
    
    @abstractmethod
    async def handle(self, topic: str, payload: Any) -> bool:
        """Handle a message for the specific handler type"""
        pass
    
    def extract_node_id(self, topic: str) -> str:
        """Extract node ID from the topic"""
        # Topic format: msh/{node_id}/json/{message_type}
        parts = topic.split('/')
        if len(parts) >= 2:
            return parts[1]
        return ""
    
    def get_or_create_node(self, node_id: str) -> Tuple[MeshtasticNode, bool]:
        """Get or create a node by ID"""
        node = MeshtasticNode.get(node_id)
        created = False
        
        if node is None:
            # Create a new node
            node = MeshtasticNode(node_id=node_id)
            created = True
        
        # Update last seen time
        node.last_seen = time.time()
        node.message_count += 1
        
        return node, created


class NodeInfoHandler(MeshtasticMessageHandler):
    """Handler for node info messages"""
    
    async def handle(self, topic: str, payload: Any) -> bool:
        """Handle a node info message"""
        try:
            node_id = self.extract_node_id(topic)
            if not node_id:
                logger.warning(f"Could not extract node ID from topic: {topic}")
                return False
            
            # Parse the payload
            if isinstance(payload, bytes):
                payload = payload.decode('utf-8')
            
            try:
                data = json.loads(payload)
            except json.JSONDecodeError:
                logger.warning(f"Failed to parse JSON from node info message: {payload}")
                return False
            
            # Get or create the node
            node, created = self.get_or_create_node(node_id)
            
            # Update node data
            if "user" in data:
                node.name = data.get("user", {}).get("longName", "Unknown")
                node.short_name = data.get("user", {}).get("shortName", "")
                node.hardware = data.get("user", {}).get("hwModel", "")
            
            # Save node to database
            node.save()
            
            if created:
                logger.info(f"Created new node {node_id}: {node.name}")
            else:
                logger.info(f"Updated node info for {node_id}: {node.name}")
            
            return True
        
        except Exception as e:
            logger.error(f"Error handling node info message: {e}")
            return False


class PositionHandler(MeshtasticMessageHandler):
    """Handler for position messages"""
    
    async def handle(self, topic: str, payload: Any) -> bool:
        """Handle a position message"""
        try:
            node_id = self.extract_node_id(topic)
            if not node_id:
                logger.warning(f"Could not extract node ID from topic: {topic}")
                return False
            
            # Parse the payload
            if isinstance(payload, bytes):
                payload = payload.decode('utf-8')
            
            try:
                data = json.loads(payload)
            except json.JSONDecodeError:
                logger.warning(f"Failed to parse JSON from position message: {payload}")
                return False
            
            # Verify position data exists
            if "position" not in data:
                logger.warning(f"Position data missing in message: {payload}")
                return False
            
            # Get or create the node
            node, _ = self.get_or_create_node(node_id)
            
            # Update position
            position_data = data["position"]
            node.position = Position(
                latitude=position_data.get("latitude", 0),
                longitude=position_data.get("longitude", 0),
                altitude=position_data.get("altitude", 0),
                timestamp=position_data.get("time", int(time.time()))
            )
            
            # Save node to database
            node.save()
            
            logger.info(f"Updated position for node {node_id}")
            return True
        
        except Exception as e:
            logger.error(f"Error handling position message: {e}")
            return False


class TextMessageHandler(MeshtasticMessageHandler):
    """Handler for text messages"""
    
    async def handle(self, topic: str, payload: Any) -> bool:
        """Handle a text message"""
        try:
            node_id = self.extract_node_id(topic)
            if not node_id:
                logger.warning(f"Could not extract node ID from topic: {topic}")
                return False
            
            # Parse the payload
            if isinstance(payload, bytes):
                payload = payload.decode('utf-8')
            
            try:
                data = json.loads(payload)
            except json.JSONDecodeError:
                logger.warning(f"Failed to parse JSON from text message: {payload}")
                return False
            
            # Verify text data exists
            if "text" not in data:
                logger.warning(f"Text data missing in message: {payload}")
                return False
            
            # Get or create the node
            node, _ = self.get_or_create_node(node_id)
            
            # Create message object
            text_data = data["text"]
            message = Message(
                text=text_data.get("text", ""),
                from_id=text_data.get("from", ""),
                to_id=text_data.get("to", ""),
                timestamp=text_data.get("time", int(time.time()))
            )
            
            # Add to node's messages
            if not hasattr(node, "messages") or node.messages is None:
                node.messages = []
            
            node.messages.append(message)
            
            # Limit stored messages to 100
            if len(node.messages) > 100:
                node.messages = node.messages[-100:]
            
            # Save node to database
            node.save()
            
            logger.info(f"Received text message from node {node_id}")
            return True
        
        except Exception as e:
            logger.error(f"Error handling text message: {e}")
            return False


class TelemetryHandler(MeshtasticMessageHandler):
    """Handler for telemetry messages"""
    
    async def handle(self, topic: str, payload: Any) -> bool:
        """Handle a telemetry message"""
        try:
            node_id = self.extract_node_id(topic)
            if not node_id:
                logger.warning(f"Could not extract node ID from topic: {topic}")
                return False
            
            # Parse the payload
            if isinstance(payload, bytes):
                payload = payload.decode('utf-8')
            
            try:
                data = json.loads(payload)
            except json.JSONDecodeError:
                logger.warning(f"Failed to parse JSON from telemetry message: {payload}")
                return False
            
            # Verify telemetry data exists
            if "telemetry" not in data:
                logger.warning(f"Telemetry data missing in message: {payload}")
                return False
            
            # Get or create the node
            node, _ = self.get_or_create_node(node_id)
            
            # Update telemetry
            node.telemetry = data["telemetry"]
            
            # Save node to database
            node.save()
            
            logger.info(f"Updated telemetry for node {node_id}")
            return True
        
        except Exception as e:
            logger.error(f"Error handling telemetry message: {e}")
            return False


class HeartbeatHandler(MeshtasticMessageHandler):
    """Handler for heartbeat messages"""
    
    async def handle(self, topic: str, payload: Any) -> bool:
        """Handle a heartbeat message"""
        try:
            node_id = self.extract_node_id(topic)
            if not node_id:
                logger.warning(f"Could not extract node ID from topic: {topic}")
                return False
            
            # Parse the payload
            if isinstance(payload, bytes):
                payload = payload.decode('utf-8')
            
            try:
                data = json.loads(payload)
            except json.JSONDecodeError:
                logger.warning(f"Failed to parse JSON from heartbeat message: {payload}")
                return False
            
            # Get or create the node
            node, _ = self.get_or_create_node(node_id)
            
            # Update heartbeat data
            node.heartbeat = data
            node.last_heartbeat_time = time.time()
            
            # Save node to database
            node.save()
            
            logger.info(f"Received heartbeat from node {node_id}")
            return True
        
        except Exception as e:
            logger.error(f"Error handling heartbeat message: {e}")
            return False


class BinaryMessageHandler(MeshtasticMessageHandler):
    """Handler for binary data messages"""
    
    async def handle(self, topic: str, payload: Any) -> bool:
        """Handle a binary data message"""
        try:
            node_id = self.extract_node_id(topic)
            if not node_id:
                logger.warning(f"Could not extract node ID from topic: {topic}")
                return False
            
            # Ensure payload is bytes
            if not isinstance(payload, bytes):
                if isinstance(payload, str):
                    try:
                        # Try to convert from base64 string
                        payload = base64.b64decode(payload)
                    except Exception:
                        logger.warning(f"Failed to decode binary data: {payload}")
                        return False
                else:
                    logger.warning(f"Unexpected payload type for binary data: {type(payload)}")
                    return False
            
            # Get or create the node
            node, _ = self.get_or_create_node(node_id)
            
            # Process binary data
            # For now, we just log it - in a real implementation,
            # you might store it or process it further
            logger.info(f"Received binary data from node {node_id}: {len(payload)} bytes")
            
            # If attributes doesn't exist, create it
            if not hasattr(node, "attributes") or node.attributes is None:
                node.attributes = {}
            
            # Store metadata about binary message
            if "binary_messages" not in node.attributes:
                node.attributes["binary_messages"] = []
            
            # Add binary message metadata (not the data itself)
            node.attributes["binary_messages"].append({
                "timestamp": time.time(),
                "size_bytes": len(payload),
                "topic": topic
            })
            
            # Limit to last 10 binary messages
            if len(node.attributes["binary_messages"]) > 10:
                node.attributes["binary_messages"] = node.attributes["binary_messages"][-10:]
            
            # Save node to database
            node.save()
            
            return True
        
        except Exception as e:
            logger.error(f"Error handling binary message: {e}")
            return False 