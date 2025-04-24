"""
Handlers for different types of Meshtastic messages
"""
import logging
import json
from typing import Dict, Any, Optional

# Configure logger
logger = logging.getLogger(__name__)

class BaseHandler:
    """Base class for all message handlers"""
    
    async def handle(self, topic: str, payload: Any) -> bool:
        """Handle a message with the given topic and payload"""
        try:
            # Parse JSON payload if it's a string
            if isinstance(payload, str):
                try:
                    payload_data = json.loads(payload)
                except json.JSONDecodeError:
                    logger.error(f"Failed to parse JSON payload for topic {topic}")
                    return False
            else:
                payload_data = payload
                
            # Process the message
            return await self.process_message(topic, payload_data)
            
        except Exception as e:
            logger.error(f"Error handling message for topic {topic}: {e}")
            return False
    
    async def process_message(self, topic: str, payload: Dict[str, Any]) -> bool:
        """Process the parsed message - to be implemented by subclasses"""
        raise NotImplementedError("Subclasses must implement this method")


class NodeInfoHandler(BaseHandler):
    """Handler for node information messages"""
    
    async def process_message(self, topic: str, payload: Dict[str, Any]) -> bool:
        """Process a node info message"""
        logger.info(f"Received node info from topic {topic}")
        return True


class PositionHandler(BaseHandler):
    """Handler for position update messages"""
    
    async def process_message(self, topic: str, payload: Dict[str, Any]) -> bool:
        """Process a position update message"""
        logger.info(f"Received position update from topic {topic}")
        return True


class TextMessageHandler(BaseHandler):
    """Handler for text messages"""
    
    async def process_message(self, topic: str, payload: Dict[str, Any]) -> bool:
        """Process a text message"""
        logger.info(f"Received text message from topic {topic}")
        return True


class TelemetryHandler(BaseHandler):
    """Handler for telemetry messages"""
    
    async def process_message(self, topic: str, payload: Dict[str, Any]) -> bool:
        """Process a telemetry message"""
        logger.info(f"Received telemetry from topic {topic}")
        return True


class HeartbeatHandler(BaseHandler):
    """Handler for heartbeat messages"""
    
    async def process_message(self, topic: str, payload: Dict[str, Any]) -> bool:
        """Process a heartbeat message"""
        logger.info(f"Received heartbeat from topic {topic}")
        return True


class BinaryMessageHandler(BaseHandler):
    """Handler for binary messages"""
    
    async def process_message(self, topic: str, payload: Dict[str, Any]) -> bool:
        """Process a binary message"""
        logger.info(f"Received binary message from topic {topic}")
        return True