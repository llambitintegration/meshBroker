"""
Router for dispatching messages to appropriate handlers
"""
import re
import logging
from typing import Dict, Any, List, Pattern, Callable, Awaitable, Optional

# Configure logger
logger = logging.getLogger(__name__)

class MessageRouter:
    """Routes messages to the appropriate handlers based on topic patterns"""
    
    def __init__(self):
        """Initialize the router with handlers for different message types"""
        # Define topic patterns and their handlers
        self.routes: Dict[Pattern, Callable[[str, Any], Awaitable[bool]]] = {}
        
        # Custom routes for additional handlers
        self.custom_routes: Dict[Pattern, Callable[[str, Any], Awaitable[bool]]] = {}
        
        # Default handler for unmatched messages
        self.default_handler: Optional[Callable[[str, Any], Awaitable[bool]]] = None
    
    def add_route(self, pattern: str, handler: Callable[[str, Any], Awaitable[bool]]):
        """Add a custom route with its handler"""
        self.custom_routes[re.compile(pattern)] = handler
    
    def set_default_handler(self, handler: Callable[[str, Any], Awaitable[bool]]):
        """Set a default handler for unmatched messages"""
        self.default_handler = handler
    
    async def route_message(self, topic: str, payload: Any) -> bool:
        """Route a message to the appropriate handler based on the topic"""
        # Track if we've attempted routing
        attempted_routing = False
        
        if topic.startswith("msh/Bob/"):
            # Meshtastic message with our prefix - try the specific routes
            
            # Try custom routes first
            for pattern, handler in self.custom_routes.items():
                if pattern.match(topic):
                    logger.debug(f"Routing message for topic {topic} to custom handler")
                    attempted_routing = True
                    try:
                        return await handler(topic, payload)
                    except Exception as e:
                        logger.error(f"Error in custom handler for topic {topic}: {e}")
                        return False
            
            # Then try predefined routes
            for pattern, handler in self.routes.items():
                if pattern.match(topic):
                    logger.debug(f"Routing message for topic {topic} to handler")
                    attempted_routing = True
                    try:
                        return await handler(topic, payload)
                    except Exception as e:
                        logger.error(f"Error in handler for topic {topic}: {e}")
                        return False
            
            # If we got here with a Meshtastic prefix but no matching pattern, log a more helpful message
            parts = topic.split('/')
            if len(parts) >= 4:
                message_type = '/'.join(parts[3:])
                logger.warning(f"No handler found for Meshtastic message type: {message_type} (full topic: {topic})")
            else:
                logger.warning(f"No handler found for topic: {topic} - malformed Meshtastic topic")
        
        # If we haven't routed the message and we have a default handler, use it
        if not attempted_routing and self.default_handler:
            logger.debug(f"Using default handler for topic: {topic}")
            try:
                return await self.default_handler(topic, payload)
            except Exception as e:
                logger.error(f"Error in default handler for topic {topic}: {e}")
                return False
        
        if not attempted_routing:
            logger.warning(f"No handler found for topic: {topic}")
        
        return False
    
    @property
    def meshtastic_topics(self) -> List[str]:
        """Get the list of Meshtastic topics to subscribe to"""
        return [
            "msh/Bob/+/json/nodeid",
            "msh/Bob/+/json/position",
            "msh/Bob/+/json/text",
            "msh/Bob/+/json/telemetry",
            "msh/Bob/+/json/heartbeat",
            "msh/Bob/+/binary"
        ]