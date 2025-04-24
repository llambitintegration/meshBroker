"""
Router for dispatching messages to appropriate handlers
"""
import re
import logging
from typing import Dict, Any, List, Pattern, Callable, Awaitable

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
    
    def add_route(self, pattern: str, handler: Callable[[str, Any], Awaitable[bool]]):
        """Add a custom route with its handler"""
        self.custom_routes[re.compile(pattern)] = handler
    
    async def route_message(self, topic: str, payload: Any) -> bool:
        """Route a message to the appropriate handler based on the topic"""
        if not topic.startswith("msh/"):
            # Not a Meshtastic message
            return False
        
        # Try custom routes first
        for pattern, handler in self.custom_routes.items():
            if pattern.match(topic):
                logger.debug(f"Routing message for topic {topic} to custom handler")
                return await handler(topic, payload)
        
        # Then try predefined routes
        for pattern, handler in self.routes.items():
            if pattern.match(topic):
                logger.debug(f"Routing message for topic {topic} to handler")
                return await handler(topic, payload)
        
        logger.warning(f"No handler found for topic: {topic}")
        return False
    
    @property
    def meshtastic_topics(self) -> List[str]:
        """Get the list of Meshtastic topics to subscribe to"""
        return [
            "msh/+/json/nodeid",
            "msh/+/json/position",
            "msh/+/json/text",
            "msh/+/json/telemetry",
            "msh/+/json/heartbeat",
            "msh/+/binary"
        ]