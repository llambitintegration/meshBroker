"""
Channel manager for topic-based WebSocket messaging
"""
import time
import re
import logging
from typing import Dict, Set, List, Optional, Pattern, Any
from pydantic import BaseModel

from .connection_models import WSSubscriptionInfo, WSConnectionInfo, WSMessage, WSMessageType

# Configure logger
logger = logging.getLogger(__name__)


class ChannelManager:
    """Manager for topic-based WebSocket channels"""
    
    def __init__(self):
        # Channel subscriptions: topic -> {connection_id, connection_id, ...}
        self.subscriptions: Dict[str, Set[str]] = {}
        
        # Connection subscriptions: connection_id -> {topic, topic, ...}
        self.connection_topics: Dict[str, Set[str]] = {}
        
        # Subscription info: (connection_id, topic) -> WSSubscriptionInfo
        self.subscription_info: Dict[str, WSSubscriptionInfo] = {}
        
        # Pattern subscriptions for wildcard topics
        self.pattern_subscriptions: Dict[str, Pattern] = {}
        self.connection_patterns: Dict[str, Set[str]] = {}
        
        # Statistics
        self.stats = {
            "total_subscriptions": 0,
            "active_subscriptions": 0,
            "total_channels": 0,
            "active_channels": 0,
            "pattern_subscriptions": 0,
            "messages_routed": 0
        }
    
    async def subscribe(self, connection_id: str, topic: str, conn_info: WSConnectionInfo) -> bool:
        """
        Subscribe a connection to a topic
        
        Args:
            connection_id: Connection ID
            topic: Topic to subscribe to
            conn_info: Connection information
            
        Returns:
            bool: True if subscription successful, False otherwise
        """
        # Check if already subscribed
        if connection_id in self.connection_topics and topic in self.connection_topics[connection_id]:
            logger.debug(f"Connection {connection_id} already subscribed to {topic}")
            return True
        
        # Create subscription key
        sub_key = f"{connection_id}:{topic}"
        
        # Create or update subscription info
        subscription = WSSubscriptionInfo(
            topic=topic,
            connection_id=connection_id,
            user_id=conn_info.user_id,
            client_id=conn_info.client_id
        )
        
        # Check if topic contains wildcards
        if '*' in topic or '#' in topic:
            # Create regex pattern for wildcard matching
            pattern_str = topic.replace('+', '[^/]+').replace('#', '.*')
            try:
                pattern = re.compile(f"^{pattern_str}$")
                
                # Store pattern
                pattern_key = f"{connection_id}:{pattern_str}"
                self.pattern_subscriptions[pattern_key] = pattern
                
                # Update connection patterns
                if connection_id not in self.connection_patterns:
                    self.connection_patterns[connection_id] = set()
                self.connection_patterns[connection_id].add(pattern_key)
                
                # Update stats
                self.stats["pattern_subscriptions"] += 1
                logger.debug(f"Connection {connection_id} subscribed to pattern {topic}")
            except re.error:
                logger.error(f"Invalid wildcard pattern: {topic}")
                return False
        else:
            # Add to regular subscriptions
            if topic not in self.subscriptions:
                self.subscriptions[topic] = set()
                self.stats["total_channels"] += 1
                self.stats["active_channels"] += 1
            
            self.subscriptions[topic].add(connection_id)
        
        # Update connection topics
        if connection_id not in self.connection_topics:
            self.connection_topics[connection_id] = set()
        self.connection_topics[connection_id].add(topic)
        
        # Store subscription info
        self.subscription_info[sub_key] = subscription
        
        # Update connection subscriptions list
        if topic not in conn_info.subscriptions:
            conn_info.subscriptions.append(topic)
        
        # Update stats
        self.stats["total_subscriptions"] += 1
        self.stats["active_subscriptions"] += 1
        
        logger.debug(f"Connection {connection_id} subscribed to {topic}")
        return True
    
    async def unsubscribe(self, connection_id: str, topic: str, conn_info: Optional[WSConnectionInfo] = None) -> bool:
        """
        Unsubscribe a connection from a topic
        
        Args:
            connection_id: Connection ID
            topic: Topic to unsubscribe from
            conn_info: Optional connection information
            
        Returns:
            bool: True if unsubscription successful, False otherwise
        """
        # Check if subscribed
        if connection_id not in self.connection_topics or topic not in self.connection_topics[connection_id]:
            logger.debug(f"Connection {connection_id} not subscribed to {topic}")
            return False
        
        # Create subscription key
        sub_key = f"{connection_id}:{topic}"
        
        # Remove from pattern subscriptions if it's a pattern
        if '*' in topic or '#' in topic:
            if connection_id in self.connection_patterns:
                pattern_keys = [k for k in self.connection_patterns[connection_id] if k.split(':', 1)[1] == topic]
                
                for pattern_key in pattern_keys:
                    if pattern_key in self.pattern_subscriptions:
                        del self.pattern_subscriptions[pattern_key]
                        self.stats["pattern_subscriptions"] -= 1
                    
                    self.connection_patterns[connection_id].remove(pattern_key)
                
                if not self.connection_patterns[connection_id]:
                    del self.connection_patterns[connection_id]
        else:
            # Remove from regular subscriptions
            if topic in self.subscriptions and connection_id in self.subscriptions[topic]:
                self.subscriptions[topic].remove(connection_id)
                
                # If no more subscribers, remove the topic
                if not self.subscriptions[topic]:
                    del self.subscriptions[topic]
                    self.stats["active_channels"] -= 1
        
        # Remove from connection topics
        self.connection_topics[connection_id].remove(topic)
        if not self.connection_topics[connection_id]:
            del self.connection_topics[connection_id]
        
        # Remove subscription info
        if sub_key in self.subscription_info:
            del self.subscription_info[sub_key]
        
        # Update connection subscriptions list if conn_info provided
        if conn_info and topic in conn_info.subscriptions:
            conn_info.subscriptions.remove(topic)
        
        # Update stats
        self.stats["active_subscriptions"] -= 1
        
        logger.debug(f"Connection {connection_id} unsubscribed from {topic}")
        return True
    
    async def unsubscribe_all(self, connection_id: str, conn_info: Optional[WSConnectionInfo] = None) -> int:
        """
        Unsubscribe a connection from all topics
        
        Args:
            connection_id: Connection ID
            conn_info: Optional connection information
            
        Returns:
            int: Number of topics unsubscribed from
        """
        if connection_id not in self.connection_topics and connection_id not in self.connection_patterns:
            return 0
        
        # Get topics for this connection
        topics = list(self.connection_topics.get(connection_id, set()))
        count = len(topics)
        
        # Unsubscribe from each topic
        for topic in topics:
            await self.unsubscribe(connection_id, topic, conn_info)
        
        # Check for pattern subscriptions
        if connection_id in self.connection_patterns:
            pattern_keys = list(self.connection_patterns[connection_id])
            
            for pattern_key in pattern_keys:
                if pattern_key in self.pattern_subscriptions:
                    del self.pattern_subscriptions[pattern_key]
                    self.stats["pattern_subscriptions"] -= 1
            
            del self.connection_patterns[connection_id]
            count += len(pattern_keys)
        
        # Clean up connection topics
        if connection_id in self.connection_topics:
            del self.connection_topics[connection_id]
        
        # Clean up subscription info
        for key in list(self.subscription_info.keys()):
            if key.startswith(f"{connection_id}:"):
                del self.subscription_info[key]
        
        # Clear connection subscriptions list if conn_info provided
        if conn_info:
            conn_info.subscriptions = []
        
        logger.debug(f"Connection {connection_id} unsubscribed from all topics ({count} topics)")
        return count
    
    def get_subscribers(self, topic: str) -> Set[str]:
        """
        Get all subscribers for a topic
        
        Args:
            topic: Topic to get subscribers for
            
        Returns:
            Set[str]: Set of connection IDs subscribed to the topic
        """
        # Direct subscribers
        subscribers = set(self.subscriptions.get(topic, set()))
        
        # Pattern subscribers
        for pattern_key, pattern in self.pattern_subscriptions.items():
            connection_id = pattern_key.split(':', 1)[0]
            
            if pattern.match(topic):
                subscribers.add(connection_id)
        
        return subscribers
    
    async def publish(self, topic: str, message: WSMessage, from_connection_id: Optional[str] = None) -> int:
        """
        Publish a message to a topic and get subscribers
        
        This doesn't actually send the message to connections,
        it only determines which connections should receive it.
        
        Args:
            topic: Topic to publish to
            message: Message to publish
            from_connection_id: Optional connection ID of publisher
            
        Returns:
            int: Number of subscribers
        """
        # Set topic in message
        message.topic = topic
        
        # Get subscribers
        subscribers = self.get_subscribers(topic)
        
        # Exclude sender if specified
        if from_connection_id and from_connection_id in subscribers:
            subscribers.remove(from_connection_id)
        
        # Update stats
        self.stats["messages_routed"] += 1
        
        return len(subscribers)
    
    def get_subscription(self, connection_id: str, topic: str) -> Optional[WSSubscriptionInfo]:
        """
        Get subscription information for a connection and topic
        
        Args:
            connection_id: Connection ID
            topic: Topic
            
        Returns:
            Optional[WSSubscriptionInfo]: Subscription information
        """
        sub_key = f"{connection_id}:{topic}"
        return self.subscription_info.get(sub_key)
    
    def get_connection_topics(self, connection_id: str) -> List[str]:
        """
        Get all topics a connection is subscribed to
        
        Args:
            connection_id: Connection ID
            
        Returns:
            List[str]: List of topics
        """
        return list(self.connection_topics.get(connection_id, set()))
    
    def get_topic_subscribers(self, topic: str) -> List[str]:
        """
        Get all subscribers for a topic
        
        Args:
            topic: Topic
            
        Returns:
            List[str]: List of connection IDs
        """
        return list(self.get_subscribers(topic))
    
    def get_topic_count(self) -> int:
        """
        Get total number of active topics
        
        Returns:
            int: Number of topics
        """
        return len(self.subscriptions)
    
    def get_subscription_count(self) -> int:
        """
        Get total number of active subscriptions
        
        Returns:
            int: Number of subscriptions
        """
        return self.stats["active_subscriptions"]
    
    def get_stats(self) -> Dict[str, Any]:
        """
        Get channel manager statistics
        
        Returns:
            Dict[str, Any]: Statistics
        """
        stats = self.stats.copy()
        stats["current_topics"] = len(self.subscriptions)
        
        # Top 10 topics by subscriber count
        top_topics = []
        for topic, subscribers in self.subscriptions.items():
            top_topics.append((topic, len(subscribers)))
        
        top_topics.sort(key=lambda x: x[1], reverse=True)
        stats["top_topics"] = [{"topic": t, "subscribers": c} for t, c in top_topics[:10]]
        
        return stats 