import re
import json
import logging
from typing import Dict, List, Any, Optional, Callable, Pattern, Union
from enum import Enum, auto

# Configure logger
logger = logging.getLogger(__name__)

class FilterAction(Enum):
    """Enum for filter actions"""
    ACCEPT = auto()  # Accept the message
    REJECT = auto()  # Reject the message
    TRANSFORM = auto()  # Apply transformation to the message


class MessageFilter:
    """Filter for MQTT messages"""
    
    def __init__(self, name: str, description: str = ""):
        self.name = name
        self.description = description
    
    def matches(self, topic: str, payload: Any) -> bool:
        """Check if the message matches this filter"""
        # Base implementation always matches
        return True
    
    def get_action(self) -> FilterAction:
        """Get the action to take when this filter matches"""
        return FilterAction.ACCEPT
    
    def transform(self, topic: str, payload: Any) -> tuple[str, Any]:
        """Transform the message (if action is TRANSFORM)"""
        return topic, payload


class TopicFilter(MessageFilter):
    """Filter messages based on topic patterns"""
    
    def __init__(self, name: str, topic_pattern: str, action: FilterAction = FilterAction.ACCEPT):
        super().__init__(name, f"Filter by topic pattern: {topic_pattern}")
        self.topic_pattern = topic_pattern
        self._action = action
        # Convert MQTT wildcards to regex patterns
        pattern = topic_pattern.replace("+", "[^/]+").replace("#", ".*")
        self._regex = re.compile(f"^{pattern}$")
    
    def matches(self, topic: str, payload: Any) -> bool:
        """Check if the topic matches the pattern"""
        return bool(self._regex.match(topic))
    
    def get_action(self) -> FilterAction:
        """Get the action for this filter"""
        return self._action


class PayloadFilter(MessageFilter):
    """Filter messages based on payload content"""
    
    def __init__(self, name: str, json_path: str, value_pattern: Union[str, Pattern], 
                action: FilterAction = FilterAction.ACCEPT):
        super().__init__(name, f"Filter by payload content: {json_path} matching {value_pattern}")
        self.json_path = json_path  # Example: "user.name" or "position.latitude"
        self.value_pattern = value_pattern
        self._action = action
        
        if isinstance(value_pattern, str):
            self._regex = re.compile(value_pattern)
        else:
            self._regex = value_pattern
    
    def matches(self, topic: str, payload: Any) -> bool:
        """Check if the payload matches the filter"""
        # Convert payload to dict if it's a string
        if isinstance(payload, str):
            try:
                payload_dict = json.loads(payload)
            except json.JSONDecodeError:
                return False
        elif isinstance(payload, dict):
            payload_dict = payload
        else:
            return False
        
        # Navigate the JSON path
        parts = self.json_path.split('.')
        current = payload_dict
        
        for part in parts:
            if isinstance(current, dict) and part in current:
                current = current[part]
            else:
                return False
        
        # Check if the value matches the pattern
        return bool(self._regex.search(str(current)))
    
    def get_action(self) -> FilterAction:
        """Get the action for this filter"""
        return self._action


class MessageTransformer(MessageFilter):
    """Transform messages based on a custom transformation function"""
    
    def __init__(self, name: str, transform_func: Callable[[str, Any], tuple[str, Any]]):
        super().__init__(name, "Custom message transformer")
        self.transform_func = transform_func
    
    def get_action(self) -> FilterAction:
        """This filter always transforms messages"""
        return FilterAction.TRANSFORM
    
    def transform(self, topic: str, payload: Any) -> tuple[str, Any]:
        """Apply the transformation function"""
        return self.transform_func(topic, payload)


class TopicTransformer(MessageFilter):
    """Transform message topics based on a pattern"""
    
    def __init__(self, name: str, topic_pattern: str, replacement: str):
        super().__init__(name, f"Transform topic from {topic_pattern} to {replacement}")
        self.topic_pattern = topic_pattern
        self.replacement = replacement
        # Convert MQTT wildcards to regex patterns with capturing groups
        pattern = topic_pattern.replace("+", "([^/]+)").replace("#", "(.*)")
        self._regex = re.compile(f"^{pattern}$")
    
    def matches(self, topic: str, payload: Any) -> bool:
        """Check if the topic matches the pattern"""
        return bool(self._regex.match(topic))
    
    def get_action(self) -> FilterAction:
        """This filter transforms messages"""
        return FilterAction.TRANSFORM
    
    def transform(self, topic: str, payload: Any) -> tuple[str, Any]:
        """Transform the topic based on the pattern"""
        match = self._regex.match(topic)
        if match:
            groups = match.groups()
            new_topic = self.replacement
            for i, group in enumerate(groups):
                new_topic = new_topic.replace(f"${i+1}", group)
            return new_topic, payload
        return topic, payload


class FilterChain:
    """Chain of filters to process messages"""
    
    def __init__(self, name: str, filters: Optional[List[MessageFilter]] = None):
        self.name = name
        self.filters = filters or []
    
    def add_filter(self, filter: MessageFilter):
        """Add a filter to the chain"""
        self.filters.append(filter)
    
    def process(self, topic: str, payload: Any) -> Optional[tuple[str, Any]]:
        """Process a message through the filter chain
        
        Returns:
            tuple[str, Any] or None: The processed message (topic, payload) or None if rejected
        """
        current_topic = topic
        current_payload = payload
        
        for filter in self.filters:
            if filter.matches(current_topic, current_payload):
                action = filter.get_action()
                
                if action == FilterAction.REJECT:
                    logger.debug(f"Message on topic '{current_topic}' rejected by filter '{filter.name}'")
                    return None
                elif action == FilterAction.TRANSFORM:
                    current_topic, current_payload = filter.transform(current_topic, current_payload)
                    logger.debug(f"Message transformed by filter '{filter.name}'")
            
        return current_topic, current_payload


class MessageProcessor:
    """Process messages through filter chains based on topic patterns"""
    
    def __init__(self):
        self.filter_chains: Dict[Pattern, FilterChain] = {}
        self.default_chain = FilterChain("Default")
    
    def add_filter_chain(self, topic_pattern: str, chain: FilterChain):
        """Add a filter chain for a specific topic pattern"""
        # Convert MQTT wildcards to regex
        pattern = topic_pattern.replace("+", "[^/]+").replace("#", ".*")
        regex = re.compile(f"^{pattern}$")
        self.filter_chains[regex] = chain
    
    def add_default_filter(self, filter: MessageFilter):
        """Add a filter to the default chain"""
        self.default_chain.add_filter(filter)
    
    def process_message(self, topic: str, payload: Any) -> Optional[tuple[str, Any]]:
        """Process a message through the appropriate filter chain"""
        # Find matching filter chains
        for pattern, chain in self.filter_chains.items():
            if pattern.match(topic):
                logger.debug(f"Processing message on topic '{topic}' with chain '{chain.name}'")
                result = chain.process(topic, payload)
                if result is not None:
                    return result
                return None
        
        # Use default chain if no specific chain matches
        logger.debug(f"Processing message on topic '{topic}' with default chain")
        return self.default_chain.process(topic, payload) 