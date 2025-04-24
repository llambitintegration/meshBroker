"""
WebSocket connection models for the backend
"""
import time
import json
from enum import Enum
from typing import Dict, Any, Optional, List
from pydantic import BaseModel, Field


class WSMessageType(str, Enum):
    """WebSocket message types"""
    CONNECT = "connect"
    DISCONNECT = "disconnect"
    SUBSCRIBE = "subscribe"
    UNSUBSCRIBE = "unsubscribe"
    MESSAGE = "message"
    PING = "ping"
    PONG = "pong"
    ERROR = "error"
    STATUS = "status"
    AUTH = "auth"


class WSConnectionInfo(BaseModel):
    """WebSocket connection information"""
    connection_id: str
    client_id: Optional[str] = None
    user_id: Optional[str] = None
    client_info: Dict[str, Any] = Field(default_factory=dict)
    is_authenticated: bool = False
    created_at: float = Field(default_factory=time.time)
    last_activity: float = Field(default_factory=time.time)
    subscriptions: List[str] = Field(default_factory=list)
    metadata: Dict[str, Any] = Field(default_factory=dict)
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary"""
        return {
            "connection_id": self.connection_id,
            "client_id": self.client_id,
            "user_id": self.user_id,
            "client_info": self.client_info,
            "is_authenticated": self.is_authenticated,
            "created_at": self.created_at,
            "last_activity": self.last_activity,
            "subscriptions": self.subscriptions,
            "metadata": self.metadata
        }
    
    def update_activity(self):
        """Update last activity timestamp"""
        self.last_activity = time.time()


class WSMessage(BaseModel):
    """WebSocket message model"""
    type: WSMessageType
    payload: Optional[Dict[str, Any]] = None
    topic: Optional[str] = None
    timestamp: float = Field(default_factory=time.time)
    message_id: Optional[str] = None
    
    def to_json(self) -> str:
        """Convert to JSON string"""
        return json.dumps({
            "type": self.type,
            "payload": self.payload,
            "topic": self.topic,
            "timestamp": self.timestamp,
            "message_id": self.message_id
        })
    
    @classmethod
    def from_json(cls, json_str: str) -> 'WSMessage':
        """Create message from JSON string"""
        try:
            data = json.loads(json_str)
            return cls(
                type=data.get("type"),
                payload=data.get("payload"),
                topic=data.get("topic"),
                timestamp=data.get("timestamp", time.time()),
                message_id=data.get("message_id")
            )
        except json.JSONDecodeError:
            # Return an error message if JSON is invalid
            return cls(
                type=WSMessageType.ERROR,
                payload={"error": "Invalid JSON format"},
                timestamp=time.time()
            )


class WSSubscriptionInfo(BaseModel):
    """WebSocket subscription information"""
    topic: str
    connection_id: str
    user_id: Optional[str] = None
    client_id: Optional[str] = None
    created_at: float = Field(default_factory=time.time)
    metadata: Dict[str, Any] = Field(default_factory=dict) 