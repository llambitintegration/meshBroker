"""
WebSocket routes for the backend
"""
from fastapi import APIRouter, WebSocket, WebSocketDisconnect, Depends, Query, HTTPException, status
from typing import Optional, List, Dict, Any, Set
import json
import logging
import asyncio
from pydantic import BaseModel

from backend.config import settings
from backend.models.user import User, Role
from backend.auth.auth_handler import get_current_user, decode_token
from backend.auth.auth_dependencies import OptionalJWTBearer
from .connection_manager import ConnectionManager
from .channel_manager import ChannelManager
from .connection_models import WSConnectionInfo, WSMessageType, WSMessage

# Configure logger
logger = logging.getLogger(__name__)

# Create router
router = APIRouter(tags=["WebSocket"])

# Create managers
connection_manager = ConnectionManager(
    max_connections=settings.WS_MAX_CONNECTIONS,
    ping_interval=settings.WS_PING_INTERVAL,
    ping_timeout=settings.WS_PING_TIMEOUT
)

channel_manager = ChannelManager()

# Models
class WSStatsResponse(BaseModel):
    """WebSocket statistics response"""
    connection_stats: Dict[str, Any]
    channel_stats: Dict[str, Any]
    
class WSConnectionResponse(BaseModel):
    """WebSocket connection information response"""
    connection_id: str
    client_id: Optional[str] = None
    user_id: Optional[str] = None
    is_authenticated: bool
    created_at: float
    last_activity: float
    subscriptions: List[str]
    
class TopicSubscription(BaseModel):
    """Topic subscription information"""
    topic: str
    subscriber_count: int
    

# Event handlers
async def on_connect(conn_info: WSConnectionInfo):
    """Handle new connection event"""
    logger.debug(f"WebSocket connection event: {conn_info.connection_id}")
    # Additional connection setup can be done here


async def on_disconnect(conn_info: WSConnectionInfo):
    """Handle disconnection event"""
    logger.debug(f"WebSocket disconnection event: {conn_info.connection_id}")
    
    # Unsubscribe from all topics
    await channel_manager.unsubscribe_all(conn_info.connection_id, conn_info)


async def on_message(conn_info: WSConnectionInfo, message: WSMessage):
    """Handle message event"""
    logger.debug(f"WebSocket message event from {conn_info.connection_id}: {message.type}")
    
    # Handle subscription/unsubscription messages
    if message.type == WSMessageType.SUBSCRIBE and message.payload and 'topic' in message.payload:
        topic = message.payload['topic']
        await channel_manager.subscribe(conn_info.connection_id, topic, conn_info)
        
        # Send confirmation message
        confirm_msg = WSMessage(
            type=WSMessageType.STATUS,
            payload={"status": "subscribed", "topic": topic}
        )
        await connection_manager.send_personal_message(confirm_msg, conn_info.connection_id)
    
    elif message.type == WSMessageType.UNSUBSCRIBE and message.payload and 'topic' in message.payload:
        topic = message.payload['topic']
        result = await channel_manager.unsubscribe(conn_info.connection_id, topic, conn_info)
        
        # Send confirmation message
        confirm_msg = WSMessage(
            type=WSMessageType.STATUS,
            payload={
                "status": "unsubscribed" if result else "not_subscribed", 
                "topic": topic
            }
        )
        await connection_manager.send_personal_message(confirm_msg, conn_info.connection_id)
    
    # Handle topic message publishing
    elif message.type == WSMessageType.MESSAGE and message.topic:
        # Only authenticated users can publish
        if not conn_info.is_authenticated:
            error_msg = WSMessage(
                type=WSMessageType.ERROR,
                payload={"error": "Authentication required to publish messages"}
            )
            await connection_manager.send_personal_message(error_msg, conn_info.connection_id)
            return
        
        # Determine subscribers and route message
        subscribers = channel_manager.get_subscribers(message.topic)
        
        # Exclude sender from recipients
        if conn_info.connection_id in subscribers:
            subscribers.remove(conn_info.connection_id)
        
        # Send to all subscribers
        for connection_id in subscribers:
            await connection_manager.send_personal_message(message, connection_id)
        
        # Send confirmation to publisher
        confirm_msg = WSMessage(
            type=WSMessageType.STATUS,
            payload={
                "status": "published", 
                "topic": message.topic,
                "recipients": len(subscribers)
            }
        )
        await connection_manager.send_personal_message(confirm_msg, conn_info.connection_id)


# Set connection manager event handlers
connection_manager.on_connect = on_connect
connection_manager.on_disconnect = on_disconnect
connection_manager.on_message = on_message


@router.websocket("/ws")
async def websocket_endpoint(
    websocket: WebSocket,
    client_id: Optional[str] = None,
    token: Optional[str] = None
):
    """
    WebSocket endpoint with optional authentication
    
    Query parameters:
    - client_id: Optional client identifier
    - token: Optional JWT token for authentication
    """
    connection_id = None
    authenticated_user = None
    
    try:
        # Accept connection
        connection_id = await connection_manager.connect(websocket, client_id)
        
        # Authenticate if token provided
        if token:
            try:
                payload = decode_token(token)
                if payload and payload.get("sub"):
                    username = payload.get("sub")
                    user = User.get(username=username)
                    
                    if user and user.is_active:
                        authenticated_user = user
                        await connection_manager.authenticate(connection_id, user)
            except Exception as e:
                logger.warning(f"Authentication failed for WebSocket connection: {e}")
                error_message = WSMessage(
                    type=WSMessageType.ERROR,
                    payload={"error": "Authentication failed"}
                )
                await connection_manager.send_personal_message(error_message, connection_id)
        
        # Process messages
        while True:
            message_text = await websocket.receive_text()
            await connection_manager.handle_message(connection_id, message_text)
            
    except WebSocketDisconnect:
        logger.info(f"WebSocket client disconnected: {connection_id}")
    except Exception as e:
        logger.error(f"Error in WebSocket connection: {e}")
    finally:
        # Clean up connection
        if connection_id:
            await connection_manager.disconnect(connection_id)


@router.websocket("/ws/authenticated")
async def authenticated_websocket_endpoint(
    websocket: WebSocket,
    token: str = Query(...),
    client_id: Optional[str] = None
):
    """
    Authenticated WebSocket endpoint
    
    Query parameters:
    - token: Required JWT token for authentication
    - client_id: Optional client identifier
    """
    connection_id = None
    
    try:
        # Accept connection
        connection_id = await connection_manager.connect(websocket, client_id)
        
        # Authenticate with token
        try:
            payload = decode_token(token)
            if not payload or not payload.get("sub"):
                raise ValueError("Invalid token")
            
            username = payload.get("sub")
            user = User.get(username=username)
            
            if not user or not user.is_active:
                raise ValueError("User not found or inactive")
            
            # Authenticate connection
            success = await connection_manager.authenticate(connection_id, user)
            if not success:
                raise ValueError("Authentication failed")
            
        except Exception as e:
            logger.warning(f"Authentication failed for WebSocket connection: {e}")
            error_message = WSMessage(
                type=WSMessageType.ERROR,
                payload={"error": "Authentication required"}
            )
            await connection_manager.send_personal_message(error_message, connection_id)
            await connection_manager.disconnect(connection_id)
            return
        
        # Process messages
        while True:
            message_text = await websocket.receive_text()
            await connection_manager.handle_message(connection_id, message_text)
            
    except WebSocketDisconnect:
        logger.info(f"Authenticated WebSocket client disconnected: {connection_id}")
    except Exception as e:
        logger.error(f"Error in authenticated WebSocket connection: {e}")
    finally:
        # Clean up connection
        if connection_id:
            await connection_manager.disconnect(connection_id)


# API endpoints for WebSocket stats and management
@router.get("/ws/stats", response_model=WSStatsResponse)
async def get_websocket_stats():
    """Get WebSocket connection and channel statistics"""
    return WSStatsResponse(
        connection_stats=connection_manager.get_stats(),
        channel_stats=channel_manager.get_stats()
    )


@router.get("/ws/connections", response_model=List[WSConnectionResponse])
async def get_websocket_connections(
    current_user: User = Depends(get_current_user)
):
    """Get active WebSocket connections (admin only)"""
    if not current_user or current_user.role != Role.ADMIN:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Admin access required"
        )
    
    connections = []
    for _, conn_info in connection_manager.active_connections.values():
        connections.append(WSConnectionResponse(
            connection_id=conn_info.connection_id,
            client_id=conn_info.client_id,
            user_id=conn_info.user_id,
            is_authenticated=conn_info.is_authenticated,
            created_at=conn_info.created_at,
            last_activity=conn_info.last_activity,
            subscriptions=conn_info.subscriptions
        ))
    
    return connections


@router.get("/ws/topics", response_model=List[TopicSubscription])
async def get_websocket_topics(
    current_user: User = Depends(get_current_user)
):
    """Get active WebSocket topics and subscriber counts (admin only)"""
    if not current_user or current_user.role != Role.ADMIN:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Admin access required"
        )
    
    topics = []
    for topic, subscribers in channel_manager.subscriptions.items():
        topics.append(TopicSubscription(
            topic=topic,
            subscriber_count=len(subscribers)
        ))
    
    # Sort by subscriber count (descending)
    topics.sort(key=lambda x: x.subscriber_count, reverse=True)
    
    return topics


@router.post("/ws/broadcast/{topic}")
async def broadcast_to_topic(
    topic: str,
    message: Dict[str, Any],
    current_user: User = Depends(get_current_user)
):
    """
    Broadcast a message to all subscribers of a topic
    
    Args:
        topic: Topic to broadcast to
        message: Message payload
    """
    if not current_user or not current_user.is_active:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Authentication required"
        )
    
    # Create message
    ws_message = WSMessage(
        type=WSMessageType.MESSAGE,
        topic=topic,
        payload=message
    )
    
    # Get subscribers
    subscribers = channel_manager.get_subscribers(topic)
    
    # Send to all subscribers
    sent_count = 0
    for connection_id in subscribers:
        if await connection_manager.send_personal_message(ws_message, connection_id):
            sent_count += 1
    
    return {
        "topic": topic,
        "recipients": sent_count,
        "total_subscribers": len(subscribers)
    } 