"""
Enhanced WebSocket connection manager for the backend
"""
import time
import asyncio
import uuid
import json
import logging
from typing import Dict, List, Set, Any, Optional, Callable, Awaitable, Tuple
from fastapi import WebSocket, WebSocketDisconnect, status, HTTPException
from pydantic import BaseModel, Field

from backend.config import settings
from backend.models.user import User
from .connection_models import WSConnectionInfo, WSMessageType, WSMessage

# Configure logger
logger = logging.getLogger(__name__)


class ConnectionManager:
    """Enhanced WebSocket connection manager with authentication and pooling"""
    
    def __init__(self, 
                 max_connections: int = settings.WS_MAX_CONNECTIONS,
                 ping_interval: int = settings.WS_PING_INTERVAL,
                 ping_timeout: int = settings.WS_PING_TIMEOUT,
                 on_connect: Optional[Callable[[WSConnectionInfo], Awaitable[None]]] = None,
                 on_disconnect: Optional[Callable[[WSConnectionInfo], Awaitable[None]]] = None,
                 on_message: Optional[Callable[[WSConnectionInfo, WSMessage], Awaitable[None]]] = None):
        # Connection storage
        self.active_connections: Dict[str, Tuple[WebSocket, WSConnectionInfo]] = {}
        self.authenticated_connections: Set[str] = set()
        
        # Configuration
        self.max_connections = max_connections
        self.ping_interval = ping_interval
        self.ping_timeout = ping_timeout
        
        # Event callbacks
        self.on_connect = on_connect
        self.on_disconnect = on_disconnect
        self.on_message = on_message
        
        # Stats
        self.stats = {
            "total_connections": 0,
            "peak_connections": 0,
            "total_messages": 0,
            "connection_errors": 0,
            "messages_sent": 0,
            "messages_failed": 0
        }
        
        # Background tasks
        self.ping_task = None
        
    async def start_ping_task(self):
        """Start the periodic ping task"""
        if self.ping_task is None:
            self.ping_task = asyncio.create_task(self._ping_connections())
            logger.info("WebSocket ping task started")
    
    async def stop_ping_task(self):
        """Stop the periodic ping task"""
        if self.ping_task:
            self.ping_task.cancel()
            try:
                await self.ping_task
            except asyncio.CancelledError:
                pass
            self.ping_task = None
            logger.info("WebSocket ping task stopped")
    
    async def _ping_connections(self):
        """Periodic ping to keep connections alive and detect stale connections"""
        while True:
            try:
                await asyncio.sleep(self.ping_interval)
                await self._check_connections()
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error in ping task: {e}")
    
    async def _check_connections(self):
        """Check connection health and clean up stale connections"""
        now = time.time()
        to_remove = []
        
        for connection_id, (websocket, conn_info) in self.active_connections.items():
            # Check if connection is stale
            if now - conn_info.last_activity > self.ping_timeout * 2:
                logger.warning(f"Connection {connection_id} timed out, removing")
                to_remove.append(connection_id)
                continue
            
            # Send ping
            try:
                ping_message = WSMessage(
                    type=WSMessageType.PING,
                    payload={"time": now}
                )
                await websocket.send_text(ping_message.to_json())
            except Exception as e:
                logger.warning(f"Failed to ping connection {connection_id}: {e}")
                to_remove.append(connection_id)
        
        # Clean up stale connections
        for connection_id in to_remove:
            await self.disconnect(connection_id)
    
    async def connect(self, websocket: WebSocket, client_id: Optional[str] = None) -> str:
        """
        Accept a new WebSocket connection
        
        Args:
            websocket: WebSocket connection
            client_id: Optional client identifier
            
        Returns:
            str: Connection ID
        """
        # Check if maximum connections reached
        if len(self.active_connections) >= self.max_connections:
            logger.warning("Maximum WebSocket connections reached, rejecting new connection")
            await websocket.close(code=status.WS_1008_POLICY_VIOLATION)
            self.stats["connection_errors"] += 1
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail="Maximum connections reached"
            )
        
        # Accept connection
        await websocket.accept()
        
        # Generate connection ID and create connection info
        connection_id = str(uuid.uuid4())
        client_info = {
            "user_agent": websocket.headers.get("user-agent", ""),
            "host": websocket.client.host,
            "port": websocket.client.port
        }
        
        conn_info = WSConnectionInfo(
            connection_id=connection_id,
            client_id=client_id,
            client_info=client_info,
            created_at=time.time(),
            last_activity=time.time()
        )
        
        # Store connection
        self.active_connections[connection_id] = (websocket, conn_info)
        
        # Update stats
        self.stats["total_connections"] += 1
        current_count = len(self.active_connections)
        if current_count > self.stats["peak_connections"]:
            self.stats["peak_connections"] = current_count
        
        logger.info(f"New WebSocket connection established: {connection_id}. Total connections: {current_count}")
        
        # Send welcome message
        welcome_message = WSMessage(
            type=WSMessageType.CONNECT,
            payload={
                "connection_id": connection_id,
                "message": "Connection established"
            }
        )
        await websocket.send_text(welcome_message.to_json())
        
        # Call connect event handler if provided
        if self.on_connect:
            try:
                await self.on_connect(conn_info)
            except Exception as e:
                logger.error(f"Error in on_connect handler: {e}")
        
        # Start ping task if not already running
        if self.ping_task is None:
            await self.start_ping_task()
        
        return connection_id
    
    async def authenticate(self, connection_id: str, user: User) -> bool:
        """
        Authenticate a WebSocket connection
        
        Args:
            connection_id: Connection ID
            user: User object from authentication
            
        Returns:
            bool: True if authentication successful, False otherwise
        """
        if connection_id not in self.active_connections:
            logger.warning(f"Cannot authenticate non-existent connection: {connection_id}")
            return False
        
        websocket, conn_info = self.active_connections[connection_id]
        
        # Update connection info
        conn_info.is_authenticated = True
        conn_info.user_id = user.username
        conn_info.update_activity()
        
        # Add to authenticated connections set
        self.authenticated_connections.add(connection_id)
        
        # Send auth success message
        auth_message = WSMessage(
            type=WSMessageType.AUTH,
            payload={
                "success": True,
                "user_id": user.username,
                "role": user.role
            }
        )
        
        try:
            await websocket.send_text(auth_message.to_json())
            logger.info(f"WebSocket connection {connection_id} authenticated as {user.username}")
            return True
        except Exception as e:
            logger.error(f"Error sending authentication success message: {e}")
            return False
    
    async def disconnect(self, connection_id: str):
        """
        Disconnect a WebSocket connection
        
        Args:
            connection_id: Connection ID
        """
        if connection_id not in self.active_connections:
            return
        
        websocket, conn_info = self.active_connections[connection_id]
        
        # Call disconnect event handler if provided
        if self.on_disconnect:
            try:
                await self.on_disconnect(conn_info)
            except Exception as e:
                logger.error(f"Error in on_disconnect handler: {e}")
        
        # Remove from authenticated connections set
        if connection_id in self.authenticated_connections:
            self.authenticated_connections.remove(connection_id)
        
        # Close WebSocket connection
        try:
            await websocket.close()
        except Exception as e:
            logger.warning(f"Error closing WebSocket connection {connection_id}: {e}")
        
        # Remove from active connections
        del self.active_connections[connection_id]
        
        logger.info(f"WebSocket connection {connection_id} closed. Remaining connections: {len(self.active_connections)}")
        
        # Stop ping task if no more connections
        if not self.active_connections and self.ping_task is not None:
            await self.stop_ping_task()
    
    async def disconnect_all(self):
        """Disconnect all active WebSocket connections"""
        connection_ids = list(self.active_connections.keys())
        for connection_id in connection_ids:
            await self.disconnect(connection_id)
        
        logger.info(f"All WebSocket connections disconnected")
    
    async def send_personal_message(self, message: WSMessage, connection_id: str) -> bool:
        """
        Send a message to a specific connection
        
        Args:
            message: Message to send
            connection_id: Connection ID
            
        Returns:
            bool: True if message sent successfully, False otherwise
        """
        if connection_id not in self.active_connections:
            logger.warning(f"Cannot send message to non-existent connection: {connection_id}")
            return False
        
        websocket, conn_info = self.active_connections[connection_id]
        
        try:
            await websocket.send_text(message.to_json())
            conn_info.update_activity()
            self.stats["messages_sent"] += 1
            return True
        except Exception as e:
            logger.error(f"Error sending message to connection {connection_id}: {e}")
            self.stats["messages_failed"] += 1
            
            # Check if connection needs to be closed
            try:
                await websocket.send_text(WSMessage(
                    type=WSMessageType.PING,
                    payload={"time": time.time()}
                ).to_json())
            except Exception:
                logger.warning(f"Connection {connection_id} appears to be closed, disconnecting")
                await self.disconnect(connection_id)
            
            return False
    
    async def broadcast(self, message: WSMessage, exclude: Optional[Set[str]] = None) -> int:
        """
        Broadcast a message to all connections
        
        Args:
            message: Message to broadcast
            exclude: Set of connection IDs to exclude from broadcast
            
        Returns:
            int: Number of connections message was sent to
        """
        if not self.active_connections:
            logger.debug("No active connections for broadcast")
            return 0
        
        exclude_set = exclude or set()
        sent_count = 0
        
        for connection_id, (websocket, conn_info) in list(self.active_connections.items()):
            if connection_id in exclude_set:
                continue
            
            try:
                await websocket.send_text(message.to_json())
                conn_info.update_activity()
                sent_count += 1
                self.stats["messages_sent"] += 1
            except Exception as e:
                logger.error(f"Error broadcasting to connection {connection_id}: {e}")
                self.stats["messages_failed"] += 1
                
                # Check if connection needs to be closed
                try:
                    await websocket.send_text(WSMessage(
                        type=WSMessageType.PING,
                        payload={"time": time.time()}
                    ).to_json())
                except Exception:
                    logger.warning(f"Connection {connection_id} appears to be closed, disconnecting")
                    await self.disconnect(connection_id)
        
        return sent_count
    
    async def broadcast_authenticated(self, message: WSMessage, exclude: Optional[Set[str]] = None) -> int:
        """
        Broadcast a message to authenticated connections only
        
        Args:
            message: Message to broadcast
            exclude: Set of connection IDs to exclude from broadcast
            
        Returns:
            int: Number of connections message was sent to
        """
        if not self.authenticated_connections:
            logger.debug("No authenticated connections for broadcast")
            return 0
        
        exclude_set = exclude or set()
        sent_count = 0
        
        for connection_id in list(self.authenticated_connections):
            if connection_id in exclude_set:
                continue
            
            if connection_id not in self.active_connections:
                self.authenticated_connections.remove(connection_id)
                continue
            
            websocket, conn_info = self.active_connections[connection_id]
            
            try:
                await websocket.send_text(message.to_json())
                conn_info.update_activity()
                sent_count += 1
                self.stats["messages_sent"] += 1
            except Exception as e:
                logger.error(f"Error broadcasting to authenticated connection {connection_id}: {e}")
                self.stats["messages_failed"] += 1
                
                # Check if connection needs to be closed
                try:
                    await websocket.send_text(WSMessage(
                        type=WSMessageType.PING,
                        payload={"time": time.time()}
                    ).to_json())
                except Exception:
                    logger.warning(f"Connection {connection_id} appears to be closed, disconnecting")
                    await self.disconnect(connection_id)
        
        return sent_count
    
    async def broadcast_to_users(self, message: WSMessage, user_ids: List[str]) -> int:
        """
        Broadcast a message to specific users
        
        Args:
            message: Message to broadcast
            user_ids: List of user IDs to broadcast to
            
        Returns:
            int: Number of connections message was sent to
        """
        if not self.active_connections:
            logger.debug("No active connections for user broadcast")
            return 0
        
        # Create a set for efficient lookup
        user_id_set = set(user_ids)
        sent_count = 0
        
        for connection_id, (websocket, conn_info) in list(self.active_connections.items()):
            if not conn_info.is_authenticated or conn_info.user_id not in user_id_set:
                continue
            
            try:
                await websocket.send_text(message.to_json())
                conn_info.update_activity()
                sent_count += 1
                self.stats["messages_sent"] += 1
            except Exception as e:
                logger.error(f"Error broadcasting to user connection {connection_id}: {e}")
                self.stats["messages_failed"] += 1
                
                # Check if connection needs to be closed
                try:
                    await websocket.send_text(WSMessage(
                        type=WSMessageType.PING,
                        payload={"time": time.time()}
                    ).to_json())
                except Exception:
                    logger.warning(f"Connection {connection_id} appears to be closed, disconnecting")
                    await self.disconnect(connection_id)
        
        return sent_count
    
    async def broadcast_by_client_id(self, message: WSMessage, client_ids: List[str]) -> int:
        """
        Broadcast a message to specific client IDs
        
        Args:
            message: Message to broadcast
            client_ids: List of client IDs to broadcast to
            
        Returns:
            int: Number of connections message was sent to
        """
        if not self.active_connections:
            logger.debug("No active connections for client broadcast")
            return 0
        
        # Create a set for efficient lookup
        client_id_set = set(client_ids)
        sent_count = 0
        
        for connection_id, (websocket, conn_info) in list(self.active_connections.items()):
            if not conn_info.client_id or conn_info.client_id not in client_id_set:
                continue
            
            try:
                await websocket.send_text(message.to_json())
                conn_info.update_activity()
                sent_count += 1
                self.stats["messages_sent"] += 1
            except Exception as e:
                logger.error(f"Error broadcasting to client connection {connection_id}: {e}")
                self.stats["messages_failed"] += 1
                
                # Check if connection needs to be closed
                try:
                    await websocket.send_text(WSMessage(
                        type=WSMessageType.PING,
                        payload={"time": time.time()}
                    ).to_json())
                except Exception:
                    logger.warning(f"Connection {connection_id} appears to be closed, disconnecting")
                    await self.disconnect(connection_id)
        
        return sent_count
    
    async def handle_message(self, connection_id: str, message_text: str) -> Optional[WSMessage]:
        """
        Handle a received message from a WebSocket connection
        
        Args:
            connection_id: Connection ID
            message_text: Raw message text
            
        Returns:
            Optional[WSMessage]: The processed message or None
        """
        if connection_id not in self.active_connections:
            logger.warning(f"Received message from non-existent connection: {connection_id}")
            return None
        
        websocket, conn_info = self.active_connections[connection_id]
        conn_info.update_activity()
        
        # Parse message
        try:
            message = WSMessage.from_json(message_text)
            self.stats["total_messages"] += 1
        except Exception as e:
            logger.error(f"Error parsing message from connection {connection_id}: {e}")
            error_message = WSMessage(
                type=WSMessageType.ERROR,
                payload={"error": "Invalid message format"}
            )
            await self.send_personal_message(error_message, connection_id)
            return None
        
        # Handle specific message types
        if message.type == WSMessageType.PING:
            # Respond to ping
            pong_message = WSMessage(
                type=WSMessageType.PONG,
                payload={
                    "time": time.time(),
                    "ping_time": message.payload.get("time") if message.payload else None
                }
            )
            await self.send_personal_message(pong_message, connection_id)
            return message
        
        elif message.type == WSMessageType.PONG:
            # Just update activity timestamp
            return message
        
        # Call message event handler if provided
        if self.on_message:
            try:
                await self.on_message(conn_info, message)
            except Exception as e:
                logger.error(f"Error in on_message handler: {e}")
        
        return message
    
    def get_connection_info(self, connection_id: str) -> Optional[WSConnectionInfo]:
        """
        Get information about a connection
        
        Args:
            connection_id: Connection ID
            
        Returns:
            Optional[WSConnectionInfo]: Connection info or None if not found
        """
        if connection_id not in self.active_connections:
            return None
        
        _, conn_info = self.active_connections[connection_id]
        return conn_info
    
    def get_stats(self) -> Dict[str, Any]:
        """
        Get connection manager statistics
        
        Returns:
            Dict[str, Any]: Statistics
        """
        stats = self.stats.copy()
        stats["current_connections"] = len(self.active_connections)
        stats["authenticated_connections"] = len(self.authenticated_connections)
        
        if stats.get("messages_sent", 0) > 0:
            stats["failure_rate"] = round(stats.get("messages_failed", 0) / stats.get("messages_sent", 1) * 100, 2)
        else:
            stats["failure_rate"] = 0
        
        return stats
    
    def get_connection_count(self) -> int:
        """
        Get total number of active connections
        
        Returns:
            int: Number of active connections
        """
        return len(self.active_connections)
    
    def get_authenticated_count(self) -> int:
        """
        Get number of authenticated connections
        
        Returns:
            int: Number of authenticated connections
        """
        return len(self.authenticated_connections)
    
    def get_connections_by_user(self, user_id: str) -> List[WSConnectionInfo]:
        """
        Get all connections for a specific user
        
        Args:
            user_id: User ID
            
        Returns:
            List[WSConnectionInfo]: List of connection info objects
        """
        connections = []
        for _, conn_info in self.active_connections.values():
            if conn_info.is_authenticated and conn_info.user_id == user_id:
                connections.append(conn_info)
        
        return connections 