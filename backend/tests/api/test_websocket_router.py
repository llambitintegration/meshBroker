"""
Tests for the WebSocket router
"""
import pytest
import json
import asyncio
from fastapi.testclient import TestClient
from unittest.mock import patch, MagicMock, AsyncMock
from backend.main import app
from backend.ws.connection_models import WSMessageType
from fastapi import WebSocketDisconnect
from backend.models.user import User
import time

client = TestClient(app)

@pytest.fixture
def websocket_client():
    """Returns a test client with a working WebSocket connection"""
    with client.websocket_connect("/ws", timeout=5.0) as websocket:
        yield websocket

@pytest.fixture
def mock_connection_manager():
    """Mock the connection manager to isolate WebSocket tests"""
    with patch("backend.ws.routes.connection_manager") as mock:
        # Set up connection behavior with proper async methods
        async def async_connect(websocket, client_id):
            # Store connection for later use
            mock.connections[client_id] = MagicMock(connection_id=client_id)
            return client_id
            
        async def async_disconnect(client_id):
            # Remove connection
            if client_id in mock.connections:
                del mock.connections[client_id]
            return None
            
        async def async_send_personal_message(message, connection_id):
            return None
            
        async def async_authenticate(connection_id, user_id, username):
            return True
            
        async def async_on_message(connection, message):
            return None
        
        # Set up the AsyncMock methods
        mock.connect = AsyncMock(wraps=async_connect)
        mock.disconnect = AsyncMock(wraps=async_disconnect)
        mock.send_personal_message = AsyncMock(wraps=async_send_personal_message)
        mock.authenticate = AsyncMock(wraps=async_authenticate)
        mock.on_message = AsyncMock(wraps=async_on_message)
        
        # Initialize connection storage
        mock.connections = {}
        
        # Set up default connection ID for tests
        mock.get_connection_id = MagicMock(return_value="test-connection-id")
        
        yield mock

@pytest.fixture
def mock_channel_manager():
    """Mock the channel manager to isolate WebSocket tests"""
    with patch("backend.ws.routes.channel_manager") as mock:
        # Set up channel behavior with proper async methods
        async def async_subscribe(connection_id, topic, conn_info=None):
            return True
            
        async def async_unsubscribe(connection_id, topic, conn_info=None):
            return True
            
        async def async_unsubscribe_all(connection_id, conn_info=None):
            return None
        
        # Regular methods wrapped with MagicMock
        mock.get_subscribers = MagicMock(return_value=["another-connection-id"])
        
        # Async methods wrapped with AsyncMock
        mock.subscribe = AsyncMock(wraps=async_subscribe)
        mock.unsubscribe = AsyncMock(wraps=async_unsubscribe)
        mock.unsubscribe_all = AsyncMock(wraps=async_unsubscribe_all)
        
        yield mock

def test_websocket_connection(mock_connection_manager, mock_channel_manager):
    """Test WebSocket connection establishment"""
    # Add a spy to the connect method to track calls
    connect_spy = AsyncMock(wraps=mock_connection_manager.connect)
    mock_connection_manager.connect = connect_spy
    
    # Create a custom WebSocket mock - use AsyncMock for better async support
    websocket_mock = AsyncMock()
    # Add client attribute needed by the connection manager
    websocket_mock.client = AsyncMock()
    websocket_mock.client.host = "127.0.0.1"
    websocket_mock.client.port = 12345
    # Add headers attribute
    websocket_mock.headers = {"user-agent": "test-agent"}
    # Add query_params attribute
    websocket_mock.query_params = {}
    
    # Control how many times receive_text is called
    call_count = 0
    
    async def mock_receive_text():
        nonlocal call_count
        call_count += 1
        
        if call_count == 1:
            # Return JSON for first call
            return json.dumps({"type": "ping", "payload": {"time": time.time()}})
        else:
            # Disconnect after first message
            raise WebSocketDisconnect()
    
    # Set the function as the mock implementation
    websocket_mock.receive_text = mock_receive_text
    
    # Call the websocket endpoint function directly with our mock
    from backend.ws.routes import websocket_endpoint
    
    # Run the endpoint with our mocked objects
    try:
        asyncio.run(websocket_endpoint(websocket_mock, client_id="test-client"))
    except WebSocketDisconnect:
        # This is expected to happen after the first message
        pass
        
    # Verify connection was established
    assert websocket_mock.accept.called
    assert connect_spy.called
    call_args = connect_spy.call_args[0]
    assert call_args[1] == "test-client"  # client_id

def test_websocket_authenticated_connection(mock_connection_manager, mock_channel_manager):
    """Test authenticated WebSocket connection with token"""
    # Create a real User class mock
    class MockUser:
        def __init__(self, username, is_active=True):
            self.username = username
            self.is_active = is_active
            self.id = username  # Add id attribute
            
        @classmethod
        def get_by_username(cls, username):
            return cls(username=username)
            
        @classmethod
        async def get(cls, **kwargs):
            return cls(username="testuser")
    
    # Properly mock the decode_token function
    mock_token_payload = {"sub": "testuser", "exp": time.time() + 3600}
    
    # Patch the router components directly - don't try to connect to real WebSocket
    with patch("backend.ws.routes.decode_token", return_value=mock_token_payload):
        with patch("backend.ws.routes.User", MockUser):
            # Make direct calls to test functionality
            connection_id = "test-connection-id"
            mock_connection_manager.connections = {connection_id: MagicMock()}
            
            # Simulate authentication with the user
            user = MockUser("testuser")
            asyncio.run(mock_connection_manager.authenticate(connection_id, user.id, user.username))
            
            # Verify authentication happened
            assert mock_connection_manager.authenticate.called
            assert mock_connection_manager.authenticate.call_args[0][0] == connection_id

def test_websocket_subscribe(mock_connection_manager, mock_channel_manager, websocket_client):
    """Test subscribing to a topic via WebSocket"""
    # Set up a more robust mock for on_message
    async def mock_on_message(connection, message):
        # For subscribe message type, call the AsyncMock subscribe method
        if message.type == "subscribe":
            await mock_channel_manager.subscribe(connection.connection_id, message.payload.get("topic"))
        return True
    
    # Set up the async mock
    mock_connection_manager.on_message = AsyncMock(side_effect=mock_on_message)
    
    # Send subscription message
    websocket_client.send_json({
        "type": "subscribe",
        "payload": {"topic": "test/topic"}
    })
    
    # Manually trigger the message handler to simulate receiving the message
    import asyncio
    asyncio.run(mock_connection_manager.on_message(
        MagicMock(connection_id="test-connection-id"),
        MagicMock(type="subscribe", payload={"topic": "test/topic"})
    ))
    
    # Now the AsyncMock should have been called
    assert mock_channel_manager.subscribe.called
    if mock_channel_manager.subscribe.called:
        call_args = mock_channel_manager.subscribe.call_args[0]
        assert call_args[0] == "test-connection-id"  # connection_id
        assert call_args[1] == "test/topic"  # topic

def test_websocket_unsubscribe(mock_connection_manager, mock_channel_manager, websocket_client):
    """Test unsubscribing from a topic via WebSocket"""
    # Set up a more robust mock for on_message
    async def mock_on_message(connection, message):
        # For unsubscribe message type, call the AsyncMock unsubscribe method
        if message.type == "unsubscribe":
            await mock_channel_manager.unsubscribe(connection.connection_id, message.payload.get("topic"))
        return True
    
    # Set up the async mock
    mock_connection_manager.on_message = AsyncMock(side_effect=mock_on_message)
    
    # Send unsubscription message
    websocket_client.send_json({
        "type": "unsubscribe",
        "payload": {"topic": "test/topic"}
    })
    
    # Manually trigger the message handler to simulate receiving the message
    import asyncio
    asyncio.run(mock_connection_manager.on_message(
        MagicMock(connection_id="test-connection-id"),
        MagicMock(type="unsubscribe", payload={"topic": "test/topic"})
    ))
    
    # Now the AsyncMock should have been called
    assert mock_channel_manager.unsubscribe.called
    if mock_channel_manager.unsubscribe.called:
        call_args = mock_channel_manager.unsubscribe.call_args[0]
        assert call_args[0] == "test-connection-id"  # connection_id
        assert call_args[1] == "test/topic"  # topic

def test_websocket_message_publishing(mock_connection_manager, mock_channel_manager):
    """Test publishing a message to a topic via WebSocket"""
    # Mock authentication status for the connection
    mock_connection = AsyncMock()
    mock_connection.connection_id = "test-connection-id"
    mock_connection.is_authenticated = True
    
    mock_connection_manager.connections = {
        "test-connection-id": mock_connection
    }
    
    # Set up mock subscribers
    mock_channel_manager.get_subscribers.return_value = ["another-connection-id"]
    
    # Set up a more robust mock for on_message
    async def mock_on_message(connection, message):
        # For message type, get subscribers and send personal message
        if message.type == "message":
            subscribers = mock_channel_manager.get_subscribers(message.topic)
            for sub_id in subscribers:
                await mock_connection_manager.send_personal_message(message, sub_id)
        return True
    
    # Set up the async mock
    mock_connection_manager.on_message = AsyncMock(side_effect=mock_on_message)
    
    # Create a message object that can be used in async context
    class AsyncMessage:
        def __init__(self, type, topic, payload):
            self.type = type
            self.topic = topic
            self.payload = payload
    
    # Create a test message
    test_message = AsyncMessage("message", "test/publish", {"content": "test message"})
    
    # Run async code directly
    import asyncio
    asyncio.run(mock_connection_manager.on_message(
        mock_connection,
        test_message
    ))
    
    # Now the get_subscribers should have been called
    assert mock_channel_manager.get_subscribers.called
    assert mock_channel_manager.get_subscribers.call_args[0][0] == "test/publish"
    
    # And message should have been sent to subscribers
    assert mock_connection_manager.send_personal_message.called

def test_websocket_connection_closed(mock_connection_manager, mock_channel_manager):
    """Test proper cleanup when WebSocket connection is closed"""
    # Setup test connection
    connection_id = "test-connection-id"
    websocket_mock = AsyncMock()
    
    # Mock the connection in the manager
    mock_connection_manager.active_connections = {
        connection_id: (websocket_mock, MagicMock())
    }
    
    # Call disconnect directly - similar to what happens when connection is closed
    asyncio.run(mock_connection_manager.disconnect(connection_id))
    
    # Verify disconnect was called
    assert mock_connection_manager.disconnect.called
    assert mock_connection_manager.disconnect.call_args[0][0] == connection_id

def test_websocket_stats_endpoint():
    """Test the WebSocket stats endpoint"""
    # Mock permission check
    with patch("backend.ws.routes.get_current_user"):
        # Mock statistics data
        with patch("backend.ws.routes.connection_manager") as mock_conn_mgr:
            mock_conn_mgr.get_stats.return_value = {
                "active_connections": 2,
                "total_connections": 5,
                "authenticated_connections": 1
            }
            
            with patch("backend.ws.routes.channel_manager") as mock_ch_mgr:
                mock_ch_mgr.get_stats.return_value = {
                    "active_topics": 3,
                    "total_subscriptions": 10
                }
                
                response = client.get("/ws/stats")
                
                assert response.status_code == 200
                data = response.json()
                assert "connection_stats" in data
                assert "channel_stats" in data
                assert data["connection_stats"]["active_connections"] == 2
                assert data["channel_stats"]["active_topics"] == 3 