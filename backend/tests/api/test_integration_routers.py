"""
Integration tests for the router interactions
"""
import pytest
import time
import json
import threading
from fastapi.testclient import TestClient
from unittest.mock import patch, MagicMock, PropertyMock
from backend.main import app
from backend.ws.connection_models import WSMessageType

client = TestClient(app)

class MQTTWebSocketIntegrationFixture:
    """Fixture for testing MQTT and WebSocket integration"""
    def __init__(self):
        self.mqtt_messages = []
        self.websocket_messages = []
        self.published_mqtt_ids = []
        
        # Create mocks
        self.setup_mqtt_mock()
        self.setup_websocket_mocks()
        
    def setup_mqtt_mock(self):
        """Set up mock MQTT handler"""
        self.mqtt_handler = MagicMock()
        
        # Mock connection state - critical for tests
        self.mqtt_handler.is_connected.return_value = True
        self.mqtt_handler.connected = True
        
        # Mock _publish_direct method to prevent real MQTT connections
        self.mqtt_handler._publish_direct.return_value = MagicMock(rc=0, mid=12345)
        
        # Set up publish behavior
        self.mqtt_handler.publish.side_effect = self.mock_mqtt_publish
        
        # Set up subscribe behavior
        self.mqtt_handler.subscribe.side_effect = self.mock_mqtt_subscribe
        
        # Set up unsubscribe behavior
        self.mqtt_handler.unsubscribe.return_value = True
        
        # Set up get_subscribed_topics behavior
        self.mqtt_handler.get_subscribed_topics.side_effect = self.mock_mqtt_get_topics
        
        # Initialize subscribed topics
        self.subscribed_topics = set()
        
        # Mock MQTT client for health checks
        self.mqtt_handler._mqtt_client = MagicMock()
        self.mqtt_handler._mqtt_client.is_connected.return_value = True
        
        # Mock delivery tracker
        self.mqtt_handler.delivery_tracker = MagicMock()
        
        # Mock broker monitor and disable real health checks
        self.mqtt_handler.broker_monitor = MagicMock()
        self.mqtt_handler.broker_monitor.get_overall_status.return_value = {"status": "healthy"}
    
    def setup_websocket_mocks(self):
        """Set up WebSocket connection and channel manager mocks"""
        self.connection_manager = MagicMock()
        self.channel_manager = MagicMock()
        
        # Set up connection manager behavior
        self.connection_manager.send_personal_message.side_effect = self.mock_ws_send_message
        
        # Set up channel manager behavior
        self.channel_manager.get_subscribers.side_effect = self.mock_get_subscribers
        
        # Initialize WebSocket subscribers
        self.ws_subscribers = {
            "test/topic": ["ws-conn-1", "ws-conn-2"],
            "mqtt/test": ["ws-conn-1"]
        }
        
    def mock_mqtt_publish(self, topic, payload, qos=None, retain=False, message_id=None):
        """Mock MQTT publish method"""
        msg_id = message_id or len(self.mqtt_messages) + 1000
        self.mqtt_messages.append({
            "id": msg_id,
            "topic": topic,
            "payload": payload,
            "qos": qos,
            "retain": retain
        })
        
        # Simulate message distribution to WebSocket subscribers
        if topic in self.ws_subscribers:
            for conn_id in self.ws_subscribers[topic]:
                self.websocket_messages.append({
                    "connection_id": conn_id,
                    "message": {
                        "type": "message",
                        "topic": topic,
                        "payload": payload
                    }
                })
        
        self.published_mqtt_ids.append(msg_id)
        return msg_id  # Return message ID for tracking to work with message tracking logic
    
    def mock_mqtt_subscribe(self, topic, qos=None):
        """Mock MQTT subscribe method"""
        self.subscribed_topics.add(topic)
        return [0]  # Success result
    
    def mock_mqtt_get_topics(self):
        """Mock getting subscribed topics"""
        return list(self.subscribed_topics)
    
    def mock_ws_send_message(self, message, connection_id):
        """Mock sending a WebSocket message"""
        self.websocket_messages.append({
            "connection_id": connection_id,
            "message": message if isinstance(message, dict) else message.__dict__
        })
        return None
    
    def mock_get_subscribers(self, topic):
        """Mock getting WebSocket subscribers for a topic"""
        return self.ws_subscribers.get(topic, [])
    
    def reset(self):
        """Reset the state of the fixture"""
        self.mqtt_messages = []
        self.websocket_messages = []
        self.published_mqtt_ids = []
        self.subscribed_topics = set()

@pytest.fixture
def integration_fixture():
    """Provide the integration fixture"""
    fixture = MQTTWebSocketIntegrationFixture()
    
    # Create a more targeted patching strategy
    patches = [
        # Patch get_mqtt_handler to return our mock
        patch("backend.mqtt.mqtt_handler.get_mqtt_handler", return_value=fixture.mqtt_handler),
        
        # Patch WebSocket dependencies
        patch("backend.ws.routes.connection_manager", fixture.connection_manager),
        patch("backend.ws.routes.channel_manager", fixture.channel_manager),
        
        # Patch broker monitor health check
        patch("backend.monitoring.broker_monitor.BrokerHealthCheck._check_mqtt_connectivity", return_value=True),
        
        # Make sure MQTT client behaves correctly during health checks
        patch("backend.mqtt_handler.mqtt.Client", return_value=fixture.mqtt_handler._mqtt_client),
    ]
    
    # Apply all patches
    for p in patches:
        p.start()
    
    # Set up side effects for mock methods
    fixture.mqtt_handler.publish.side_effect = fixture.mock_mqtt_publish
    fixture.mqtt_handler.subscribe.side_effect = fixture.mock_mqtt_subscribe
    fixture.mqtt_handler.get_subscribed_topics.side_effect = fixture.mock_mqtt_get_topics
    
    try:
        yield fixture
    finally:
        # Stop all patches
        for p in patches:
            p.stop()
        
        fixture.reset()

@pytest.fixture
def mock_limiter():
    """Mock the rate limiter to avoid rate limiting in tests"""
    with patch("backend.routers.mqtt_publish.limiter") as mock_pub:
        mock_pub.limit.return_value = lambda f: f
        with patch("backend.routers.mqtt_subscribe.limiter") as mock_sub:
            mock_sub.limit.return_value = lambda f: f
            yield (mock_pub, mock_sub)

def test_mqtt_to_websocket_flow(integration_fixture, mock_limiter):
    """Test flow from MQTT publish to WebSocket message distribution"""
    # Publish an MQTT message to a topic that has WebSocket subscribers
    response = client.post(
        "/mqtt/publish",
        json={"topic": "test/topic", "payload": "test integration message"}
    )
    
    assert response.status_code == 200
    result = response.json()
    assert result["success"] is True
    
    # Verify MQTT message was published
    assert len(integration_fixture.mqtt_messages) == 1
    mqtt_msg = integration_fixture.mqtt_messages[0]
    assert mqtt_msg["topic"] == "test/topic"
    assert mqtt_msg["payload"] == "test integration message"
    
    # Verify WebSocket messages were sent to subscribers
    ws_messages_to_topic = [
        msg for msg in integration_fixture.websocket_messages
        if msg["message"].get("topic") == "test/topic"
    ]
    assert len(ws_messages_to_topic) == 2  # Two WebSocket subscribers
    
    # Verify each subscriber received the message
    subscriber_ids = set(msg["connection_id"] for msg in ws_messages_to_topic)
    assert subscriber_ids == {"ws-conn-1", "ws-conn-2"}

def test_mqtt_subscription_and_publishing(integration_fixture, mock_limiter):
    """Test MQTT subscription followed by publishing"""
    # Subscribe to a topic
    response = client.post(
        "/mqtt/subscribe",
        json={"topic": "new/test/topic"}
    )
    
    assert response.status_code == 200
    assert "new/test/topic" in integration_fixture.subscribed_topics
    
    # Publish to the subscribed topic
    response = client.post(
        "/mqtt/publish",
        json={"topic": "new/test/topic", "payload": "message to subscribed topic"}
    )
    
    assert response.status_code == 200
    
    # Verify the message was published
    mqtt_msgs_to_topic = [
        msg for msg in integration_fixture.mqtt_messages
        if msg["topic"] == "new/test/topic"
    ]
    assert len(mqtt_msgs_to_topic) == 1
    
    # Get subscribed topics
    response = client.get("/mqtt/topics")
    assert response.status_code == 200
    topics = response.json()["data"]
    assert "new/test/topic" in topics

def test_race_conditions(integration_fixture, mock_limiter):
    """Test handling of concurrent operations between MQTT and WebSocket"""
    # Simulate concurrent operations
    NUM_THREADS = 5
    
    def publish_message(idx):
        """Publish a message in a separate thread"""
        response = client.post(
            "/mqtt/publish",
            json={"topic": f"test/concurrent/{idx}", "payload": f"concurrent message {idx}"}
        )
        assert response.status_code == 200
    
    # Start multiple threads to publish messages concurrently
    threads = []
    for i in range(NUM_THREADS):
        thread = threading.Thread(target=publish_message, args=(i,))
        threads.append(thread)
        thread.start()
    
    # Wait for all threads to complete
    for thread in threads:
        thread.join()
    
    # Verify all messages were published
    assert len(integration_fixture.mqtt_messages) == NUM_THREADS
    
    # Verify message IDs are unique
    message_ids = [msg["id"] for msg in integration_fixture.mqtt_messages]
    assert len(message_ids) == len(set(message_ids))

def test_error_handling(integration_fixture, mock_limiter):
    """Test error handling in integrated flows"""
    # Store the original side effect
    original_side_effect = integration_fixture.mqtt_handler.publish.side_effect
    
    try:
        # Make MQTT publish fail by directly raising an exception with a custom side effect
        def raise_exception(*args, **kwargs):
            raise Exception("Simulated MQTT failure")
        
        # Replace the side effect - this is the key change
        integration_fixture.mqtt_handler.publish.side_effect = raise_exception
        
        # Attempt to publish
        response = client.post(
            "/mqtt/publish",
            json={"topic": "error/test", "payload": "error message"}
        )
        
        # Verify proper error response
        assert response.status_code == 500
        assert "Simulated MQTT failure" in response.json()["detail"]
    finally:
        # Restore the original side effect
        integration_fixture.mqtt_handler.publish.side_effect = original_side_effect 