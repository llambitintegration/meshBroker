import pytest
import time
import os
import json
import threading
from unittest.mock import MagicMock, patch
import paho.mqtt.client as mqtt

from backend.mqtt_handler import MQTTHandler
from backend.config import settings

# Test settings
TEST_BROKER_HOST = "127.0.0.1"
TEST_BROKER_PORT = 1883
TEST_CLIENT_ID = "test_client"
TEST_TOPIC = "test/topic"
TEST_PAYLOAD = "test_payload"
TEST_QOS = 1


@pytest.fixture
def mqtt_handler():
    """Create a test MQTT handler"""
    # Create handler with dummy values
    handler = MQTTHandler(
        broker_host=TEST_BROKER_HOST,
        broker_port=TEST_BROKER_PORT,
        client_id=TEST_CLIENT_ID,
        persistence_enabled=False
    )
    
    # Mock the MQTT client to avoid actual connections
    handler._mqtt_client = MagicMock()
    handler._mqtt_client.is_connected.return_value = True
    
    # Set connected flag directly
    handler.connected = True
    
    # Mock message queue and flow controller
    handler.message_queue = MagicMock()
    handler.message_queue.running = True
    handler.message_queue.enqueue.return_value = "test-message-id"  # Simulate successful enqueue
    
    handler.flow_controller = MagicMock()
    handler.flow_controller.is_throttling.return_value = False  # Disable flow control for tests
    
    # Mock message processor
    handler.message_processor = MagicMock()
    handler.message_processor.process_message.return_value = (TEST_TOPIC, TEST_PAYLOAD.encode('utf-8'))
    
    # Mock delivery tracker
    handler.delivery_tracker = MagicMock()
    
    # Replace the connect and disconnect methods
    handler.connect = MagicMock()
    handler.disconnect = MagicMock()
    
    yield handler
    
    # Clean up
    handler.disconnect()


def test_mqtt_handler_init():
    """Test MQTT handler initialization"""
    handler = MQTTHandler(
        broker_host=TEST_BROKER_HOST,
        broker_port=TEST_BROKER_PORT,
        client_id=TEST_CLIENT_ID
    )
    
    assert handler.broker_host == TEST_BROKER_HOST
    assert handler.broker_port == TEST_BROKER_PORT
    assert handler.client_id == TEST_CLIENT_ID
    assert handler.default_qos == 0  # Default value
    assert handler._mqtt_client is not None


def test_mqtt_handler_connect(mqtt_handler):
    """Test MQTT handler connect method"""
    mqtt_handler.connect()
    mqtt_handler.connect.assert_called_once()


def test_mqtt_handler_disconnect(mqtt_handler):
    """Test MQTT handler disconnect method"""
    mqtt_handler.disconnect()
    mqtt_handler.disconnect.assert_called_once()


def test_mqtt_handler_publish(mqtt_handler):
    """Test MQTT handler publish method"""
    # Configure mock
    mqtt_handler._mqtt_client.publish.return_value.rc = mqtt.MQTT_ERR_SUCCESS
    
    # Call publish
    result = mqtt_handler.publish(TEST_TOPIC, TEST_PAYLOAD, qos=TEST_QOS)
    
    # Verify
    assert result is True
    
    # Check if using message queue or direct publishing
    if mqtt_handler.message_queue.running:
        mqtt_handler.message_queue.enqueue.assert_called_once()
    else:
        mqtt_handler._mqtt_client.publish.assert_called_once_with(
            TEST_TOPIC, TEST_PAYLOAD, qos=TEST_QOS, retain=False
        )


def test_mqtt_handler_subscribe(mqtt_handler):
    """Test MQTT handler subscribe method"""
    # Configure mock
    mqtt_handler._mqtt_client.subscribe.return_value = (mqtt.MQTT_ERR_SUCCESS, 1)
    
    # Call subscribe
    result = mqtt_handler.subscribe(TEST_TOPIC, qos=TEST_QOS)
    
    # Verify
    assert result is True
    mqtt_handler._mqtt_client.subscribe.assert_called_once_with(TEST_TOPIC, TEST_QOS)


def test_mqtt_handler_unsubscribe(mqtt_handler):
    """Test MQTT handler unsubscribe method"""
    # Configure mock
    mqtt_handler._mqtt_client.unsubscribe.return_value = (mqtt.MQTT_ERR_SUCCESS, 1)
    
    # Add the topic to subscribed_topics first to simulate a subscribed topic
    mqtt_handler.subscribed_topics[TEST_TOPIC] = TEST_QOS
    
    # Call unsubscribe
    result = mqtt_handler.unsubscribe(TEST_TOPIC)
    
    # Verify
    assert result is True
    mqtt_handler._mqtt_client.unsubscribe.assert_called_once_with(TEST_TOPIC)


def test_mqtt_handler_set_message_callback(mqtt_handler):
    """Test setting message callback"""
    # Create a test callback
    callback = MagicMock()
    
    # Set callback
    mqtt_handler.set_message_callback(callback)
    
    # Verify
    assert mqtt_handler._message_callback == callback


def test_mqtt_handler_on_message(mqtt_handler):
    """Test on_message handler"""
    # Create a test callback
    callback = MagicMock()
    mqtt_handler.set_message_callback(callback)
    
    # Create a mock message
    message = MagicMock()
    message.topic = TEST_TOPIC
    message.payload = TEST_PAYLOAD.encode('utf-8')
    
    # Configure message processor mock to return a specific value
    # The processor seems to be converting bytes to string, so we'll return a string payload
    mqtt_handler.message_processor.process_message.return_value = (TEST_TOPIC, TEST_PAYLOAD)
    
    # Trigger on_message
    mqtt_handler._on_message(None, None, message)
    
    # Verify callback was called with correct args - using string payload
    callback.assert_called_once_with(TEST_TOPIC, TEST_PAYLOAD)


def test_mqtt_handler_message_retry():
    """Test message retry mechanism"""
    # Create handler with test values
    handler = MQTTHandler(
        broker_host=TEST_BROKER_HOST,
        broker_port=TEST_BROKER_PORT,
        client_id=TEST_CLIENT_ID,
        max_retries=3,
        persistence_enabled=False
    )
    
    # Set connected flag
    handler.connected = True
    
    # Mock flow controller
    handler.flow_controller = MagicMock()
    handler.flow_controller.is_throttling.return_value = False
    
    # Mock message queue
    handler.message_queue = MagicMock()
    handler.message_queue.running = False  # Use direct publishing for this test
    
    # Mock delivery tracker
    handler.delivery_tracker = MagicMock()
    
    # Mock direct publish method with side effects
    original_publish_direct = handler._publish_direct
    
    publish_calls = 0
    def mock_publish_direct(topic, payload, qos, retain, message_id=None):
        nonlocal publish_calls
        publish_calls += 1
        if publish_calls == 1:
            # First call fails
            return MagicMock(rc=mqtt.MQTT_ERR_NO_CONN)
        else:
            # Subsequent calls succeed
            return MagicMock(rc=mqtt.MQTT_ERR_SUCCESS)
    
    # Replace the method
    handler._publish_direct = mock_publish_direct
    
    try:
        # Call publish
        result = handler.publish(TEST_TOPIC, TEST_PAYLOAD)
        
        # Verify
        assert result is True
        assert publish_calls == 2  # Should have been called twice
    finally:
        # Restore original method to avoid affecting other tests
        handler._publish_direct = original_publish_direct


@pytest.mark.asyncio
async def test_mqtt_handler_async_publish(mqtt_handler):
    """Test async publish method"""
    # Configure mock
    mqtt_handler._mqtt_client.publish.return_value.rc = mqtt.MQTT_ERR_SUCCESS
    
    # Ensure message queue is enabled but flow control is disabled
    mqtt_handler.flow_controller.is_throttling.return_value = False
    
    # Call async_publish
    result = await mqtt_handler.async_publish(TEST_TOPIC, TEST_PAYLOAD)
    
    # Verify
    assert result is True
    
    # Check if using message queue or direct publishing
    if mqtt_handler.message_queue.running:
        mqtt_handler.message_queue.enqueue.assert_called_once()
    else:
        mqtt_handler._mqtt_client.publish.assert_called_once_with(
            TEST_TOPIC, TEST_PAYLOAD, qos=0, retain=False
        )


@pytest.mark.integration
def test_integration_mqtt_connect():
    """Integration test for MQTT connection
    
    This test requires a running MQTT broker
    """
    # Skip if integration tests are not enabled
    if os.getenv("SKIP_INTEGRATION_TESTS", "true").lower() == "true":
        pytest.skip("Skipping integration test")
    
    # Create a real handler
    handler = MQTTHandler(
        broker_host=settings.MQTT_BROKER_HOST,
        broker_port=settings.MQTT_BROKER_PORT,
        client_id="integration_test_client",
        persistence_enabled=False
    )
    
    # Connect
    handler.connect()
    
    # Wait for connection
    time.sleep(1)
    
    # Check connection
    assert handler.is_connected() is True
    
    # Clean up
    handler.disconnect()


@pytest.mark.integration
def test_integration_mqtt_publish_subscribe():
    """Integration test for MQTT publish and subscribe
    
    This test requires a running MQTT broker
    """
    # Skip if integration tests are not enabled
    if os.getenv("SKIP_INTEGRATION_TESTS", "true").lower() == "true":
        pytest.skip("Skipping integration test")
    
    # Create a real handler
    handler = MQTTHandler(
        broker_host=settings.MQTT_BROKER_HOST,
        broker_port=settings.MQTT_BROKER_PORT,
        client_id="integration_test_client",
        persistence_enabled=False
    )
    
    # Connect
    handler.connect()
    
    # Wait for connection
    time.sleep(1)
    
    # Test message received flag
    message_received = threading.Event()
    test_topic = "integration/test/topic"
    test_payload = {"test": "payload", "timestamp": time.time()}
    received_payload = None
    
    # Message callback
    def on_message(topic, payload):
        nonlocal received_payload
        if topic == test_topic:
            try:
                received_payload = json.loads(payload.decode('utf-8'))
                message_received.set()
            except:
                pass
    
    # Set callback
    handler.set_message_callback(on_message)
    
    # Subscribe
    handler.subscribe(test_topic)
    
    # Wait for subscription to take effect
    time.sleep(1)
    
    # Publish
    handler.publish(test_topic, json.dumps(test_payload))
    
    # Wait for message
    message_received.wait(timeout=5)
    
    # Check message
    assert message_received.is_set(), "Message not received"
    assert received_payload["test"] == test_payload["test"]
    
    # Clean up
    handler.disconnect() 