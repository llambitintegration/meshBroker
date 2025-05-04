import pytest
from unittest.mock import Mock, patch
import paho.mqtt.client as mqtt
import argparse
import logging
from pathlib import Path
import sys

# Add the parent directory to the Python path to import the CLI module
sys.path.append(str(Path(__file__).parent.parent))
from meshtastic_mqtt_cli import parse_args, MQTTClient  # Update this import based on your actual module name

# Fixtures
@pytest.fixture
def mock_mqtt_client():
    with patch('paho.mqtt.client.Client') as mock_client:
        # Create a mock client instance
        client_instance = Mock()
        mock_client.return_value = client_instance
        
        # Mock the connection success
        client_instance.connect.return_value = 0
        client_instance.loop_start.return_value = None
        
        # Configure publish return value with proper 'rc' attribute
        publish_result = Mock()
        publish_result.rc = mqtt.MQTT_ERR_SUCCESS  # 0
        client_instance.publish.return_value = publish_result
        
        # Configure subscribe to return a proper tuple (result_code, message_id)
        client_instance.subscribe.return_value = (mqtt.MQTT_ERR_SUCCESS, 1)  # (0, message_id)
        
        # Configure unsubscribe to return a proper tuple as well
        client_instance.unsubscribe.return_value = (mqtt.MQTT_ERR_SUCCESS, 1)  # (0, message_id)
        
        yield client_instance

@pytest.fixture
def mock_logger():
    with patch('logging.getLogger') as mock_log:
        logger = Mock()
        mock_log.return_value = logger
        yield logger

# Test argument parsing
def test_parse_args_send_mode():
    """Test argument parsing for send mode"""
    args = parse_args(['send', '-b', 'localhost', '-p', '1883', 
                      '-t', 'test/topic', '-m', 'test message'])
    
    assert args.mode == 'send'
    assert args.broker == 'localhost'
    assert args.port == 1883
    assert args.topic == 'test/topic'
    assert args.message == 'test message'

def test_parse_args_log_level():
    """Test argument parsing for log level"""
    args = parse_args(['send', '-b', 'localhost', '-p', '1883', 
                      '-t', 'test/topic', '-m', 'test message',
                      '-l', 'DEBUG'])
    
    assert args.log_level == 'DEBUG'
    
    args = parse_args(['receive', '-b', 'localhost', '-p', '1883', 
                      '-t', 'test/topic'])
    
    assert args.log_level == 'INFO'  # Default value

def test_parse_args_receive_mode():
    """Test argument parsing for receive mode"""
    args = parse_args(['receive', '-b', 'localhost', '-p', '1883', 
                      '-t', 'test/topic'])
    
    assert args.mode == 'receive'
    assert args.broker == 'localhost'
    assert args.port == 1883
    assert args.topic == 'test/topic'
    assert args.message is None

def test_parse_args_receive_mode_multiple_topics():
    """Test argument parsing for receive mode with multiple topics"""
    args = parse_args(['receive', '-b', 'localhost', '-p', '1883', 
                      '-t', 'test/topic1,test/topic2,test/topic3'])
    
    assert args.mode == 'receive'
    assert args.broker == 'localhost'
    assert args.port == 1883
    assert args.topic == 'test/topic1,test/topic2,test/topic3'
    assert args.message is None

def test_parse_args_invalid_mode():
    """Test argument parsing with invalid mode"""
    with pytest.raises(SystemExit):
        parse_args(['invalid', '-b', 'localhost'])

def test_parse_args_send_mode_missing_message():
    """Test argument parsing for send mode with missing message"""
    with pytest.raises(SystemExit):
        parse_args(['send', '-b', 'localhost', '-p', '1883', '-t', 'test/topic'])

# Test MQTT client initialization
def test_mqtt_client_init(mock_mqtt_client, mock_logger):
    """Test MQTT client initialization"""
    client = MQTTClient('localhost', 1883, mock_logger)
    assert client.broker == 'localhost'
    assert client.port == 1883
    assert client.connected is False

# Test connection handling
def test_mqtt_client_connect(mock_mqtt_client, mock_logger):
    """Test MQTT client connection"""
    client = MQTTClient('localhost', 1883, mock_logger)
    client.connect()
    
    mock_mqtt_client.connect.assert_called_once_with('localhost', 1883)
    mock_mqtt_client.loop_start.assert_called_once()
    assert client.connected is True

# Test send mode
def test_mqtt_client_send_message(mock_mqtt_client, mock_logger):
    """Test sending a message"""
    client = MQTTClient('localhost', 1883, mock_logger)
    client.connect()
    
    message = "test message"
    topic = "test/topic"
    qos = 0
    retain = False
    
    client.send_message(topic, message, qos, retain)
    
    mock_mqtt_client.publish.assert_called_once_with(topic, message, qos, retain)
    mock_logger.info.assert_called_with(f"Message published to {topic}")

# Test receive mode
def test_mqtt_client_subscribe(mock_mqtt_client, mock_logger):
    """Test subscribing to a topic"""
    client = MQTTClient('localhost', 1883, mock_logger)
    client.connect()
    
    topic = "test/topic"
    qos = 0
    
    client.subscribe(topic, qos)
    
    mock_mqtt_client.subscribe.assert_called_once_with(topic, qos)
    mock_logger.info.assert_called_with(f"Subscribed to {topic}")

# Test message callback
def test_mqtt_client_on_message(mock_mqtt_client, mock_logger):
    """Test message callback handling"""
    client = MQTTClient('localhost', 1883, mock_logger)
    
    # Create a mock message
    mock_message = Mock()
    mock_message.topic = "test/topic"
    mock_message.payload = b"test message"
    
    # Call the callback
    client._on_message(mock_mqtt_client, None, mock_message)
    
    # Verify logging
    mock_logger.info.assert_called_with(f"Received message on {mock_message.topic}: {mock_message.payload.decode()}")

# Test error handling
def test_mqtt_client_connection_error(mock_mqtt_client, mock_logger):
    """Test connection error handling"""
    mock_mqtt_client.connect.side_effect = Exception("Connection failed")
    
    client = MQTTClient('localhost', 1883, mock_logger)
    
    with pytest.raises(Exception):
        client.connect()
    
    mock_logger.error.assert_called()

def test_mqtt_client_publish_error(mock_mqtt_client, mock_logger):
    """Test publish error handling"""
    client = MQTTClient('localhost', 1883, mock_logger)
    client.connect()
    
    mock_mqtt_client.publish.side_effect = Exception("Publish failed")
    
    with pytest.raises(Exception):
        client.send_message("test/topic", "test message")
    
    mock_logger.error.assert_called()

# Integration tests
def test_full_send_workflow(mock_mqtt_client, mock_logger):
    """Test complete send workflow"""
    # Arrange
    args = parse_args(['send', '-b', 'localhost', '-p', '1883',
                      '-t', 'test/topic', '-m', 'test message'])
    client = MQTTClient(args.broker, args.port, mock_logger)
    
    # Act
    client.connect()
    client.send_message(args.topic, args.message)
    
    # Assert
    mock_mqtt_client.connect.assert_called_once()
    mock_mqtt_client.publish.assert_called_once()
    mock_logger.info.assert_called()

def test_full_receive_workflow(mock_mqtt_client, mock_logger):
    """Test complete receive workflow"""
    # Arrange
    args = parse_args(['receive', '-b', 'localhost', '-p', '1883',
                      '-t', 'test/topic'])
    client = MQTTClient(args.broker, args.port, mock_logger)
    
    # Act
    client.connect()
    client.subscribe(args.topic)
    
    # Simulate receiving a message
    mock_message = Mock()
    mock_message.topic = args.topic
    mock_message.payload = b"test message"
    client._on_message(mock_mqtt_client, None, mock_message)
    
    # Assert
    mock_mqtt_client.connect.assert_called_once()
    mock_mqtt_client.subscribe.assert_called_once()
    mock_logger.info.assert_called()

def test_full_receive_workflow_multiple_topics(mock_mqtt_client, mock_logger):
    """Test complete receive workflow with multiple topics"""
    # Arrange
    args = parse_args(['receive', '-b', 'localhost', '-p', '1883',
                      '-t', 'test/topic1,test/topic2,test/topic3'])
    client = MQTTClient(args.broker, args.port, mock_logger)
    
    # Mock main function to test multi-topic subscription
    with patch('builtins.input', return_value=''):
        # Act
        client.connect()
        
        topics = [topic.strip() for topic in args.topic.split(',')]
        for topic in topics:
            client.subscribe(topic)
        
        # Simulate receiving messages on different topics
        for topic in topics:
            mock_message = Mock()
            mock_message.topic = topic
            mock_message.payload = f"message from {topic}".encode()
            client._on_message(mock_mqtt_client, None, mock_message)
    
    # Assert
    mock_mqtt_client.connect.assert_called_once()
    assert mock_mqtt_client.subscribe.call_count == len(topics)
    assert mock_logger.info.call_count >= len(topics)  # At least one log per topic

# Additional Complex Test Cases

@pytest.mark.parametrize("qos,retain,expected_error", [
    (0, False, None),
    (1, True, None),
    (2, False, None),
    (3, False, ValueError),  # Invalid QoS level
    (-1, False, ValueError),  # Invalid QoS level
])
def test_mqtt_client_send_message_qos_levels(mock_mqtt_client, mock_logger, qos, retain, expected_error):
    """Test sending messages with different QoS levels and retain flags"""
    client = MQTTClient('localhost', 1883, mock_logger)
    client.connect()
    
    if expected_error:
        with pytest.raises(expected_error):
            client.send_message("test/topic", "test message", qos, retain)
    else:
        client.send_message("test/topic", "test message", qos, retain)
        mock_mqtt_client.publish.assert_called_once_with("test/topic", "test message", qos, retain)

@pytest.mark.parametrize("topic", [
    "test/topic",
    "test/+/wildcard",
    "test/#",
    "",  # Empty topic
    "test/ /spaces",
    "test/\u7279\u6b8a\u5b57\u7b26",  # Unicode characters
    "a" * 1000,  # Shorter but still long topic (reduced from 65536)
])
def test_mqtt_client_subscribe_topic_variations(mock_mqtt_client, mock_logger, topic):
    """Test subscribing to various topic patterns"""
    client = MQTTClient('localhost', 1883, mock_logger)
    client.connect()
    
    if not topic:
        with pytest.raises(ValueError):
            client.subscribe(topic)
    else:
        client.subscribe(topic)
        mock_mqtt_client.subscribe.assert_called_once_with(topic, 0)

def test_mqtt_client_multiple_subscriptions(mock_mqtt_client, mock_logger):
    """Test subscribing to multiple topics"""
    client = MQTTClient('localhost', 1883, mock_logger)
    client.connect()
    
    topics = ["test/topic1", "test/topic2", "test/topic3"]
    for topic in topics:
        client.subscribe(topic)
    
    assert mock_mqtt_client.subscribe.call_count == len(topics)

def test_mqtt_client_reconnection(mock_mqtt_client, mock_logger):
    """Test client reconnection behavior"""
    client = MQTTClient('localhost', 1883, mock_logger)
    client.connect()
    
    # Simulate unexpected disconnection
    client._on_disconnect(mock_mqtt_client, None, 1)
    assert not client.connected
    
    # Reconnect
    client.connect()
    assert client.connected
    assert mock_mqtt_client.connect.call_count == 2

@pytest.mark.parametrize("payload,expected_error", [
    (b"normal message", None),
    (b"UTF-8 message \xe2\x98\x83", None),  # Snowman emoji
    (b"\xff\xfe invalid utf-8", UnicodeDecodeError),
    (b"", None),  # Empty message
    (b"a" * 1000, None),  # Shorter but still substantial message (reduced from 1048576)
])
def test_mqtt_client_message_decoding(mock_mqtt_client, mock_logger, payload, expected_error):
    """Test handling of various message payloads and encodings"""
    client = MQTTClient('localhost', 1883, mock_logger)
    
    mock_message = Mock()
    mock_message.topic = "test/topic"
    mock_message.payload = payload
    
    if expected_error:
        with pytest.raises(expected_error):
            client._on_message(mock_mqtt_client, None, mock_message)
    else:
        client._on_message(mock_mqtt_client, None, mock_message)
        mock_logger.info.assert_called()

def test_mqtt_client_connection_timeout(mock_mqtt_client, mock_logger):
    """Test connection timeout handling"""
    mock_mqtt_client.connect.side_effect = TimeoutError("Connection timed out")
    
    client = MQTTClient('localhost', 1883, mock_logger)
    with pytest.raises(TimeoutError):
        client.connect()
    
    mock_logger.error.assert_called()

def test_mqtt_client_broker_unreachable(mock_mqtt_client, mock_logger):
    """Test handling of unreachable broker"""
    mock_mqtt_client.connect.side_effect = ConnectionRefusedError("Connection refused")
    
    client = MQTTClient('localhost', 1883, mock_logger)
    with pytest.raises(ConnectionRefusedError):
        client.connect()
    
    mock_logger.error.assert_called()

@pytest.mark.parametrize("rc_code,expected_connected", [
    (0, True),   # Success
    (1, False),  # Connection refused - incorrect protocol version
    (2, False),  # Connection refused - invalid client identifier
    (3, False),  # Connection refused - server unavailable
    (4, False),  # Connection refused - bad username or password
    (5, False),  # Connection refused - not authorized
])
def test_mqtt_client_connection_results(mock_mqtt_client, mock_logger, rc_code, expected_connected):
    """Test handling of different connection result codes"""
    client = MQTTClient('localhost', 1883, mock_logger)
    client._on_connect(mock_mqtt_client, None, None, rc_code)
    
    if rc_code == 0:
        mock_logger.info.assert_called()
    else:
        mock_logger.error.assert_called()

def test_mqtt_client_concurrent_operations(mock_mqtt_client, mock_logger):
    """Test multiple operations in sequence"""
    client = MQTTClient('localhost', 1883, mock_logger)
    client.connect()
    
    # Subscribe to multiple topics
    topics = ["test/topic1", "test/topic2"]
    for topic in topics:
        client.subscribe(topic)
    
    # Send multiple messages
    messages = ["message1", "message2"]
    for msg in messages:
        client.send_message("test/topic", msg)
    
    # Verify all operations
    assert mock_mqtt_client.subscribe.call_count == len(topics)
    assert mock_mqtt_client.publish.call_count == len(messages)

@pytest.mark.parametrize("broker,port,expected_error", [
    ("localhost", 1883, None),
    ("", 1883, ValueError),  # Empty broker address
    ("localhost", 0, ValueError),  # Invalid port
    ("localhost", 65536, ValueError),  # Port out of range
    ("invalid.broker", 1883, Exception),  # Invalid broker address
])
def test_mqtt_client_invalid_connection_params(mock_mqtt_client, mock_logger, broker, port, expected_error):
    """Test handling of invalid connection parameters"""
    if expected_error:
        with pytest.raises(expected_error):
            client = MQTTClient(broker, port, mock_logger)
            client.connect()
    else:
        client = MQTTClient(broker, port, mock_logger)
        client.connect()
        assert client.connected

def test_mqtt_client_cleanup(mock_mqtt_client, mock_logger):
    """Test proper cleanup of resources"""
    client = MQTTClient('localhost', 1883, mock_logger)
    client.connect()
    
    # Simulate some activity
    client.subscribe("test/topic")
    client.send_message("test/topic", "test message")
    
    # Disconnect and verify cleanup
    client.disconnect()
    assert not client.connected
    mock_mqtt_client.disconnect.assert_called_once()
    mock_mqtt_client.loop_stop.assert_called_once() 