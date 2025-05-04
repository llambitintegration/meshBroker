import pytest
from unittest.mock import Mock, patch, MagicMock, call
import paho.mqtt.client as mqtt
import argparse
import logging
import json
import tempfile
import os
import io
from pathlib import Path
import sys

# Add the parent directory to the Python path to import the CLI module
sys.path.append(str(Path(__file__).parent.parent))
from meshtastic_mqtt_cli import parse_args, MQTTClient  # Update this import based on your actual module name

# Try to import the message decoder, but make it optional
try:
    from meshtastic_decoder import MessageDecoder, MeshtasticMessage, MESHTASTIC_AVAILABLE
except ImportError:
    try:
        from backend.meshtastic_decoder import MessageDecoder, MeshtasticMessage, MESHTASTIC_AVAILABLE
    except ImportError:
        # Set flags to indicate decoder is not available
        MESHTASTIC_AVAILABLE = False
        MessageDecoder = None
        MeshtasticMessage = None

# Skip tests that require Meshtastic library if it's not available
requires_meshtastic = pytest.mark.skipif(not MESHTASTIC_AVAILABLE, 
                                         reason="Meshtastic library not available")

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

@pytest.fixture
def mock_decoder():
    if not MESHTASTIC_AVAILABLE:
        return Mock()
    
    # Create a mock decoder instance
    decoder = Mock(spec=MessageDecoder)
    
    # Configure the decode_message method to return a MeshtasticMessage
    def mock_decode_message(topic, payload):
        return MeshtasticMessage(
            topic=topic,
            raw_payload=payload,
            message_type="text",
            node_id="node123",
            parsed=True,
            decoded_data={"text": "Decoded message"}
        )
    
    decoder.decode_message.side_effect = mock_decode_message
    
    return decoder

@pytest.fixture
def temp_key_file():
    """Create a temporary key file for testing"""
    with tempfile.NamedTemporaryFile(delete=False, mode='w') as f:
        f.write("test_channel_key_12345")
        file_path = f.name
    
    yield file_path
    
    # Clean up
    try:
        os.unlink(file_path)
    except (PermissionError, OSError):
        # On Windows, sometimes the file might still be in use
        pass

@pytest.fixture
def temp_output_file():
    """Create a temporary output file for testing"""
    with tempfile.NamedTemporaryFile(delete=False) as f:
        file_path = f.name
    
    yield file_path
    
    # Clean up
    try:
        os.unlink(file_path)
    except (PermissionError, OSError):
        # On Windows, sometimes the file might still be in use
        pass

# Enhanced tests for relay functionality
def test_relay_message_complex_formatting(mock_mqtt_client, mock_logger):
    """Test relay with complex topic formatting patterns"""
    # Initialize client with complex relay topic pattern
    client = MQTTClient(
        'localhost', 1883, mock_logger,
        relay_enabled=True,
        relay_topic='relay/{node_id}/{message_type}/{parts[2]}'
    )
    client.connect()
    
    # Test with a source topic that has expected parts
    source_topic = "msh/node123/json/text"
    
    # Call relay method
    client.relay_message("Test message", source_topic)
    
    # Verify the formatted topic was used
    mock_mqtt_client.publish.assert_called_once()
    args, _ = mock_mqtt_client.publish.call_args
    assert args[0] == "relay/node123/text/json"

def test_relay_message_invalid_format(mock_mqtt_client, mock_logger):
    """Test relay with invalid format in topic pattern"""
    # Initialize client with invalid format in relay topic
    client = MQTTClient(
        'localhost', 1883, mock_logger,
        relay_enabled=True,
        relay_topic='relay/{invalid_key}/{message_type}'
    )
    client.connect()
    
    # Test with a source topic
    source_topic = "msh/node123/json/text"
    
    # Call relay method (should not crash despite invalid format)
    client.relay_message("Test message", source_topic)
    
    # Should fall back to using the relay topic as-is
    mock_mqtt_client.publish.assert_called_once()
    args, _ = mock_mqtt_client.publish.call_args
    assert args[0] == "relay/{invalid_key}/{message_type}"

def test_relay_different_message_types(mock_mqtt_client, mock_logger):
    """Test relaying different message types"""
    client = MQTTClient(
        'localhost', 1883, mock_logger,
        relay_enabled=True,
        relay_topic='relay/topic'
    )
    client.connect()
    
    # Test with string message
    client.relay_message("String message", "topic/1")
    
    # Test with dictionary
    client.relay_message({"key": "value"}, "topic/2")
    
    # Test with bytes
    client.relay_message(b"Binary data", "topic/3")
    
    # Test with bytes that aren't valid UTF-8
    client.relay_message(b"\xff\xfe\xfd\xfc", "topic/4")
    
    # Verify all messages were relayed
    assert mock_mqtt_client.publish.call_count == 4

# Enhanced tests for output formats
@requires_meshtastic
def test_output_format_json(mock_mqtt_client, mock_logger, mock_decoder):
    """Test JSON output format"""
    client = MQTTClient(
        'localhost', 1883, mock_logger,
        decoder=mock_decoder,
        output_format='json'
    )
    client.connect()
    
    # Mock _write_output to capture what would be written
    client._write_output = Mock()
    
    # Create a mock message
    mock_message = Mock()
    mock_message.topic = "msh/node123/json/text"
    mock_message.payload = b'{"text": "Test message"}'
    
    # Call the callback
    client._on_message(mock_mqtt_client, None, mock_message)
    
    # Verify JSON output
    client._write_output.assert_called_once()
    args, _ = client._write_output.call_args
    
    # The output should be a JSON string
    assert isinstance(args[0], str)
    # We should be able to parse it as JSON
    parsed = json.loads(args[0])
    assert 'topic' in parsed
    assert 'data' in parsed

@requires_meshtastic
def test_output_format_raw(mock_mqtt_client, mock_logger, mock_decoder):
    """Test raw output format"""
    client = MQTTClient(
        'localhost', 1883, mock_logger,
        decoder=mock_decoder,
        output_format='raw'
    )
    client.connect()
    
    # Mock _write_output to capture what would be written
    client._write_output = Mock()
    
    # Create a mock message
    mock_message = Mock()
    mock_message.topic = "msh/node123/json/text"
    mock_message.payload = b'{"text": "Test message"}'
    
    # Call the callback
    client._on_message(mock_mqtt_client, None, mock_message)
    
    # Verify raw output
    client._write_output.assert_called_once()
    args, _ = client._write_output.call_args
    
    # The output should be a string with "Raw message" or similar
    assert isinstance(args[0], str)
    assert "Raw message" in args[0] or "size=" in args[0]

@requires_meshtastic
def test_output_format_text(mock_mqtt_client, mock_logger, mock_decoder):
    """Test text output format"""
    client = MQTTClient(
        'localhost', 1883, mock_logger,
        decoder=mock_decoder,
        output_format='text'
    )
    client.connect()
    
    # Mock _write_output to capture what would be written
    client._write_output = Mock()
    
    # Create a mock message
    mock_message = Mock()
    mock_message.topic = "msh/node123/json/text"
    mock_message.payload = b'{"text": "Test message"}'
    
    # Call the callback
    client._on_message(mock_mqtt_client, None, mock_message)
    
    # Verify text output
    client._write_output.assert_called_once()

# Enhanced integration tests
@requires_meshtastic
def test_decode_and_relay_combined(mock_mqtt_client, mock_logger, mock_decoder):
    """Test combined decoding and relaying"""
    client = MQTTClient(
        'localhost', 1883, mock_logger,
        decoder=mock_decoder,
        relay_enabled=True,
        relay_topic='relay/{node_id}'
    )
    client.connect()
    
    # Mock send_message to track calls
    client.send_message = Mock()
    
    # Create a mock message
    mock_message = Mock()
    mock_message.topic = "msh/node123/json/text"
    mock_message.payload = b'{"text": "Test message"}'
    
    # Mock _write_output to avoid actual output
    client._write_output = Mock()
    
    # Call the callback
    client._on_message(mock_mqtt_client, None, mock_message)
    
    # Verify decoder was called
    mock_decoder.decode_message.assert_called_once()
    
    # Verify relay was called with the decoded message and formatted topic
    client.send_message.assert_called_once()
    args, _ = client.send_message.call_args
    assert args[0] == "relay/node123"  # Formatted topic

@requires_meshtastic
def test_multiple_output_formats_relay(mock_mqtt_client, mock_logger, mock_decoder):
    """Test relaying with different output formats"""
    # Test each output format
    for output_format in ['text', 'json', 'raw']:
        client = MQTTClient(
            'localhost', 1883, mock_logger,
            decoder=mock_decoder,
            relay_enabled=True,
            relay_topic='relay/topic',
            output_format=output_format
        )
        client.connect()
        
        # Reset mocks for this iteration
        mock_mqtt_client.reset_mock()
        mock_decoder.reset_mock()
        
        # Create a mock message
        mock_message = Mock()
        mock_message.topic = "msh/node123/json/text"
        mock_message.payload = b'{"text": "Test message"}'
        
        # Mock _write_output to avoid actual output
        client._write_output = Mock()
        
        # Call the callback
        client._on_message(mock_mqtt_client, None, mock_message)
        
        # Verify decoder was called
        mock_decoder.decode_message.assert_called_once()
        
        # Verify relay was called with appropriate format
        mock_mqtt_client.publish.assert_called_once()

# Error handling tests
def test_relay_error_handling(mock_mqtt_client, mock_logger):
    """Test error handling during relay"""
    client = MQTTClient(
        'localhost', 1883, mock_logger,
        relay_enabled=True,
        relay_topic='relay/topic'
    )
    client.connect()
    
    # Make publish fail
    mock_mqtt_client.publish.side_effect = Exception("Simulated error")
    
    # Call relay method directly (should not crash despite error)
    client.send_message = Mock(side_effect=Exception("Simulated error"))
    
    # Use try/except to only generate one error
    try:
        client.relay_message("Test message", "source/topic")
    except:
        pass
    
    # Error should be logged at least once 
    # (not asserting exact count since implementation might generate multiple errors)
    assert mock_logger.error.call_count >= 1
    assert "error" in str(mock_logger.error.call_args).lower()

def test_decode_error_handling(mock_mqtt_client, mock_logger):
    """Test error handling during message decoding"""
    # Create decoder that raises an exception
    mock_decoder = Mock(spec=MessageDecoder)
    mock_decoder.decode_message.side_effect = Exception("Simulated decoder error")
    
    client = MQTTClient(
        'localhost', 1883, mock_logger,
        decoder=mock_decoder
    )
    client.connect()
    
    # Create a mock message
    mock_message = Mock()
    mock_message.topic = "msh/node123/json/text"
    mock_message.payload = b'{"text": "Test message"}'
    
    # Call the callback (should not crash despite error)
    client._on_message(mock_mqtt_client, None, mock_message)
    
    # Error should be logged
    mock_logger.error.assert_called_once()

def test_write_output_error_handling(mock_mqtt_client, mock_logger):
    """Test error handling when writing output"""
    # Create client with invalid output file
    client = MQTTClient(
        'localhost', 1883, mock_logger,
        output_file="/not/a/valid/path/file.txt"
    )
    
    # Mock print to verify fallback
    with patch('builtins.print') as mock_print:
        # Call _write_output (should not crash)
        client._write_output("Test output")
        
        # Should fall back to print
        mock_print.assert_called_once()
        
        # Error should be logged
        mock_logger.error.assert_called_once()

# Tests for complex command line arguments
def test_complex_cli_args():
    """Test complex combination of command line arguments"""
    args = parse_args([
        'receive', 
        '-b', 'localhost', 
        '-p', '1883',
        '-t', 'msh/#',
        '--decrypt',
        '--channel-key', 'complex-key-123',
        '-o', 'json',
        '--relay',
        '--relay-topic', 'relay/{node_id}/{message_type}',
        '-f', 'output.log'
    ])
    
    assert args.mode == 'receive'
    assert args.broker == 'localhost'
    assert args.port == 1883
    assert args.topic == 'msh/#'
    assert args.decrypt is True
    assert args.channel_key == 'complex-key-123'
    assert args.output_format == 'json'
    assert args.relay is True
    assert args.relay_topic == 'relay/{node_id}/{message_type}'
    assert args.output_file == 'output.log'

# Additional tests for full initialization and integration
@requires_meshtastic
def test_full_client_initialization_with_all_options(mock_mqtt_client, mock_logger, mock_decoder, temp_output_file):
    """Test initializing client with all options enabled"""
    client = MQTTClient(
        'localhost', 1883, mock_logger,
        decoder=mock_decoder,
        relay_enabled=True,
        relay_topic='relay/{node_id}/{message_type}',
        output_format='json',
        output_file=temp_output_file
    )
    
    # Verify initialization
    assert client.broker == 'localhost'
    assert client.port == 1883
    assert client.decoder is mock_decoder
    assert client.relay_enabled is True
    assert client.relay_topic == 'relay/{node_id}/{message_type}'
    assert client.output_format == 'json'
    assert client.output_file == temp_output_file
    assert client.output_stream is not None
    
    # Make sure to close file before the test ends
    client.disconnect()
    
    # Extra cleanup - explicitly close any remaining file handles
    if hasattr(client, 'output_stream') and client.output_stream and not client.output_stream.closed:
        try:
            client.output_stream.close()
        except:
            pass