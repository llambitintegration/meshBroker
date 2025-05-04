import pytest
from unittest.mock import Mock, patch
import paho.mqtt.client as mqtt
import argparse
import logging
from pathlib import Path
import sys
import tempfile
import os
import time

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
    os.unlink(file_path)

@pytest.fixture
def temp_output_file():
    """Create a temporary output file for testing"""
    # Create a temporary file but close it right away so the handle isn't kept open
    with tempfile.NamedTemporaryFile(delete=False) as f:
        file_path = f.name
        f.close()  # Explicitly close the file handle
    
    yield file_path
    
    # Clean up - make sure any remaining handles are closed before deletion
    try:
        if os.path.exists(file_path):
            os.close(os.open(file_path, os.O_RDONLY))  # Try to close any remaining handles
    except:
        pass

    # Try to delete the file with retries
    for i in range(3):
        try:
            if os.path.exists(file_path):
                os.unlink(file_path)
            break
        except PermissionError:
            # Wait briefly and try again
            time.sleep(0.5)
            # If this is the last attempt, don't suppress the error
            if i == 2:
                # Just log the error instead of raising an exception
                print(f"Warning: Could not delete temporary file {file_path}")

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

# Phase 3 arguments tests
def test_parse_args_decrypt_options():
    """Test parsing decrypt-related arguments"""
    args = parse_args(['receive', '-t', 'msh/#', '--decrypt', '--channel-key', 'test-key'])
    
    assert args.decrypt is True
    assert args.channel_key == 'test-key'
    assert args.keyfile is None

def test_parse_args_keyfile_option():
    """Test parsing keyfile option"""
    args = parse_args(['receive', '-t', 'msh/#', '--decrypt', '--keyfile', 'psk.key'])
    
    assert args.decrypt is True
    assert args.keyfile == 'psk.key'
    assert args.channel_key is None

def test_parse_args_decrypt_without_key():
    """Test parsing decrypt without key option (should fail)"""
    with pytest.raises(SystemExit):
        parse_args(['receive', '-t', 'msh/#', '--decrypt'])

def test_parse_args_output_format():
    """Test parsing output format option"""
    args = parse_args(['receive', '-t', 'msh/#', '-o', 'json'])
    
    assert args.output_format == 'json'
    
    args = parse_args(['receive', '-t', 'msh/#', '-o', 'raw'])
    
    assert args.output_format == 'raw'
    
    args = parse_args(['receive', '-t', 'msh/#'])
    
    assert args.output_format == 'text'  # Default value

def test_parse_args_output_file():
    """Test parsing output file option"""
    args = parse_args(['receive', '-t', 'msh/#', '-f', 'output.log'])
    
    assert args.output_file == 'output.log'

def test_parse_args_relay_options():
    """Test parsing relay options"""
    args = parse_args(['receive', '-t', 'msh/#', '--relay', '--relay-topic', 'relay/{node_id}'])
    
    assert args.relay is True
    assert args.relay_topic == 'relay/{node_id}'

def test_parse_args_relay_without_topic():
    """Test parsing relay without topic (should fail)"""
    with pytest.raises(SystemExit):
        parse_args(['receive', '-t', 'msh/#', '--relay'])

# Test MQTT client initialization
def test_mqtt_client_init(mock_mqtt_client, mock_logger):
    """Test MQTT client initialization"""
    client = MQTTClient('localhost', 1883, mock_logger)
    assert client.broker == 'localhost'
    assert client.port == 1883
    assert client.connected is False

def test_mqtt_client_init_with_decoder(mock_mqtt_client, mock_logger, mock_decoder):
    """Test MQTT client initialization with decoder"""
    client = MQTTClient('localhost', 1883, mock_logger, decoder=mock_decoder)
    
    assert client.broker == 'localhost'
    assert client.port == 1883
    assert client.decoder is mock_decoder

def test_mqtt_client_init_with_relay(mock_mqtt_client, mock_logger):
    """Test MQTT client initialization with relay options"""
    client = MQTTClient(
        'localhost', 1883, mock_logger,
        relay_enabled=True,
        relay_topic='relay/topic'
    )
    
    assert client.relay_enabled is True
    assert client.relay_topic == 'relay/topic'

def test_mqtt_client_init_with_output(mock_mqtt_client, mock_logger, temp_output_file):
    """Test MQTT client initialization with output options"""
    client = MQTTClient(
        'localhost', 1883, mock_logger,
        output_format='json',
        output_file=temp_output_file
    )
    
    assert client.output_format == 'json'
    assert client.output_file == temp_output_file
    assert client.output_stream is not None
    
    client.disconnect()  # Close the file

# Test message decoding
@requires_meshtastic
def test_mqtt_client_decode_meshtastic_message(mock_mqtt_client, mock_logger, mock_decoder):
    """Test decoding Meshtastic message"""
    client = MQTTClient(
        'localhost', 1883, mock_logger,
        decoder=mock_decoder
    )
    client.connect()
    
    # Create a mock message
    mock_message = Mock()
    mock_message.topic = "msh/node123/json/text"
    mock_message.payload = b'{"text": "Test message"}'
    
    # Mock _write_output to capture output
    client._write_output = Mock()
    
    # Call the callback
    client._on_message(mock_mqtt_client, None, mock_message)
    
    # Verify decoder was called
    mock_decoder.decode_message.assert_called_once_with(
        mock_message.topic, mock_message.payload
    )
    
    # Verify output was written
    client._write_output.assert_called_once()

# Test relay functionality
def test_mqtt_client_relay_message(mock_mqtt_client, mock_logger):
    """Test relaying message to another topic"""
    client = MQTTClient(
        'localhost', 1883, mock_logger,
        relay_enabled=True,
        relay_topic='relay/topic'
    )
    client.connect()
    
    # Test with a regular message
    client.relay_message("Test message", "source/topic")
    
    # Verify publish was called
    mock_mqtt_client.publish.assert_called_once()

def test_mqtt_client_relay_message_with_formatting(mock_mqtt_client, mock_logger):
    """Test relaying message with topic formatting"""
    client = MQTTClient(
        'localhost', 1883, mock_logger,
        relay_enabled=True,
        relay_topic='relay/{node_id}/{message_type}'
    )
    client.connect()
    
    # Test with a source topic that has expected parts
    source_topic = "msh/node123/json/text"
    client.relay_message("Test message", source_topic)
    
    # Verify publish was called with formatted topic
    mock_mqtt_client.publish.assert_called_once()
    args, _ = mock_mqtt_client.publish.call_args
    assert args[0] == "relay/node123/text"  # Formatted topic

@requires_meshtastic
def test_mqtt_client_relay_meshtastic_message(mock_mqtt_client, mock_logger):
    """Test relaying MeshtasticMessage"""
    # Skip if not available
    if not MESHTASTIC_AVAILABLE:
        pytest.skip("Meshtastic library not available")
    
    client = MQTTClient(
        'localhost', 1883, mock_logger,
        relay_enabled=True,
        relay_topic='relay/topic'
    )
    client.connect()
    
    # Create a MeshtasticMessage
    message = MeshtasticMessage(
        topic="msh/node123/json/text",
        raw_payload=b'{"text": "Hello world"}',
        message_type="text",
        node_id="node123",
        parsed=True,
        decoded_data={"text": "Hello world"}
    )
    
    # Test with different output formats
    for output_format in ['text', 'json', 'raw']:
        client.output_format = output_format
        mock_mqtt_client.publish.reset_mock()
        
        # Call relay
        client.relay_message(message, message.topic)
        
        # Verify publish was called
        mock_mqtt_client.publish.assert_called_once()

# Test output functionality
def test_mqtt_client_write_output_stdout(mock_mqtt_client, mock_logger):
    """Test writing output to stdout"""
    client = MQTTClient('localhost', 1883, mock_logger)
    
    # Mock print function
    with patch('builtins.print') as mock_print:
        client._write_output("Test output")
        mock_print.assert_called_once()

def test_mqtt_client_write_output_file(mock_mqtt_client, mock_logger, temp_output_file):
    """Test writing output to file"""
    client = MQTTClient(
        'localhost', 1883, mock_logger,
        output_file=temp_output_file
    )
    
    # Write some output
    client._write_output("Test output")
    
    # Close the file
    client.disconnect()
    
    # Verify file content
    with open(temp_output_file, 'r') as f:
        content = f.read()
        assert "Test output" in content

def test_mqtt_client_write_output_file_error(mock_mqtt_client, mock_logger):
    """Test writing output to file with error"""
    # Use a directory as the output file (which will cause an error)
    client = MQTTClient(
        'localhost', 1883, mock_logger,
        output_file="/dev/null/invalid"  # This should cause an error on open
    )
    
    # Mock print function
    with patch('builtins.print') as mock_print:
        # Write should fall back to stdout
        client._write_output("Test output")
        mock_print.assert_called_once()

# Integration tests with Phase 3 features
@requires_meshtastic
def test_integration_with_decoder(mock_mqtt_client, mock_logger, mock_decoder):
    """Integration test with decoder"""
    client = MQTTClient(
        'localhost', 1883, mock_logger,
        decoder=mock_decoder,
        output_format='json'
    )
    client.connect()
    
    # Create a mock message
    mock_message = Mock()
    mock_message.topic = "msh/node123/json/text"
    mock_message.payload = b'{"text": "Test message"}'
    
    # Mock _write_output
    client._write_output = Mock()
    
    # Call the callback
    client._on_message(mock_mqtt_client, None, mock_message)
    
    # Verify decoder was called
    mock_decoder.decode_message.assert_called_once()
    
    # Verify output was written in JSON format
    client._write_output.assert_called_once()

@requires_meshtastic
def test_integration_with_decoder_and_relay(mock_mqtt_client, mock_logger, mock_decoder):
    """Integration test with decoder and relay"""
    client = MQTTClient(
        'localhost', 1883, mock_logger,
        decoder=mock_decoder,
        relay_enabled=True,
        relay_topic='relay/topic'
    )
    client.connect()
    
    # Create a mock message
    mock_message = Mock()
    mock_message.topic = "msh/node123/json/text"
    mock_message.payload = b'{"text": "Test message"}'
    
    # Mock methods
    client._write_output = Mock()
    client.send_message = Mock()
    
    # Call the callback
    client._on_message(mock_mqtt_client, None, mock_message)
    
    # Verify decoder was called
    mock_decoder.decode_message.assert_called_once()
    
    # Verify output was written
    client._write_output.assert_called_once()
    
    # Verify relay was called
    client.send_message.assert_called_once()

# Test command line arguments
def test_full_cli_args_receive_with_decrypt(mock_mqtt_client, mock_logger, temp_key_file):
    """Test full CLI arguments for receive with decrypt"""
    args = parse_args([
        'receive', 
        '-b', 'localhost', 
        '-p', '1883',
        '-t', 'msh/#',
        '--decrypt',
        '--keyfile', temp_key_file,
        '-o', 'json',
        '--relay',
        '--relay-topic', 'relay/{node_id}'
    ])
    
    assert args.mode == 'receive'
    assert args.broker == 'localhost'
    assert args.port == 1883
    assert args.topic == 'msh/#'
    assert args.decrypt is True
    assert args.keyfile == temp_key_file
    assert args.output_format == 'json'
    assert args.relay is True
    assert args.relay_topic == 'relay/{node_id}' 