import pytest
from unittest.mock import Mock, patch, MagicMock
import paho.mqtt.client as mqtt
import json
import base64
import tempfile
import os
import sys
from pathlib import Path

# Add the parent directory to the Python path to import the needed modules
sys.path.append(str(Path(__file__).parent.parent))

# Import required modules with fallback for import errors
try:
    from meshtastic_mqtt_cli import MQTTClient
    from meshtastic_decoder import MessageDecoder, MeshtasticMessage, MESHTASTIC_AVAILABLE
except ImportError:
    try:
        from backend.meshtastic_mqtt_cli import MQTTClient
        from backend.meshtastic_decoder import MessageDecoder, MeshtasticMessage, MESHTASTIC_AVAILABLE
    except ImportError:
        # Set flags to indicate modules are not available
        MQTTClient = Mock
        MessageDecoder = Mock
        MeshtasticMessage = Mock
        MESHTASTIC_AVAILABLE = False

# Mark tests that require Meshtastic
requires_meshtastic = pytest.mark.skipif(not MESHTASTIC_AVAILABLE, 
                                         reason="Meshtastic library not available")

# Fixtures
@pytest.fixture
def mock_mqtt_client():
    with patch('paho.mqtt.client.Client') as mock_client:
        client_instance = Mock()
        mock_client.return_value = client_instance
        
        # Mock the connection success
        client_instance.connect.return_value = 0
        client_instance.loop_start.return_value = None
        
        # Configure publish return value
        publish_result = Mock()
        publish_result.rc = mqtt.MQTT_ERR_SUCCESS
        client_instance.publish.return_value = publish_result
        
        # Configure subscribe to return a success tuple
        client_instance.subscribe.return_value = (mqtt.MQTT_ERR_SUCCESS, 1)
        
        yield client_instance

@pytest.fixture
def mock_logger():
    with patch('logging.getLogger') as mock_log:
        logger = Mock()
        mock_log.return_value = logger
        yield logger

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
    with tempfile.NamedTemporaryFile(delete=False) as f:
        file_path = f.name
    
    yield file_path
    
    # Clean up
    os.unlink(file_path)

# Mock Meshtastic Protocol Buffer Classes for binary message testing
class MockMeshPacket:
    def __init__(self, from_node=1234, to_node=4321, id=42, encrypted=False):
        self.from_node = from_node
        self.to_node = to_node
        self.id = id
        self.encrypted = encrypted
        self.decoded = MockDecodedMessage()
    
    def ParseFromString(self, data):
        self.parse_called = True
        self.parse_data = data

class MockDecodedMessage:
    def __init__(self, portnum=1, payload=b'Hello, World!'):
        self.portnum = portnum
        self.payload = payload

# Integration tests - full flow
@requires_meshtastic
def test_full_decoding_flow(mock_mqtt_client, mock_logger, temp_key_file, temp_output_file):
    """Test full message flow: receive -> decode -> relay -> output"""
    
    # Mock the Meshtastic protocol buffer classes
    with patch('meshtastic_decoder.mesh_pb2') as mock_mesh_pb2, \
         patch('meshtastic_decoder.portnums_pb2') as mock_portnums_pb2, \
         patch('meshtastic_decoder.telemetry_pb2') as mock_telemetry_pb2, \
         patch('meshtastic_decoder.MessageToDict') as mock_message_to_dict:
        
        # Configure mock protocol buffers
        mock_mesh_pb2.MeshPacket = MockMeshPacket
        mock_portnums_pb2.TEXT_MESSAGE_APP = 1
        mock_portnums_pb2.POSITION_APP = 3
        mock_portnums_pb2.TELEMETRY_APP = 8
        
        # Configure mock MessageToDict
        mock_message_to_dict.return_value = {"mock": "dict_result"}
        
        # Create a real decoder with the test key file
        with open(temp_key_file, 'r') as f:
            channel_key = f.read().strip()
        
        decoder = MessageDecoder(channel_key=channel_key)
        
        # Create client with all features enabled
        client = MQTTClient(
            'localhost', 1883, mock_logger,
            decoder=decoder,
            relay_enabled=True,
            relay_topic='relay/{node_id}/{message_type}',
            output_format='json',
            output_file=temp_output_file
        )
        
        # Connect the client
        client.connect()
        
        # Test with different message types
        test_messages = [
            # Text message
            {
                'topic': 'msh/node123/json/text',
                'payload': b'{"text": "Hello world"}'
            },
            # Binary message
            {
                'topic': 'msh/node456/binary',
                'payload': b'\x01\x02\x03\x04'
            }
        ]
        
        for test_msg in test_messages:
            # Reset mocks
            mock_mqtt_client.reset_mock()
            
            # Create a mock message
            mock_message = Mock()
            mock_message.topic = test_msg['topic']
            mock_message.payload = test_msg['payload']
            
            # Process the message
            client._on_message(mock_mqtt_client, None, mock_message)
            
            # Check that relay happened
            if 'json/text' in test_msg['topic']:
                # For JSON messages
                assert mock_mqtt_client.publish.called
                args, _ = mock_mqtt_client.publish.call_args
                assert args[0] == f"relay/node123/text"  # Formatted topic
            
            elif 'binary' in test_msg['topic']:
                # For binary messages
                assert mock_mqtt_client.publish.called
        
        # Verify that output was written to the file
        client.disconnect()  # Close the file
        
        with open(temp_output_file, 'r') as f:
            content = f.read()
            assert len(content) > 0

# Test error recovery in the integration flow
@requires_meshtastic
def test_error_recovery_in_flow(mock_mqtt_client, mock_logger):
    """Test error recovery in the message flow"""
    
    # Create a decoder that sometimes fails
    decoder = Mock(spec=MessageDecoder)
    
    # Make decode_message fail on binary messages but work on JSON
    def mock_decode_message(topic, payload):
        if b'json' in topic.encode() or 'json' in topic:
            return MeshtasticMessage(
                topic=topic,
                raw_payload=payload,
                message_type="text",
                node_id="node123",
                parsed=True,
                decoded_data={"text": "Decoded message"}
            )
        else:
            raise Exception("Simulated decoder error")
    
    decoder.decode_message.side_effect = mock_decode_message
    
    # Create client with decoder and relay
    client = MQTTClient(
        'localhost', 1883, mock_logger,
        decoder=decoder,
        relay_enabled=True,
        relay_topic='relay/topic'
    )
    
    # Connect the client
    client.connect()
    
    # Test with both message types
    test_messages = [
        # JSON message - should succeed
        {
            'topic': 'msh/node123/json/text',
            'payload': b'{"text": "Hello world"}'
        },
        # Binary message - should handle error gracefully
        {
            'topic': 'msh/node456/binary',
            'payload': b'\x01\x02\x03\x04'
        }
    ]
    
    for test_msg in test_messages:
        # Reset mocks
        mock_mqtt_client.reset_mock()
        mock_logger.reset_mock()
        
        # Create a mock message
        mock_message = Mock()
        mock_message.topic = test_msg['topic']
        mock_message.payload = test_msg['payload']
        
        # Process the message - should not crash even if decoder fails
        client._on_message(mock_mqtt_client, None, mock_message)
        
        if 'binary' in test_msg['topic']:
            # Binary message should trigger an error log but not crash
            mock_logger.error.assert_called_once()
        else:
            # JSON message should be processed normally
            assert mock_mqtt_client.publish.called

# Test with different output formats
@requires_meshtastic
def test_different_output_formats(mock_mqtt_client, mock_logger, temp_output_file):
    """Test processing messages with different output formats"""
    
    # Create a basic decoder
    decoder = Mock(spec=MessageDecoder)
    decoder.decode_message.return_value = MeshtasticMessage(
        topic="msh/node123/json/text",
        raw_payload=b'{"text": "Hello world"}',
        message_type="text",
        node_id="node123",
        parsed=True,
        decoded_data={"text": "Hello world"}
    )
    
    # Test each output format
    for output_format in ['text', 'json', 'raw']:
        # Create client with this output format
        client = MQTTClient(
            'localhost', 1883, mock_logger,
            decoder=decoder,
            output_format=output_format,
            output_file=temp_output_file
        )
        
        # Connect the client
        client.connect()
        
        # Create a mock message
        mock_message = Mock()
        mock_message.topic = "msh/node123/json/text"
        mock_message.payload = b'{"text": "Hello world"}'
        
        # Process the message
        client._on_message(mock_mqtt_client, None, mock_message)
        
        # Close the client to flush output
        client.disconnect()
        
        # Verify output was written in the expected format
        with open(temp_output_file, 'r') as f:
            content = f.read()
            
            if output_format == 'json':
                # Should contain JSON structure
                assert '{' in content and '}' in content
                assert 'node123' in content
                
            elif output_format == 'text':
                # Should be formatted as text
                assert '[node123]' in content
                
            elif output_format == 'raw':
                # Should mention raw format
                assert 'Raw message' in content or 'size=' in content
        
        # Clear the file for next iteration
        with open(temp_output_file, 'w') as f:
            f.write('')

# Test decryption integration
@requires_meshtastic
def test_decryption_integration(mock_mqtt_client, mock_logger):
    """Test decryption integration with message flow"""
    
    # Mock the Meshtastic protocol buffer classes
    with patch('meshtastic_decoder.mesh_pb2') as mock_mesh_pb2, \
         patch('meshtastic_decoder.portnums_pb2') as mock_portnums_pb2, \
         patch('meshtastic_decoder.MESHTASTIC_AVAILABLE', True):
        
        # Configure mock protocol buffers
        mock_packet = MockMeshPacket(encrypted=True)
        mock_mesh_pb2.MeshPacket.return_value = mock_packet
        mock_portnums_pb2.TEXT_MESSAGE_APP = 1
        
        # Create a real decoder with encryption key
        decoder = MessageDecoder(channel_key="test-key-12345")
        
        # Mock the decrypt_packet method to simulate decryption
        with patch.object(decoder, '_decrypt_packet') as mock_decrypt:
            # Make decrypt return a "decrypted" packet
            decrypted_packet = MockMeshPacket(encrypted=False)
            decrypted_packet.decoded.payload = b'Decrypted text'
            mock_decrypt.return_value = decrypted_packet
            
            # Create client with decoder
            client = MQTTClient(
                'localhost', 1883, mock_logger,
                decoder=decoder
            )
            
            # Connect the client
            client.connect()
            
            # Mock _write_output to capture output
            client._write_output = Mock()
            
            # Create a mock encrypted binary message
            mock_message = Mock()
            mock_message.topic = "msh/node123/binary"
            mock_message.payload = b'\x01\x02\x03\x04'  # Encrypted data
            
            # Process the message
            client._on_message(mock_mqtt_client, None, mock_message)
            
            # Verify decryption was attempted
            mock_decrypt.assert_called_once()
            
            # Verify output was processed
            client._write_output.assert_called_once()