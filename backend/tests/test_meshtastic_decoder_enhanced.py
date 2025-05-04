import pytest
from unittest.mock import Mock, patch, MagicMock
import json
import base64
import binascii
from pathlib import Path
import sys
import io

# Add the parent directory to the Python path to import the module
sys.path.append(str(Path(__file__).parent.parent))

# Try importing our module, but handle case where Meshtastic might not be available
try:
    from meshtastic_decoder import MessageDecoder, MeshtasticMessage, MESHTASTIC_AVAILABLE
except ImportError:
    # Create mocks for testing without actual dependencies
    MessageDecoder = Mock
    MeshtasticMessage = Mock
    MESHTASTIC_AVAILABLE = False

# Skip tests that require Meshtastic library if it's not available
requires_meshtastic = pytest.mark.skipif(not MESHTASTIC_AVAILABLE, 
                                         reason="Meshtastic library not available")

# Mock protocol buffer classes
class MockMeshPacket:
    """Mock for MeshPacket class"""
    def __init__(self, from_node=1234, to_node=4321, id=42, encrypted=False):
        self.from_node = from_node
        self.to_node = to_node
        self.id = id
        self.encrypted = encrypted
        self.decoded = MockDecodedMessage()
    
    def ParseFromString(self, data):
        """Mock for protocol buffer ParseFromString"""
        # Just record that this was called
        self.parse_called = True
        self.parse_data = data

class MockDecodedMessage:
    """Mock for DecodedMessage class"""
    def __init__(self, portnum=1, payload=b'Hello, World!'):
        self.portnum = portnum
        self.payload = payload

class MockPosition:
    """Mock for Position protocol buffer"""
    def __init__(self, latitude=37.7749, longitude=-122.4194, altitude=0, time=0):
        self.latitude = latitude
        self.longitude = longitude
        self.altitude = altitude
        self.time = time
    
    def ParseFromString(self, data):
        """Mock for protocol buffer ParseFromString"""
        self.parse_called = True
        self.parse_data = data

class MockTelemetry:
    """Mock for Telemetry protocol buffer"""
    def __init__(self):
        self.device_metrics = {"battery_level": 80, "voltage": 3.7}
        self.environment_metrics = {"temperature": 22.5, "relative_humidity": 45}
    
    def ParseFromString(self, data):
        """Mock for protocol buffer ParseFromString"""
        self.parse_called = True
        self.parse_data = data

# Fixtures
@pytest.fixture
def sample_message():
    """Create a basic MeshtasticMessage for testing"""
    return MeshtasticMessage(
        topic="msh/node123/json/text",
        raw_payload=b'{"text": "Hello world"}',
        message_type="text",
        node_id="node123"
    )

@pytest.fixture
def decoder():
    """Create a MessageDecoder instance for testing"""
    return MessageDecoder(channel_key="test-key")

@pytest.fixture
def mock_mesh_protobuf():
    """Set up mocks for mesh protocol buffer classes"""
    mock_mesh_pb2 = MagicMock()
    mock_mesh_pb2.MeshPacket = MockMeshPacket
    mock_mesh_pb2.Position = MockPosition
    
    mock_telemetry_pb2 = MagicMock()
    mock_telemetry_pb2.Telemetry = MockTelemetry
    
    mock_portnums_pb2 = MagicMock()
    mock_portnums_pb2.TEXT_MESSAGE_APP = 1
    mock_portnums_pb2.POSITION_APP = 3
    mock_portnums_pb2.TELEMETRY_APP = 8
    
    mock_message_to_dict = MagicMock()
    mock_message_to_dict.return_value = {"mock": "dict_result"}
    
    with patch.multiple(
        "meshtastic_decoder",
        mesh_pb2=mock_mesh_pb2,
        telemetry_pb2=mock_telemetry_pb2,
        portnums_pb2=mock_portnums_pb2,
        MessageToDict=mock_message_to_dict
    ):
        yield {
            "mesh_pb2": mock_mesh_pb2,
            "telemetry_pb2": mock_telemetry_pb2,
            "portnums_pb2": mock_portnums_pb2,
            "MessageToDict": mock_message_to_dict
        }

# Test Enhanced Message Decryption
@requires_meshtastic
def test_decrypt_packet_with_key(decoder, mock_mesh_protobuf):
    """Test decrypting a packet with a valid key"""
    # Create a mock encrypted packet
    packet = MockMeshPacket(encrypted=True)
    
    # Test decryption
    result = decoder._decrypt_packet(packet)
    
    # Currently, the implementation just returns the original packet
    # with a warning, so we can't test much. In a real implementation,
    # we would verify the decryption worked.
    assert result is not None
    assert result is packet  # The current implementation just returns the packet

@requires_meshtastic
def test_decrypt_packet_without_key():
    """Test decrypting a packet without a key"""
    # Create decoder without a key
    decoder = MessageDecoder()
    assert not decoder.decrypt_enabled
    
    # Create a mock encrypted packet
    packet = MockMeshPacket(encrypted=True)
    
    # Test decryption
    result = decoder._decrypt_packet(packet)
    
    # Should fail because no key is provided
    assert result is None

# Test Binary Message Decoding
@requires_meshtastic
def test_decode_binary_text_message(decoder, mock_mesh_protobuf):
    """Test decoding a binary text message"""
    with patch('meshtastic_decoder.MESHTASTIC_AVAILABLE', True):
        # Create binary packet with text message
        mock_payload = b'Hello, World!'
        decoded = MockDecodedMessage(portnum=1, payload=mock_payload)  # 1 = TEXT_MESSAGE_APP
        packet = MockMeshPacket(encrypted=False)
        packet.decoded = decoded
        
        # Mock ParseFromString to return our packet
        with patch.object(mock_mesh_protobuf['mesh_pb2'].MeshPacket, 'ParseFromString', 
                         return_value=None) as mock_parse:
            # Set up patch to return our packet when ParseFromString is called
            mock_mesh_protobuf['mesh_pb2'].MeshPacket.return_value = packet
            
            # Test decoding
            result = decoder.decode_message("msh/node123/binary", b'binary_data')
            
            # Verify result
            assert result.message_type == "text"
            assert result.parsed
            assert "text" in result.decoded_data
            assert isinstance(result.decoded_data, dict)

@requires_meshtastic
def test_decode_binary_position_message(decoder, mock_mesh_protobuf):
    """Test decoding a binary position message"""
    with patch('meshtastic_decoder.MESHTASTIC_AVAILABLE', True):
        # Create binary packet with position message
        position_payload = b'position_data'
        decoded = MockDecodedMessage(portnum=3, payload=position_payload)  # 3 = POSITION_APP
        packet = MockMeshPacket(encrypted=False)
        packet.decoded = decoded
        
        # Mock ParseFromString to return our packet
        with patch.object(mock_mesh_protobuf['mesh_pb2'].MeshPacket, 'ParseFromString', 
                         return_value=None) as mock_parse:
            # Set up patch to return our packet when ParseFromString is called
            mock_mesh_protobuf['mesh_pb2'].MeshPacket.return_value = packet
            # Mock the Position.ParseFromString method
            position_mock = MockPosition()
            mock_mesh_protobuf['mesh_pb2'].Position.return_value = position_mock
            
            # Test decoding
            result = decoder.decode_message("msh/node123/binary", b'binary_data')
            
            # Verify result
            assert result.message_type == "position"
            assert result.parsed
            assert isinstance(result.decoded_data, dict)
            assert "mock" in result.decoded_data

@requires_meshtastic
def test_decode_binary_telemetry_message(decoder, mock_mesh_protobuf):
    """Test decoding a binary telemetry message"""
    with patch('meshtastic_decoder.MESHTASTIC_AVAILABLE', True):
        # Create binary packet with telemetry message
        telemetry_payload = b'telemetry_data'
        decoded = MockDecodedMessage(portnum=8, payload=telemetry_payload)  # 8 = TELEMETRY_APP
        packet = MockMeshPacket(encrypted=False)
        packet.decoded = decoded
        
        # Mock ParseFromString to return our packet
        with patch.object(mock_mesh_protobuf['mesh_pb2'].MeshPacket, 'ParseFromString', 
                         return_value=None) as mock_parse:
            # Set up patch to return our packet when ParseFromString is called
            mock_mesh_protobuf['mesh_pb2'].MeshPacket.return_value = packet
            # Mock the Telemetry.ParseFromString method
            telemetry_mock = MockTelemetry()
            mock_mesh_protobuf['telemetry_pb2'].Telemetry.return_value = telemetry_mock
            
            # Test decoding
            result = decoder.decode_message("msh/node123/binary", b'binary_data')
            
            # Verify result
            assert result.message_type == "telemetry"
            assert result.parsed
            assert isinstance(result.decoded_data, dict)
            assert "mock" in result.decoded_data

@requires_meshtastic
def test_decode_binary_unknown_portnum(decoder, mock_mesh_protobuf):
    """Test decoding a binary message with unknown portnum"""
    with patch('meshtastic_decoder.MESHTASTIC_AVAILABLE', True):
        # Create binary packet with unknown message type
        unknown_payload = b'unknown_data'
        decoded = MockDecodedMessage(portnum=999, payload=unknown_payload)  # Unknown portnum
        packet = MockMeshPacket(encrypted=False)
        packet.decoded = decoded
        
        # Mock ParseFromString to return our packet
        with patch.object(mock_mesh_protobuf['mesh_pb2'].MeshPacket, 'ParseFromString', 
                         return_value=None) as mock_parse:
            # Set up patch to return our packet when ParseFromString is called
            mock_mesh_protobuf['mesh_pb2'].MeshPacket.return_value = packet
            
            # Test decoding
            result = decoder.decode_message("msh/node123/binary", b'binary_data')
            
            # Verify result
            assert result.message_type == "unknown"  # Should default to unknown
            assert result.parsed
            assert "portnum" in result.decoded_data
            assert result.decoded_data["portnum"] == 999

@requires_meshtastic
def test_decode_binary_encrypted_message(decoder, mock_mesh_protobuf):
    """Test decoding an encrypted binary message"""
    with patch('meshtastic_decoder.MESHTASTIC_AVAILABLE', True):
        # Create encrypted binary packet
        packet = MockMeshPacket(encrypted=True)
        
        # Mock ParseFromString to return our packet
        with patch.object(mock_mesh_protobuf['mesh_pb2'].MeshPacket, 'ParseFromString', 
                         return_value=None) as mock_parse:
            # Set up patch to return our packet when ParseFromString is called
            mock_mesh_protobuf['mesh_pb2'].MeshPacket.return_value = packet
            
            # Mock the decrypt_packet method
            with patch.object(decoder, '_decrypt_packet', return_value=None) as mock_decrypt:
                # Test decoding (should fail to decrypt)
                result = decoder.decode_message("msh/node123/binary", b'encrypted_data')
                
                # Verify result
                assert not result.decrypted
                assert "encrypted" in result.decoded_data
                assert result.decoded_data["encrypted"] is True

@requires_meshtastic
def test_decode_binary_invalid_protobuf(decoder, mock_mesh_protobuf):
    """Test decoding invalid protobuf data"""
    with patch('meshtastic_decoder.MESHTASTIC_AVAILABLE', True):
        # Mock ParseFromString to raise an exception
        with patch.object(mock_mesh_protobuf['mesh_pb2'].MeshPacket, 'ParseFromString', 
                         side_effect=Exception("Invalid protobuf")) as mock_parse:
            
            # Test decoding
            result = decoder.decode_message("msh/node123/binary", b'invalid_data')
            
            # Verify result
            assert not result.parsed
            assert "error" in result.decoded_data
            assert "Invalid protobuf" in result.decoded_data["error"]

# Test key parsing and loading
@pytest.mark.parametrize("key_input,expected", [
    ("0123456789abcdef", b"\x01\x23\x45\x67\x89\xab\xcd\xef"),  # Hex
    ("dGVzdF9rZXlfMTIzNDU=", b"test_key_12345"),  # Base64
    ("plain_test_key", b"plain_test_key"),  # Plain text
    ("", None),  # Empty string
])
def test_parse_key_comprehensive(key_input, expected):
    """Test comprehensive key parsing"""
    if not MESHTASTIC_AVAILABLE:
        pytest.skip("Meshtastic library not available")
    
    if key_input == "":
        with pytest.raises(Exception):
            MessageDecoder.parse_key(key_input)
    else:
        result = MessageDecoder.parse_key(key_input)
        if expected is None:
            assert result is None
        else:
            assert result == expected

def test_load_key_from_nonexistent_file_detailed():
    """Test detailed behavior when loading key from nonexistent file"""
    if not MESHTASTIC_AVAILABLE:
        pytest.skip("Meshtastic library not available")
    
    # Test with a definitely nonexistent file
    result = MessageDecoder.load_key_from_file("/does/not/exist/at/all.key")
    
    assert result is None