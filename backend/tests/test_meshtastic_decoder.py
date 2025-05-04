import pytest
from unittest.mock import Mock, patch
import json
import base64
from pathlib import Path
import sys

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

# Test MeshtasticMessage class
def test_meshtastic_message_init():
    """Test MeshtasticMessage initialization"""
    # Skip if not available
    if not MESHTASTIC_AVAILABLE:
        pytest.skip("Meshtastic library not available")
        
    msg = MeshtasticMessage(
        topic="msh/node123/json/text",
        raw_payload=b'{"text": "Hello world"}',
        message_type="text",
        node_id="node123"
    )
    
    assert msg.topic == "msh/node123/json/text"
    assert msg.raw_payload == b'{"text": "Hello world"}'
    assert msg.message_type == "text"
    assert msg.node_id == "node123"
    assert not msg.decrypted
    assert not msg.parsed
    assert msg.decoded_data is None

def test_meshtastic_message_to_dict(sample_message):
    """Test converting MeshtasticMessage to dict"""
    # Skip if not available
    if not MESHTASTIC_AVAILABLE:
        pytest.skip("Meshtastic library not available")
        
    # Set some parsed data
    sample_message.parsed = True
    sample_message.decoded_data = {"text": "Hello world"}
    
    result = sample_message.to_dict()
    
    assert result["topic"] == "msh/node123/json/text"
    assert result["message_type"] == "text"
    assert result["node_id"] == "node123"
    assert not result["decrypted"]
    assert result["parsed"]
    assert result["data"] == {"text": "Hello world"}

def test_meshtastic_message_to_json(sample_message):
    """Test converting MeshtasticMessage to JSON"""
    # Skip if not available
    if not MESHTASTIC_AVAILABLE:
        pytest.skip("Meshtastic library not available")
        
    # Set some parsed data
    sample_message.parsed = True
    sample_message.decoded_data = {"text": "Hello world"}
    
    json_str = sample_message.to_json()
    data = json.loads(json_str)
    
    assert data["topic"] == "msh/node123/json/text"
    assert data["data"] == {"text": "Hello world"}
    
    # Test pretty printing
    pretty_json = sample_message.to_json(pretty=True)
    assert pretty_json.count("\n") > 0  # Should have line breaks

def test_meshtastic_message_get_text_unparsed(sample_message):
    """Test getting text representation of unparsed message"""
    # Skip if not available
    if not MESHTASTIC_AVAILABLE:
        pytest.skip("Meshtastic library not available")
        
    # Message is unparsed by default
    text = sample_message.get_text()
    
    assert "Unparsed text message" in text
    assert "node123" in text

def test_meshtastic_message_get_text_parsed_text(sample_message):
    """Test getting text representation of parsed text message"""
    # Skip if not available
    if not MESHTASTIC_AVAILABLE:
        pytest.skip("Meshtastic library not available")
        
    # Set parsed text data
    sample_message.parsed = True
    sample_message.decoded_data = {"text": "Hello world"}
    
    text = sample_message.get_text()
    
    assert text == "[node123] Hello world"

def test_meshtastic_message_get_text_parsed_position(sample_message):
    """Test getting text representation of parsed position message"""
    # Skip if not available
    if not MESHTASTIC_AVAILABLE:
        pytest.skip("Meshtastic library not available")
        
    # Set parsed position data
    sample_message.parsed = True
    sample_message.message_type = "position"
    sample_message.decoded_data = {
        "latitude": 37.7749,
        "longitude": -122.4194
    }
    
    text = sample_message.get_text()
    
    assert "Position" in text
    assert "37.7749" in text
    assert "-122.4194" in text

def test_meshtastic_message_get_text_parsed_telemetry(sample_message):
    """Test getting text representation of parsed telemetry message"""
    # Skip if not available
    if not MESHTASTIC_AVAILABLE:
        pytest.skip("Meshtastic library not available")
        
    # Set parsed telemetry data
    sample_message.parsed = True
    sample_message.message_type = "telemetry"
    sample_message.decoded_data = {
        "device_metrics": {
            "battery_level": 85,
            "voltage": 3.7
        },
        "environment_metrics": {
            "temperature": 22.5,
            "relative_humidity": 45
        }
    }
    
    text = sample_message.get_text()
    
    assert "Telemetry" in text
    assert "battery=85%" in text
    assert "voltage=3.7V" in text
    assert "temp=22.5°C" in text
    assert "humidity=45%" in text

# Test MessageDecoder class
def test_message_decoder_init(decoder):
    """Test MessageDecoder initialization"""
    # Skip if not available
    if not MESHTASTIC_AVAILABLE:
        pytest.skip("Meshtastic library not available")
        
    assert decoder.channel_key == "test-key"
    assert decoder.decrypt_enabled == True
    assert decoder.meshtastic_available == MESHTASTIC_AVAILABLE

@pytest.mark.parametrize("topic,expected_match", [
    ("msh/node123/json/text", True),
    ("msh/node123/binary", True),
    ("msh/node123/json", True),
    ("invalid/topic", False),
    ("mesh/node123/json", False),
    ("", False),
])
def test_message_decoder_topic_pattern(decoder, topic, expected_match):
    """Test topic pattern matching"""
    # Skip if not available
    if not MESHTASTIC_AVAILABLE:
        pytest.skip("Meshtastic library not available")
        
    match = decoder.TOPIC_PATTERN.match(topic)
    assert bool(match) == expected_match

def test_decode_message_invalid_topic(decoder):
    """Test decoding message with invalid topic"""
    # Skip if not available
    if not MESHTASTIC_AVAILABLE:
        pytest.skip("Meshtastic library not available")
        
    result = decoder.decode_message("invalid/topic", b"payload")
    
    assert result.topic == "invalid/topic"
    assert result.raw_payload == b"payload"
    assert result.message_type == "unknown"
    assert result.node_id is None
    assert not result.parsed

def test_decode_json_message(decoder):
    """Test decoding a JSON message"""
    # Skip if not available
    if not MESHTASTIC_AVAILABLE:
        pytest.skip("Meshtastic library not available")
        
    topic = "msh/node123/json/text"
    payload = b'{"text": "Hello world"}'
    
    result = decoder.decode_message(topic, payload)
    
    assert result.topic == topic
    assert result.raw_payload == payload
    assert result.message_type == "text"
    assert result.node_id == "node123"
    assert result.parsed
    assert result.decoded_data == {"text": "Hello world"}

def test_decode_json_message_invalid_json(decoder):
    """Test decoding an invalid JSON message"""
    # Skip if not available
    if not MESHTASTIC_AVAILABLE:
        pytest.skip("Meshtastic library not available")
        
    topic = "msh/node123/json/text"
    payload = b'{invalid json}'
    
    result = decoder.decode_message(topic, payload)
    
    assert result.topic == topic
    assert result.raw_payload == payload
    assert result.message_type == "text"
    assert result.node_id == "node123"
    assert not result.parsed

@requires_meshtastic
def test_decode_binary_message():
    """Test decoding a binary message (requires Meshtastic)"""
    # This test is more complex and would require mocking the Meshtastic protocol
    # buffer classes to properly test binary message decoding.
    # For a basic test, we'll just verify that the function handles missing Meshtastic gracefully.
    
    with patch('meshtastic_decoder.MESHTASTIC_AVAILABLE', False):
        decoder = MessageDecoder(channel_key="test-key")
        topic = "msh/node123/binary"
        payload = b'\x01\x02\x03\x04'
        
        result = decoder.decode_message(topic, payload)
        
        assert result.topic == topic
        assert not result.parsed

def test_parse_key_hex():
    """Test parsing hex keys"""
    # Skip if not available
    if not MESHTASTIC_AVAILABLE:
        pytest.skip("Meshtastic library not available")
        
    hex_key = "0123456789abcdef"
    result = MessageDecoder.parse_key(hex_key)
    
    assert result == bytes.fromhex(hex_key)

def test_parse_key_base64():
    """Test parsing base64 keys"""
    # Skip if not available
    if not MESHTASTIC_AVAILABLE:
        pytest.skip("Meshtastic library not available")
        
    # Create a base64 encoded key
    original = b"test_key_12345"
    base64_key = base64.b64encode(original).decode('utf-8')
    
    result = MessageDecoder.parse_key(base64_key)
    
    assert result == original

@pytest.mark.parametrize("key_string,expected_type", [
    ("0123456789abcdef", bytes),   # Hex key
    ("dGVzdF9rZXlfMTIzNDU=", bytes),  # Base64 key
    ("plain_text_key", bytes),  # Plain text fallback
])
def test_parse_key_variations(key_string, expected_type):
    """Test parsing different key formats"""
    # Skip if not available
    if not MESHTASTIC_AVAILABLE:
        pytest.skip("Meshtastic library not available")
        
    result = MessageDecoder.parse_key(key_string)
    
    assert isinstance(result, expected_type)

def test_load_key_from_file(tmp_path):
    """Test loading key from file"""
    # Skip if not available
    if not MESHTASTIC_AVAILABLE:
        pytest.skip("Meshtastic library not available")
        
    # Create a temporary key file
    key_content = "test_channel_key_12345"
    key_file = tmp_path / "test_key.txt"
    key_file.write_text(key_content)
    
    result = MessageDecoder.load_key_from_file(str(key_file))
    
    assert result == key_content

def test_load_key_from_nonexistent_file():
    """Test loading key from nonexistent file"""
    # Skip if not available
    if not MESHTASTIC_AVAILABLE:
        pytest.skip("Meshtastic library not available")
        
    result = MessageDecoder.load_key_from_file("nonexistent_file.txt")
    
    assert result is None 