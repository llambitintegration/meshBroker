"""
Meshtastic message decoder module for decoding Protocol Buffer messages and handling encryption.
"""
import logging
import json
import base64
import re
import struct
import binascii
from typing import Dict, Any, Optional, Tuple, List, Union

# Configure logger
logger = logging.getLogger(__name__)

# Try to import Meshtastic modules, but make them optional
MESHTASTIC_AVAILABLE = False
try:
    import meshtastic
    import meshtastic.mesh_pb2 as mesh_pb2
    import meshtastic.portnums_pb2 as portnums_pb2
    import meshtastic.telemetry_pb2 as telemetry_pb2
    from google.protobuf.json_format import MessageToDict
    MESHTASTIC_AVAILABLE = True
    logger.info("Meshtastic library available for message decoding")
except ImportError:
    logger.warning("Meshtastic library not available, decryption will be limited")


class MeshtasticMessage:
    """Container for a decoded Meshtastic message"""
    def __init__(
        self, 
        topic: str, 
        raw_payload: bytes,
        message_type: str = "unknown",
        node_id: Optional[str] = None,
        decrypted: bool = False,
        parsed: bool = False,
        decoded_data: Any = None
    ):
        self.topic = topic
        self.raw_payload = raw_payload
        self.message_type = message_type
        self.node_id = node_id
        self.decrypted = decrypted
        self.parsed = parsed
        self.decoded_data = decoded_data

    def to_dict(self) -> Dict[str, Any]:
        """Convert message to dictionary representation"""
        result = {
            "topic": self.topic,
            "message_type": self.message_type,
            "node_id": self.node_id,
            "decrypted": self.decrypted,
            "parsed": self.parsed,
        }
        
        # Include decoded data if present
        if self.decoded_data is not None:
            if isinstance(self.decoded_data, dict):
                result["data"] = self.decoded_data
            elif hasattr(self.decoded_data, "__dict__"):
                result["data"] = self.decoded_data.__dict__
            else:
                result["data"] = str(self.decoded_data)
        else:
            # Include some raw data info
            result["data"] = {
                "size": len(self.raw_payload),
                "hex_preview": self.raw_payload.hex()[:30] + ("..." if len(self.raw_payload) > 15 else "")
            }
        
        return result

    def to_json(self, pretty: bool = False) -> str:
        """Convert message to JSON string"""
        indent = 2 if pretty else None
        return json.dumps(self.to_dict(), indent=indent)

    def get_text(self) -> str:
        """Get human-readable representation of the message"""
        if not self.parsed:
            return f"Unparsed {self.message_type} message from {self.node_id or 'unknown'} ({len(self.raw_payload)} bytes)"
        
        if self.message_type == "text":
            if isinstance(self.decoded_data, dict) and "text" in self.decoded_data:
                return f"[{self.node_id}] {self.decoded_data['text']}"
            return f"[{self.node_id}] Text message (could not extract text content)"
        
        elif self.message_type == "position":
            if isinstance(self.decoded_data, dict):
                lat = self.decoded_data.get("latitude", "unknown")
                lon = self.decoded_data.get("longitude", "unknown")
                return f"[{self.node_id}] Position: lat={lat}, lon={lon}"
            return f"[{self.node_id}] Position update (could not extract coordinates)"
        
        elif self.message_type == "telemetry":
            if isinstance(self.decoded_data, dict):
                metrics = []
                if "device_metrics" in self.decoded_data:
                    battery = self.decoded_data["device_metrics"].get("battery_level", "unknown")
                    voltage = self.decoded_data["device_metrics"].get("voltage", "unknown")
                    metrics.append(f"battery={battery}%, voltage={voltage}V")
                if "environment_metrics" in self.decoded_data:
                    temp = self.decoded_data["environment_metrics"].get("temperature", "unknown")
                    humidity = self.decoded_data["environment_metrics"].get("relative_humidity", "unknown")
                    metrics.append(f"temp={temp}°C, humidity={humidity}%")
                
                if metrics:
                    return f"[{self.node_id}] Telemetry: {', '.join(metrics)}"
            return f"[{self.node_id}] Telemetry update"
        
        # Default fallback for other message types
        return f"[{self.node_id}] {self.message_type.capitalize()} message"


class MessageDecoder:
    """Decoder for Meshtastic messages"""
    
    # Topic pattern for Meshtastic messages
    TOPIC_PATTERN = re.compile(r'^msh/([^/]+)/([^/]+)(?:/([^/]+))?$')
    
    # Known message types and their protobuf handlers
    MESSAGE_TYPES = {
        "text": portnums_pb2.TEXT_MESSAGE_APP if MESHTASTIC_AVAILABLE else 1,
        "position": portnums_pb2.POSITION_APP if MESHTASTIC_AVAILABLE else 3,
        "telemetry": portnums_pb2.TELEMETRY_APP if MESHTASTIC_AVAILABLE else 8,
        "nodeid": 0,  # Special case for node info
        "user": 0,    # Special case for user info
        "heartbeat": 0  # Special case for heartbeat
    }
    
    def __init__(self, channel_key: Optional[str] = None):
        """
        Initialize the message decoder
        
        Args:
            channel_key: Channel encryption key (PSK) if available
        """
        self.channel_key = channel_key
        # Fix: Empty key should disable decryption
        self.decrypt_enabled = channel_key is not None and len(channel_key) > 0
        self.meshtastic_available = MESHTASTIC_AVAILABLE

    def decode_message(self, topic: str, payload: bytes) -> MeshtasticMessage:
        """
        Decode a Meshtastic message
        
        Args:
            topic: MQTT topic
            payload: Raw message payload
            
        Returns:
            Decoded message
        """
        # Extract information from topic
        topic_match = self.TOPIC_PATTERN.match(topic)
        if not topic_match:
            logger.warning(f"Topic {topic} does not match Meshtastic pattern")
            return MeshtasticMessage(topic, payload)
        
        # Extract topic parts
        node_id = topic_match.group(1)
        format_type = topic_match.group(2)  # 'json', 'binary', etc.
        message_type = topic_match.group(3) if topic_match.group(3) else "unknown"
        
        # Create message container
        message = MeshtasticMessage(
            topic=topic,
            raw_payload=payload,
            message_type=message_type,
            node_id=node_id
        )
        
        # Handle different format types
        if format_type == "json":
            return self._decode_json_message(message)
        elif format_type == "binary":
            return self._decode_binary_message(message)
        else:
            logger.warning(f"Unknown format type: {format_type}")
            return message

    def _decode_json_message(self, message: MeshtasticMessage) -> MeshtasticMessage:
        """Decode a JSON format message"""
        try:
            # Parse JSON payload
            if isinstance(message.raw_payload, bytes):
                json_data = json.loads(message.raw_payload.decode('utf-8'))
            else:
                json_data = json.loads(message.raw_payload)
            
            # Check for encrypted data that needs further decryption
            if self.decrypt_enabled and "encrypted" in json_data:
                # TODO: Implement JSON payload decryption if needed
                pass
            
            # Store decoded data
            message.decoded_data = json_data
            message.parsed = True
            
            return message
            
        except json.JSONDecodeError:
            logger.error(f"Failed to parse JSON payload for topic {message.topic}")
            return message
        except Exception as e:
            logger.error(f"Error decoding JSON message: {e}")
            return message

    def _decode_binary_message(self, message: MeshtasticMessage) -> MeshtasticMessage:
        """Decode a binary format message"""
        if not MESHTASTIC_AVAILABLE:
            logger.warning("Cannot decode binary message without Meshtastic library")
            return message
        
        try:
            # Attempt to decode as a MeshPacket
            # First check if it looks like a valid protobuf message
            # (This is just a basic check, not comprehensive)
            if len(message.raw_payload) < 2:
                logger.warning(f"Payload too small to be a valid protobuf message")
                return message
            
            # Try to parse as MeshPacket
            packet = mesh_pb2.MeshPacket()
            try:
                packet.ParseFromString(message.raw_payload)
                
                # Check if decryption is needed
                if packet.encrypted and self.decrypt_enabled:
                    # Decrypt the packet
                    decrypted_packet = self._decrypt_packet(packet)
                    if decrypted_packet:
                        packet = decrypted_packet
                        message.decrypted = True
                    else:
                        logger.warning("Failed to decrypt packet")
                
                # Parse the payload based on the portnum
                if hasattr(packet, 'decoded') and packet.decoded.portnum:
                    portnum = packet.decoded.portnum
                    payload_data = packet.decoded.payload
                    
                    # Handle different message types based on portnum
                    if portnum == self.MESSAGE_TYPES.get("text"):
                        message.message_type = "text"
                        text = payload_data.decode('utf-8', errors='replace')
                        message.decoded_data = {"text": text}
                        message.parsed = True
                        
                    elif portnum == self.MESSAGE_TYPES.get("position"):
                        message.message_type = "position"
                        position = mesh_pb2.Position()
                        position.ParseFromString(payload_data)
                        message.decoded_data = MessageToDict(position)
                        message.parsed = True
                        
                    elif portnum == self.MESSAGE_TYPES.get("telemetry"):
                        message.message_type = "telemetry"
                        telemetry = telemetry_pb2.Telemetry()
                        telemetry.ParseFromString(payload_data)
                        message.decoded_data = MessageToDict(telemetry)
                        message.parsed = True
                        
                    else:
                        logger.info(f"Unknown portnum: {portnum}")
                        # Store basic packet info
                        message.decoded_data = {
                            "from": packet.from_node,
                            "to": packet.to_node,
                            "id": packet.id,
                            "portnum": portnum,
                            "payload_size": len(payload_data)
                        }
                        message.parsed = True
                
                else:
                    # Store basic packet info
                    message.decoded_data = {
                        "from": packet.from_node,
                        "to": packet.to_node,
                        "id": packet.id,
                        "encrypted": packet.encrypted
                    }
                    message.parsed = True
                
            except Exception as e:
                logger.error(f"Error parsing MeshPacket: {e}")
                # If it's not a MeshPacket, try other message types or store raw data
                message.decoded_data = {
                    "error": f"Failed to parse protobuf message: {str(e)}",
                    "size": len(message.raw_payload),
                    "hex": message.raw_payload.hex()[:50] + "..." if len(message.raw_payload) > 25 else message.raw_payload.hex()
                }
            
            return message
            
        except Exception as e:
            logger.error(f"Error decoding binary message: {e}")
            return message

    def _decrypt_packet(self, packet: Any) -> Optional[Any]:
        """
        Decrypt a MeshPacket using the channel key
        
        Args:
            packet: Encrypted MeshPacket
            
        Returns:
            Decrypted MeshPacket or None if decryption fails
        """
        if not self.decrypt_enabled or not self.channel_key:
            return None
            
        try:
            if not MESHTASTIC_AVAILABLE:
                logger.warning("Cannot decrypt without Meshtastic library")
                return None
                
            # Note: This is a placeholder. Actual implementation would use
            # Meshtastic's crypto functions to decrypt the packet
            # Something like:
            # from meshtastic.crypto import decrypt_packet
            # return decrypt_packet(packet, self.channel_key)
            
            # For now, return the original packet
            logger.warning("Packet decryption not fully implemented")
            return packet
            
        except Exception as e:
            logger.error(f"Error decrypting packet: {e}")
            return None

    @staticmethod
    def parse_key(key_string: str) -> Optional[bytes]:
        """
        Parse a key string into bytes
        
        Args:
            key_string: Key as a hex string or base64 string
            
        Returns:
            Key as bytes or None if parsing fails
        """
        try:
            # Try as hex
            return binascii.unhexlify(key_string)
        except binascii.Error:
            # Try as base64
            try:
                return base64.b64decode(key_string)
            except Exception:
                pass
        
        # If all else fails, try as raw bytes
        try:
            return key_string.encode('utf-8')
        except Exception as e:
            logger.error(f"Failed to parse key: {e}")
            return None

    @staticmethod
    def load_key_from_file(filename: str) -> Optional[str]:
        """
        Load a key from a file
        
        Args:
            filename: Path to the key file
            
        Returns:
            Key as a string or None if loading fails
        """
        try:
            with open(filename, 'r') as f:
                return f.read().strip()
        except Exception as e:
            logger.error(f"Failed to load key from file {filename}: {e}")
            return None 