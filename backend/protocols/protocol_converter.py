"""
Protocol conversion utilities for Meshtastic messages
"""
import json
import base64
import logging
from typing import Dict, Any, Optional, Union, Tuple

# Import Meshtastic protocol buffer definitions
try:
    from meshtastic.protobuf import mesh_pb2, portnums_pb2, telemetry_pb2
except ImportError:
    logging.warning("Meshtastic protocol buffer definitions not found. Some features will be limited.")
    mesh_pb2 = portnums_pb2 = telemetry_pb2 = None

# Configure logger
logger = logging.getLogger(__name__)

class ProtocolConverter:
    """Converts between different message formats in the Meshtastic ecosystem"""
    
    @staticmethod
    def binary_to_json(binary_data: bytes, topic: str) -> Optional[Dict[str, Any]]:
        """
        Convert binary Meshtastic message to JSON format
        
        Args:
            binary_data: The binary message payload
            topic: The MQTT topic which helps determine the message type
            
        Returns:
            Dict containing the JSON representation of the message or None if conversion fails
        """
        try:
            # Check if we have the protocol buffer modules
            if not mesh_pb2:
                logger.warning("Cannot convert binary data without Meshtastic protocol buffers")
                return {
                    "binary_size": len(binary_data),
                    "binary_base64": base64.b64encode(binary_data).decode('utf-8'),
                    "warning": "Protocol buffer conversion not available"
                }
            
            # Try to determine message type from topic
            # Topics are typically in the format: msh/{node_id}/{message_type}
            parts = topic.split('/')
            if len(parts) < 3:
                logger.warning(f"Cannot determine message type from topic: {topic}")
                return None
            
            # Extract the message type
            message_type = parts[2]  # e.g., 'binary' or 'data'
            
            # Parse as appropriate message type
            if message_type == "data":
                # Parse as a MeshPacket
                packet = mesh_pb2.MeshPacket()
                packet.ParseFromString(binary_data)
                return ProtocolConverter._mesh_packet_to_json(packet)
            
            elif message_type == "binary":
                # For raw binary data, we just base64 encode
                return {
                    "type": "binary",
                    "size": len(binary_data),
                    "data": base64.b64encode(binary_data).decode('utf-8')
                }
            
            else:
                logger.warning(f"Unknown message type: {message_type}")
                return None
                
        except Exception as e:
            logger.error(f"Error converting binary to JSON: {e}")
            return None
    
    @staticmethod
    def _mesh_packet_to_json(packet: Any) -> Dict[str, Any]:
        """Convert a MeshPacket to JSON representation"""
        result = {
            "from": packet.from_node,
            "to": packet.to_node,
            "id": packet.id,
            "timestamp": packet.rx_time,
        }
        
        # Add channel info if available
        if hasattr(packet, "channel"):
            result["channel"] = packet.channel
        
        # Add hop limit if available
        if hasattr(packet, "hop_limit"):
            result["hop_limit"] = packet.hop_limit
        
        # Add priority if available
        if hasattr(packet, "priority"):
            result["priority"] = packet.priority
        
        # Add payload data based on type
        if packet.payload_variant == "decoded":
            # Handle the decoded payload
            decoded = packet.decoded
            
            # Get port number name
            port_num = decoded.portnum
            port_name = portnums_pb2.PortNum.Name(port_num) if port_num in portnums_pb2.PortNum.values() else str(port_num)
            
            result["port"] = {
                "number": port_num,
                "name": port_name
            }
            
            # Handle different payload types based on port number
            if port_num == portnums_pb2.PortNum.TEXT_MESSAGE_APP:
                # Text message
                result["type"] = "text"
                result["text"] = decoded.payload.decode('utf-8', errors='replace')
                
            elif port_num == portnums_pb2.PortNum.TELEMETRY_APP:
                # Telemetry data
                result["type"] = "telemetry"
                telemetry = telemetry_pb2.Telemetry()
                telemetry.ParseFromString(decoded.payload)
                
                telemetry_data = {}
                
                # Extract environment data if present
                if telemetry.HasField("environment"):
                    env = telemetry.environment
                    telemetry_data["environment"] = {}
                    if env.HasField("temperature"):
                        telemetry_data["environment"]["temperature"] = env.temperature
                    if env.HasField("relative_humidity"):
                        telemetry_data["environment"]["humidity"] = env.relative_humidity
                    if env.HasField("barometric_pressure"):
                        telemetry_data["environment"]["pressure"] = env.barometric_pressure
                    if env.HasField("gas_resistance"):
                        telemetry_data["environment"]["gas_resistance"] = env.gas_resistance
                
                # Extract device metrics if present
                if telemetry.HasField("device_metrics"):
                    metrics = telemetry.device_metrics
                    telemetry_data["device"] = {}
                    if metrics.HasField("battery_level"):
                        telemetry_data["device"]["battery_level"] = metrics.battery_level
                    if metrics.HasField("voltage"):
                        telemetry_data["device"]["voltage"] = metrics.voltage
                    if metrics.HasField("channel_utilization"):
                        telemetry_data["device"]["channel_utilization"] = metrics.channel_utilization
                    if metrics.HasField("air_util_tx"):
                        telemetry_data["device"]["air_util_tx"] = metrics.air_util_tx
                
                result["telemetry"] = telemetry_data
                
            else:
                # Other types of payloads
                result["type"] = "other"
                result["port_number"] = port_num
                result["payload_base64"] = base64.b64encode(decoded.payload).decode('utf-8')
        
        elif packet.payload_variant == "encrypted":
            # For encrypted payloads, we can't decode the content
            result["type"] = "encrypted"
            result["payload_size"] = len(packet.encrypted.payload)
            
        return result
    
    @staticmethod
    def json_to_binary(json_data: Dict[str, Any], message_type: str) -> Optional[bytes]:
        """
        Convert JSON data to binary format for Meshtastic protocol
        
        Args:
            json_data: The JSON data to convert
            message_type: The type of message to create (text, telemetry, etc.)
            
        Returns:
            bytes containing the binary representation or None if conversion fails
        """
        try:
            # Check if we have the protocol buffer modules
            if not mesh_pb2:
                logger.warning("Cannot convert to binary without Meshtastic protocol buffers")
                return None
            
            # Create a MeshPacket
            packet = mesh_pb2.MeshPacket()
            
            # Set basic fields
            if "from" in json_data:
                packet.from_node = json_data["from"]
            if "to" in json_data:
                packet.to_node = json_data["to"]
            
            # Create decoded payload
            decoded = mesh_pb2.Data()
            
            # Set port number based on message type
            if message_type == "text":
                decoded.portnum = portnums_pb2.PortNum.TEXT_MESSAGE_APP
                if "text" in json_data:
                    decoded.payload = json_data["text"].encode('utf-8')
                else:
                    logger.warning("Missing 'text' field in JSON data")
                    return None
            
            elif message_type == "telemetry":
                decoded.portnum = portnums_pb2.PortNum.TELEMETRY_APP
                # Creating telemetry message is complex and would require more fields
                logger.warning("Telemetry message creation not fully implemented")
                return None
            
            elif message_type == "position":
                decoded.portnum = portnums_pb2.PortNum.POSITION_APP
                # Creating position message would require more fields
                logger.warning("Position message creation not fully implemented")
                return None
            
            else:
                logger.warning(f"Unsupported message type: {message_type}")
                return None
            
            # Set the decoded payload in the packet
            packet.payload_variant = "decoded"
            packet.decoded.CopyFrom(decoded)
            
            # Serialize the packet
            return packet.SerializeToString()
            
        except Exception as e:
            logger.error(f"Error converting JSON to binary: {e}")
            return None
    
    @staticmethod
    def detect_message_format(payload: Union[bytes, str]) -> Tuple[str, Any]:
        """
        Detect the format of a message payload and convert to appropriate representation
        
        Args:
            payload: The message payload, either as bytes or string
            
        Returns:
            Tuple of (format_type, parsed_payload)
            format_type can be 'json', 'binary', or 'text'
        """
        # First, handle binary data
        if isinstance(payload, bytes):
            try:
                # Try to decode as utf-8 text
                text = payload.decode('utf-8')
                
                # See if it's actually JSON
                try:
                    data = json.loads(text)
                    return ("json", data)
                except json.JSONDecodeError:
                    # Just plain text
                    return ("text", text)
                    
            except UnicodeDecodeError:
                # It's truly binary data
                return ("binary", payload)
        
        # Handle string input
        elif isinstance(payload, str):
            # See if it's JSON
            try:
                data = json.loads(payload)
                return ("json", data)
            except json.JSONDecodeError:
                # Just plain text
                return ("text", payload)
        
        # Fallback
        return ("unknown", payload)