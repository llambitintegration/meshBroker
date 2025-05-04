import argparse
import logging
import paho.mqtt.client as mqtt
from typing import Optional, Dict, Any, List, Union
import socket
import json
import re
import base64
import os
import sys
import io
from datetime import datetime

# Try to import the message decoder, but make it optional
try:
    from backend.meshtastic_decoder import MessageDecoder, MeshtasticMessage, MESHTASTIC_AVAILABLE
except ImportError:
    # Try relative import if backend package is not in the path
    try:
        from meshtastic_decoder import MessageDecoder, MeshtasticMessage, MESHTASTIC_AVAILABLE
    except ImportError:
        # Set flags to indicate decoder is not available
        MESHTASTIC_AVAILABLE = False
        MessageDecoder = None
        MeshtasticMessage = None

class MQTTClient:
    def __init__(
        self, 
        broker: str, 
        port: int, 
        logger: Optional[logging.Logger] = None,
        decoder: Optional[Any] = None,
        relay_enabled: bool = False,
        relay_topic: Optional[str] = None,
        output_format: str = "text",
        output_file: Optional[str] = None
    ):
        if not broker:
            raise ValueError("Broker address cannot be empty")
        if not (0 < port < 65536):
            raise ValueError("Port must be between 1 and 65535")
            
        self.broker = broker
        self.port = port
        self.connected = False
        self.logger = logger or logging.getLogger(__name__)
        self._client = mqtt.Client()
        
        # Message decoder for Meshtastic messages
        self.decoder = decoder
        
        # Relay settings
        self.relay_enabled = relay_enabled
        self.relay_topic = relay_topic
        
        # Output settings
        self.output_format = output_format
        self.output_file = output_file
        self.output_stream = None
        
        # Set up callbacks
        self._client.on_connect = self._on_connect
        self._client.on_message = self._on_message
        self._client.on_disconnect = self._on_disconnect
        
        # Meshtastic topic patterns
        self.meshtastic_topic_pattern = re.compile(r'^msh/.*')
        self.meshtastic_binary_topic_pattern = re.compile(r'^msh/.*/binary$')
        
        # Initialize output file if specified
        if self.output_file:
            try:
                self.output_stream = open(self.output_file, 'a', encoding='utf-8')
                self.logger.info(f"Opened output file: {self.output_file}")
            except Exception as e:
                self.logger.error(f"Failed to open output file {self.output_file}: {e}")
                self.output_stream = None

    def __del__(self):
        # Close output file if open
        if self.output_stream and not self.output_stream.closed:
            try:
                self.output_stream.close()
            except:
                pass

    def connect(self):
        try:
            # Validate broker address
            try:
                socket.gethostbyname(self.broker)
            except socket.gaierror:
                raise ValueError(f"Invalid broker address: {self.broker}")
            
            result = self._client.connect(self.broker, self.port)
            if result != 0:
                raise ConnectionError(f"Connection failed with result code: {result}")
                
            self._client.loop_start()
            self.connected = True
            self.logger.info(f"Connected to broker at {self.broker}:{self.port}")
        except Exception as e:
            self.logger.error(f"Failed to connect to broker: {e}")
            raise

    def disconnect(self):
        if self.connected:
            try:
                self._client.disconnect()
                self._client.loop_stop()
            finally:
                self.connected = False
                self.logger.info("Disconnected from broker")
                
                # Close output file if open
                if self.output_stream and not self.output_stream.closed:
                    self.output_stream.close()
                    self.logger.info(f"Closed output file: {self.output_file}")

    def send_message(self, topic: str, message: str, qos: int = 0, retain: bool = False):
        if not self.connected:
            raise RuntimeError("Not connected to broker")
            
        # Validate QoS level
        if not (0 <= qos <= 2):
            raise ValueError(f"Invalid QoS level: {qos}. Must be 0, 1, or 2.")
            
        # Validate topic
        if not topic:
            raise ValueError("Topic cannot be empty")
            
        try:
            result = self._client.publish(topic, message, qos, retain)
            if result.rc != mqtt.MQTT_ERR_SUCCESS:
                raise RuntimeError(f"Failed to publish message: {mqtt.error_string(result.rc)}")
            self.logger.info(f"Message published to {topic}")
        except Exception as e:
            self.logger.error(f"Failed to publish message: {e}")
            raise

    def relay_message(self, message: Any, source_topic: str):
        """Relay a message to another topic"""
        if not self.relay_enabled or not self.relay_topic:
            return
        
        try:
            # Format the message for relay based on the source and destination
            relay_payload = None
            
            # If message is a MeshtasticMessage object
            if isinstance(message, MeshtasticMessage):
                if self.output_format == "json":
                    relay_payload = message.to_json()
                elif self.output_format == "raw":
                    relay_payload = message.raw_payload
                else:  # text format
                    relay_payload = message.get_text()
            else:
                # Regular message, just convert to string if needed
                if isinstance(message, (dict, list)):
                    relay_payload = json.dumps(message)
                elif isinstance(message, bytes):
                    try:
                        relay_payload = message.decode('utf-8')
                    except UnicodeDecodeError:
                        # If not valid UTF-8, use base64
                        relay_payload = base64.b64encode(message).decode('utf-8')
                else:
                    relay_payload = str(message)
            
            if relay_payload:
                # Construct the relay topic - include source topic info if relay topic doesn't specify a pattern
                relay_topic = self.relay_topic
                if not '{' in relay_topic:
                    # No formatting placeholders, use as-is
                    pass
                else:
                    # Try to format with topic parts
                    try:
                        # Extract source topic parts for formatting
                        parts = source_topic.split('/')
                        topic_dict = {
                            'topic': source_topic,
                            'parts': parts
                        }
                        # If we have enough parts, map them to common names
                        if len(parts) >= 3:
                            topic_dict.update({
                                'prefix': parts[0],
                                'node_id': parts[1],
                                'format': parts[2],
                                'message_type': parts[3] if len(parts) > 3 else ''
                            })
                        
                        relay_topic = self.relay_topic.format(**topic_dict)
                    except Exception as e:
                        self.logger.error(f"Failed to format relay topic: {e}")
                        # Fall back to using relay_topic as-is
                
                # Publish to relay topic
                self.send_message(relay_topic, relay_payload)
                self.logger.info(f"Relayed message from {source_topic} to {relay_topic}")
        
        except Exception as e:
            self.logger.error(f"Failed to relay message: {e}")

    def subscribe(self, topic: str, qos: int = 0):
        if not self.connected:
            raise RuntimeError("Not connected to broker")
            
        # Validate topic
        if not topic:
            raise ValueError("Topic cannot be empty")
            
        # Validate QoS level
        if not (0 <= qos <= 2):
            raise ValueError(f"Invalid QoS level: {qos}. Must be 0, 1, or 2.")
            
        try:
            result, _ = self._client.subscribe(topic, qos)
            if result != mqtt.MQTT_ERR_SUCCESS:
                raise RuntimeError(f"Failed to subscribe: {mqtt.error_string(result)}")
            self.logger.info(f"Subscribed to {topic}")
        except Exception as e:
            self.logger.error(f"Failed to subscribe to {topic}: {e}")
            raise
    
    def _is_meshtastic_topic(self, topic: str) -> bool:
        """Check if a topic matches Meshtastic topic patterns"""
        return bool(self.meshtastic_topic_pattern.match(topic))
    
    def _is_meshtastic_binary_topic(self, topic: str) -> bool:
        """Check if a topic matches Meshtastic binary topic patterns"""
        return bool(self.meshtastic_binary_topic_pattern.match(topic))
    
    def _format_meshtastic_binary(self, payload: bytes, topic: str) -> Dict[str, Any]:
        """Format binary Meshtastic data for display"""
        # For Phase 1, just provide the basic info without full protocol parsing
        # This will be enhanced in Phase 3 with proper protocol buffer parsing
        result = {
            "type": "binary",
            "size": len(payload),
            "hex_preview": payload.hex()[:30] + ("..." if len(payload) > 15 else ""),
            "base64": base64.b64encode(payload).decode('utf-8')
        }
        
        # Extract topic parts for context
        parts = topic.split('/')
        if len(parts) >= 3:
            result["node_id"] = parts[1]
            result["message_type"] = parts[2]
            
        return result
    
    def _write_output(self, content: str):
        """Write content to the appropriate output (file or stdout)"""
        # Add timestamp
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        formatted_content = f"[{timestamp}] {content}"
        
        # Write to file if configured
        if self.output_stream and not self.output_stream.closed:
            try:
                self.output_stream.write(formatted_content + "\n")
                self.output_stream.flush()
            except Exception as e:
                self.logger.error(f"Failed to write to output file: {e}")
                # Fall back to stdout
                print(formatted_content)
        else:
            # Write to stdout
            print(formatted_content)

    def _on_connect(self, client, userdata, flags, rc):
        if rc == 0:
            self.logger.info("Connected to MQTT broker")
            self.connected = True
        else:
            error_messages = {
                1: "Connection refused - incorrect protocol version",
                2: "Connection refused - invalid client identifier",
                3: "Connection refused - server unavailable",
                4: "Connection refused - bad username or password",
                5: "Connection refused - not authorized"
            }
            error_msg = error_messages.get(rc, f"Connection failed with code {rc}")
            self.logger.error(error_msg)
            self.connected = False

    def _on_message(self, client, userdata, message):
        try:
            # Handle different payload types
            payload = message.payload
            topic = message.topic
            
            # Check if this is a Meshtastic topic
            is_meshtastic = self._is_meshtastic_topic(topic)
            is_meshtastic_binary = self._is_meshtastic_binary_topic(topic)
            
            # If we have a decoder and this is a Meshtastic topic, try to decode the message
            decoded_message = None
            if is_meshtastic and self.decoder:
                try:
                    decoded_message = self.decoder.decode_message(topic, payload)
                    
                    # Format output based on preferences
                    if self.output_format == "json":
                        # JSON format (pretty-printed)
                        output = decoded_message.to_json(pretty=True)
                    elif self.output_format == "raw":
                        # Raw format - just display size and preview
                        output = f"Raw message on {topic}: size={len(payload)} bytes"
                    else:
                        # Text format (default)
                        output = decoded_message.get_text()
                    
                    # Write to output
                    self._write_output(output)
                    
                    # Relay the message if enabled
                    if self.relay_enabled:
                        self.relay_message(decoded_message, topic)
                    
                    return  # Processed by decoder
                    
                except Exception as e:
                    self.logger.error(f"Error decoding Meshtastic message: {e}")
                    # Fall back to standard handling
            
            # Handle regular messages (or Meshtastic messages if decoder failed)
            if isinstance(payload, bytes):
                try:
                    # Attempt to decode as UTF-8 text
                    text = payload.decode('utf-8')
                    # Try to parse as JSON if it looks like JSON
                    if text.strip().startswith('{') or text.strip().startswith('['):
                        try:
                            json_data = json.loads(text)
                            
                            if self.output_format == "json":
                                # Format as JSON string
                                output = json.dumps(json_data, indent=2)
                            else:
                                # Basic summary
                                output = f"Received JSON message on {topic}: {json_data}"
                            
                            self._write_output(output)
                            
                            # Relay the message if enabled
                            if self.relay_enabled:
                                self.relay_message(json_data, topic)
                                
                            return
                        except json.JSONDecodeError:
                            # Not JSON, treat as text
                            pass
                    
                    # Log as text
                    output = f"Received text message on {topic}: {text}"
                    self._write_output(output)
                    
                    # Relay the message if enabled
                    if self.relay_enabled:
                        self.relay_message(text, topic)
                        
                except UnicodeDecodeError:
                    # Binary data that's not UTF-8 text
                    if is_meshtastic_binary:
                        # Format Meshtastic binary data
                        formatted = self._format_meshtastic_binary(payload, topic)
                        output = (
                            f"Received Meshtastic binary message on {topic}: "
                            f"size={formatted['size']} bytes, preview={formatted['hex_preview']}"
                        )
                        self._write_output(output)
                        
                        # Relay the message if enabled
                        if self.relay_enabled:
                            self.relay_message(formatted, topic)
                            
                    else:
                        # Generic binary data
                        binary_info = f"<binary data, length: {len(payload)} bytes>"
                        if len(payload) > 0:
                            # Show hex for first few bytes for debugging
                            hex_preview = payload.hex()[:20]
                            if len(payload) > 10:
                                hex_preview += "..."
                            binary_info += f" hex: {hex_preview}"
                            
                        output = f"Received binary message on {topic}: {binary_info}"
                        self._write_output(output)
                        
                        # Relay the message if enabled
                        if self.relay_enabled:
                            self.relay_message(payload, topic)
            else:
                # Non-bytes payload (shouldn't typically happen with MQTT)
                output = f"Received message on {topic}: {payload}"
                self._write_output(output)
                
                # Relay the message if enabled
                if self.relay_enabled:
                    self.relay_message(payload, topic)
                    
        except Exception as e:
            self.logger.error(f"Error processing message: {e}")
            # Log error but don't crash

    def _on_disconnect(self, client, userdata, rc):
        self.connected = False
        if rc != 0:
            self.logger.warning(f"Unexpected disconnection from broker: {rc}")
        else:
            self.logger.info("Disconnected from broker")

def parse_args(args=None):
    parser = argparse.ArgumentParser(description="Meshtastic MQTT CLI")
    parser.add_argument("mode", choices=["send", "receive"], help="Operation mode", nargs="?")
    parser.add_argument("-b", "--broker", default="localhost", help="MQTT broker address")
    parser.add_argument("-p", "--port", type=int, default=1883, help="MQTT broker port")
    parser.add_argument("-t", "--topic", required=True, help="MQTT topic or topics (comma-separated for multiple topics in receive mode)")
    parser.add_argument("-q", "--qos", type=int, choices=[0, 1, 2], default=0, help="MQTT QoS level")
    parser.add_argument("-r", "--retain", action="store_true", help="MQTT retain flag")
    parser.add_argument("-l", "--log-level", choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"], 
                      default="INFO", help="Set the logging level")
    parser.add_argument("-m", "--message", help="Message to publish (required for send mode)")
    
    # Phase 3 additions for message decryption and relay
    parser.add_argument("-d", "--decrypt", action="store_true", help="Enable message decryption")
    parser.add_argument("-k", "--channel-key", help="Channel key (PSK) for message decryption")
    parser.add_argument("--keyfile", help="File containing the channel key")
    parser.add_argument("-o", "--output-format", choices=["text", "json", "raw"], default="text", 
                      help="Output format for received messages")
    parser.add_argument("-f", "--output-file", help="Write output to file instead of stdout")
    parser.add_argument("--relay", action="store_true", help="Enable message relay")
    parser.add_argument("--relay-topic", help="Topic for relaying messages (can include format placeholders like {node_id})")
    
    parsed_args = parser.parse_args(args)
    
    # Check if mode is provided
    if parsed_args.mode is None:
        parser.error("Operation mode is required: choose 'send' or 'receive'")
    
    # Validate that message is provided in send mode
    if parsed_args.mode == "send" and not parsed_args.message:
        parser.error("--message is required when mode is 'send'")
    
    # Validate relay settings
    if parsed_args.relay and not parsed_args.relay_topic:
        parser.error("--relay-topic is required when --relay is enabled")
    
    # Validate decryption settings
    if parsed_args.decrypt and not (parsed_args.channel_key or parsed_args.keyfile):
        parser.error("Either --channel-key or --keyfile must be provided for decryption")
    
    return parsed_args

def main():
    args = parse_args()
    # Use the log level from command line arguments
    log_level = getattr(logging, args.log_level)
    logging.basicConfig(level=log_level)
    logger = logging.getLogger(__name__)

    # Load channel key if specified
    channel_key = None
    if args.decrypt:
        if not MESHTASTIC_AVAILABLE:
            logger.warning("Meshtastic library not available. Decryption will be limited.")
        
        # Get the channel key
        if args.keyfile:
            channel_key = MessageDecoder.load_key_from_file(args.keyfile)
            if not channel_key:
                logger.error(f"Failed to load key from file: {args.keyfile}")
                return
        elif args.channel_key:
            channel_key = args.channel_key
    
    # Create the message decoder if decryption is enabled
    decoder = None
    if args.decrypt and MessageDecoder:
        decoder = MessageDecoder(channel_key=channel_key)
        logger.info("Message decoder initialized")

    # Create the MQTT client
    client = MQTTClient(
        args.broker, 
        args.port, 
        logger, 
        decoder=decoder,
        relay_enabled=args.relay,
        relay_topic=args.relay_topic,
        output_format=args.output_format,
        output_file=args.output_file
    )
    
    try:
        client.connect()
        
        if args.mode == "send":
            client.send_message(args.topic, args.message, args.qos, args.retain)
        else:  # receive mode
            # Support comma-separated list of topics
            topics = [topic.strip() for topic in args.topic.split(",")]
            for topic in topics:
                client.subscribe(topic, args.qos)
                
            if args.decrypt and decoder:
                logger.info("Decryption enabled. Press Ctrl+C to exit.")
            else:
                logger.info("Press Ctrl+C to exit.")
                
            try:
                # Wait indefinitely
                import time
                while True:
                    time.sleep(1)
            except KeyboardInterrupt:
                logger.info("Operation interrupted by user")
            
    except KeyboardInterrupt:
        logger.info("Operation interrupted by user")
    except Exception as e:
        logger.error(f"Error: {e}")
    finally:
        client.disconnect()

if __name__ == "__main__":
    main() 