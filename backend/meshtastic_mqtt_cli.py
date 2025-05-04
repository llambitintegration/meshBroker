import argparse
import logging
import paho.mqtt.client as mqtt
from typing import Optional
import socket

class MQTTClient:
    def __init__(self, broker: str, port: int, logger: Optional[logging.Logger] = None):
        if not broker:
            raise ValueError("Broker address cannot be empty")
        if not (0 < port < 65536):
            raise ValueError("Port must be between 1 and 65535")
            
        self.broker = broker
        self.port = port
        self.connected = False
        self.logger = logger or logging.getLogger(__name__)
        self._client = mqtt.Client()
        
        # Set up callbacks
        self._client.on_connect = self._on_connect
        self._client.on_message = self._on_message
        self._client.on_disconnect = self._on_disconnect

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
            payload = message.payload.decode('utf-8')
            self.logger.info(f"Received message on {message.topic}: {payload}")
        except UnicodeDecodeError as e:
            self.logger.error(f"Failed to decode message payload: {e}")
            raise
        except Exception as e:
            self.logger.error(f"Error processing message: {e}")
            raise

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
    
    parsed_args = parser.parse_args(args)
    
    # Check if mode is provided
    if parsed_args.mode is None:
        parser.error("Operation mode is required: choose 'send' or 'receive'")
    
    # Validate that message is provided in send mode
    if parsed_args.mode == "send" and not parsed_args.message:
        parser.error("--message is required when mode is 'send'")
    
    return parsed_args

def main():
    args = parse_args()
    # Use the log level from command line arguments
    log_level = getattr(logging, args.log_level)
    logging.basicConfig(level=log_level)
    logger = logging.getLogger(__name__)

    client = MQTTClient(args.broker, args.port, logger)
    
    try:
        client.connect()
        
        if args.mode == "send":
            client.send_message(args.topic, args.message, args.qos, args.retain)
        else:  # receive mode
            # Support comma-separated list of topics
            topics = [topic.strip() for topic in args.topic.split(",")]
            for topic in topics:
                client.subscribe(topic, args.qos)
            input("Press Enter to exit...\n")
            
    except KeyboardInterrupt:
        logger.info("Operation interrupted by user")
    except Exception as e:
        logger.error(f"Error: {e}")
    finally:
        client.disconnect()

if __name__ == "__main__":
    main() 