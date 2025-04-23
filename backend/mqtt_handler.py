import paho.mqtt.client as mqtt
from typing import Dict, List, Callable, Any, Optional
import logging
import json
import time

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


class MQTTHandler:
    """Handler for MQTT operations"""

    def __init__(
        self,
        broker_host: str = "localhost",
        broker_port: int = 1883,
        client_id: str = "fastapi_client",
        username: Optional[str] = None,
        password: Optional[str] = None,
        clean_session: bool = True,
    ):
        self.broker_host = broker_host
        self.broker_port = broker_port
        self.client_id = client_id
        self.username = username
        self.password = password
        self.clean_session = clean_session
        
        # Initialize client
        self.client = mqtt.Client(client_id=client_id, clean_session=clean_session)
        
        # Set callbacks
        self.client.on_connect = self._on_connect
        self.client.on_disconnect = self._on_disconnect
        self.client.on_message = self._on_message
        self.client.on_subscribe = self._on_subscribe
        self.client.on_unsubscribe = self._on_unsubscribe
        self.client.on_publish = self._on_publish
        
        # Set authentication if provided
        if username and password:
            self.client.username_pw_set(username, password)
        
        # Store subscribed topics
        self.subscribed_topics: Dict[str, int] = {}
        
        # Message callback for external handlers
        self.message_callback: Optional[Callable] = None
        
        # Connection status
        self.connected = False
        self.last_reconnect_attempt = 0
        self.reconnect_interval = 5  # seconds

    def connect(self):
        """Connect to MQTT broker"""
        try:
            self.client.connect(self.broker_host, self.broker_port)
            self.client.loop_start()
            logger.info(f"Connected to MQTT broker at {self.broker_host}:{self.broker_port}")
        except Exception as e:
            logger.error(f"Failed to connect to MQTT broker: {e}")
            self.connected = False

    def disconnect(self):
        """Disconnect from MQTT broker"""
        self.client.loop_stop()
        self.client.disconnect()
        self.connected = False
        logger.info("Disconnected from MQTT broker")

    def publish(self, topic: str, payload: Any, qos: int = 0, retain: bool = False):
        """Publish a message to a topic"""
        if isinstance(payload, dict) or isinstance(payload, list):
            payload = json.dumps(payload)
        
        info = self.client.publish(topic, payload, qos, retain)
        logger.debug(f"Published message to {topic}: {payload}, QoS {qos}, Retain {retain}")
        return info

    def subscribe(self, topic: str, qos: int = 0):
        """Subscribe to a topic"""
        result = self.client.subscribe(topic, qos)
        if result[0] == mqtt.MQTT_ERR_SUCCESS:
            self.subscribed_topics[topic] = qos
            logger.info(f"Subscribed to topic {topic} with QoS {qos}")
        else:
            logger.error(f"Failed to subscribe to topic {topic}")
        return result

    def unsubscribe(self, topic: str):
        """Unsubscribe from a topic"""
        result = self.client.unsubscribe(topic)
        if result[0] == mqtt.MQTT_ERR_SUCCESS and topic in self.subscribed_topics:
            del self.subscribed_topics[topic]
            logger.info(f"Unsubscribed from topic {topic}")
        else:
            logger.error(f"Failed to unsubscribe from topic {topic}")
        return result

    def get_subscribed_topics(self) -> List[str]:
        """Get list of subscribed topics"""
        return list(self.subscribed_topics.keys())

    def set_message_callback(self, callback: Callable):
        """Set callback for received messages"""
        self.message_callback = callback

    def is_connected(self) -> bool:
        """Check if connected to MQTT broker"""
        return self.connected

    def _on_connect(self, client, userdata, flags, rc):
        """Callback for when the client connects to the broker"""
        if rc == 0:
            self.connected = True
            logger.info("Connected to MQTT broker")
            
            # Resubscribe to topics on reconnect
            for topic, qos in self.subscribed_topics.items():
                self.client.subscribe(topic, qos)
                logger.info(f"Resubscribed to topic {topic}")
        else:
            self.connected = False
            logger.error(f"Failed to connect to MQTT broker with code {rc}")

    def _on_disconnect(self, client, userdata, rc):
        """Callback for when the client disconnects from the broker"""
        self.connected = False
        if rc != 0:
            logger.warning(f"Unexpected disconnection from MQTT broker with code {rc}")
            current_time = time.time()
            if current_time - self.last_reconnect_attempt > self.reconnect_interval:
                self.last_reconnect_attempt = current_time
                logger.info("Attempting to reconnect...")
                try:
                    self.client.reconnect()
                except Exception as e:
                    logger.error(f"Reconnection failed: {e}")
        else:
            logger.info("Disconnected from MQTT broker")

    def _on_message(self, client, userdata, msg):
        """Callback for when a message is received from the broker"""
        logger.debug(f"Received message on topic {msg.topic}: {msg.payload}")
        if self.message_callback:
            import asyncio
            loop = asyncio.get_event_loop()
            loop.create_task(self.message_callback(msg.topic, msg.payload))

    def _on_subscribe(self, client, userdata, mid, granted_qos):
        """Callback for when the client subscribes to a topic"""
        logger.debug(f"Subscribed with message ID {mid} and QoS {granted_qos}")

    def _on_unsubscribe(self, client, userdata, mid):
        """Callback for when the client unsubscribes from a topic"""
        logger.debug(f"Unsubscribed with message ID {mid}")

    def _on_publish(self, client, userdata, mid):
        """Callback for when a message is published"""
        logger.debug(f"Published message with ID {mid}")
