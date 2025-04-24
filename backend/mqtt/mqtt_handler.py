import paho.mqtt.client as mqtt
from typing import Dict, List, Callable, Any, Optional
import logging
import json
import time
import ssl
import uuid
import os
from backend.config import settings

# Import our relocated modules
from backend.messaging.message_store import MessageStore
from backend.messaging.message_processor import MessageProcessor, FilterChain, TopicFilter, FilterAction
from backend.messaging.message_queue import MessageQueue, FlowController
from backend.messaging.message_delivery import MessageDeliveryTracker, QoSHandler
from backend.monitoring.broker_monitor import MQTTBrokerMonitor

# Configure logger
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
        use_tls: bool = False,
        keepalive: int = 60,
        default_qos: int = 0,
        persistence_enabled: bool = True,
        persistence_path: str = "mqtt_data",
        max_queue_size: int = 10000,
        worker_count: int = 2,
        max_retries: int = 5
    ):
        self.broker_host = broker_host
        self.broker_port = broker_port
        self.client_id = client_id
        self.username = username
        self.password = password
        self.clean_session = clean_session
        self.use_tls = use_tls
        self.keepalive = keepalive
        self.default_qos = default_qos
        
        # Store the main event loop for async callbacks
        self._main_event_loop = None
        
        # Create data directory if it doesn't exist
        if persistence_enabled and not os.path.exists(persistence_path):
            os.makedirs(persistence_path)
            logger.info(f"Created persistence directory at {persistence_path}")
        
        # Initialize client
        self.client = mqtt.Client(client_id=client_id, clean_session=clean_session)
        
        # Set callbacks
        self.client.on_connect = self._on_connect
        self.client.on_disconnect = self._on_disconnect
        self.client.on_message = self._on_message
        self.client.on_subscribe = self._on_subscribe
        self.client.on_unsubscribe = self._on_unsubscribe
        self.client.on_publish = self._on_publish
        
        # Additional callbacks for QoS handling
        self.client.on_publish = self._on_publish
        self.client.on_puback = self._on_puback
        self.client.on_pubrec = self._on_pubrec
        self.client.on_pubrel = self._on_pubrel
        self.client.on_pubcomp = self._on_pubcomp
        
        # Set authentication if provided
        if username and password:
            self.client.username_pw_set(username, password)
            
        # Configure TLS if enabled
        if use_tls:
            self.client.tls_set(
                certfile=None,
                keyfile=None,
                cert_reqs=ssl.CERT_REQUIRED,
                tls_version=ssl.PROTOCOL_TLS,
                ciphers=None,
            )
        
        # Store subscribed topics
        self.subscribed_topics: Dict[str, int] = {}
        
        # Message callback for external handlers
        self.message_callback: Optional[Callable] = None
        
        # Connection status
        self.connected = False
        self.last_reconnect_attempt = 0
        self.reconnect_attempt_count = 0
        self.reconnect_interval = 5  # seconds
        self.max_reconnect_interval = 300  # 5 minutes
        self.max_reconnect_attempts = 10  # Set to None for infinite retries
        
        # Initialize message persistence
        self.persistence_enabled = persistence_enabled
        self.persistence_path = persistence_path
        if persistence_enabled:
            db_path = os.path.join(persistence_path, "messages.db")
            self.message_store = MessageStore(db_path=db_path, max_retries=max_retries)
            logger.info(f"Message persistence enabled with database at {db_path}")
        else:
            self.message_store = None
            logger.info("Message persistence disabled")
        
        # Initialize message processor for filtering and transformation
        self.message_processor = MessageProcessor()
        
        # Initialize message queue
        self.message_queue = MessageQueue(max_size=max_queue_size, worker_count=worker_count)
        self.message_queue.add_message_handler(self._process_queued_message)
        
        # Initialize flow controller
        self.flow_controller = FlowController(
            message_queue=self.message_queue,
            high_watermark=0.8,
            low_watermark=0.6
        )
        self.flow_controller.add_flow_listener(self._on_flow_control_change)
        
        # Initialize message delivery tracking
        self.delivery_tracker = MessageDeliveryTracker(max_retries=max_retries)
        self.delivery_tracker.set_publish_callback(self._retry_publish)
        
        # Initialize QoS handler
        self.qos_handler = QoSHandler(self.delivery_tracker)
        
        # Initialize broker monitor
        self.broker_monitor = MQTTBrokerMonitor()

    def set_event_loop(self, loop):
        """Set the main event loop for callbacks"""
        self._main_event_loop = loop
        logger.debug("Main event loop set for MQTT handler")

    def connect(self):
        """Connect to MQTT broker"""
        try:
            # Store the current event loop if we're in the main thread
            import asyncio
            import threading
            if threading.current_thread() is threading.main_thread():
                try:
                    self._main_event_loop = asyncio.get_event_loop()
                    logger.debug("Stored main event loop for callbacks")
                except RuntimeError:
                    logger.warning("No running event loop found during connect")
            
            logger.info(f"Connecting to MQTT broker at {self.broker_host}:{self.broker_port}")
            self.client.connect(self.broker_host, self.broker_port, keepalive=self.keepalive)
            self.client.loop_start()
            
            # Start message queue and flow controller
            self.message_queue.start()
            self.flow_controller.start()
            
            # Add broker to monitor
            self.broker_monitor.add_broker(
                name="primary",
                broker_host=self.broker_host,
                broker_port=self.broker_port,
                mqtt_client=self.client
            )
        except Exception as e:
            logger.error(f"Failed to connect to MQTT broker: {e}")
            self.connected = False
            self._handle_connection_failure()

    def disconnect(self):
        """Disconnect from MQTT broker"""
        try:
            # Stop message queue and flow controller
            self.flow_controller.stop()
            self.message_queue.stop()
            self.delivery_tracker.stop()
            
            # Stop broker monitoring
            self.broker_monitor.remove_broker("primary")
            
            # Disconnect MQTT client
            self.client.loop_stop()
            self.client.disconnect()
            self.connected = False
            logger.info("Disconnected from MQTT broker")
        except Exception as e:
            logger.error(f"Error disconnecting from MQTT broker: {e}")

    def publish(self, topic: str, payload: Any, qos: int = None, retain: bool = False) -> Any:
        """Publish a message to a topic"""
        if qos is None:
            qos = self.default_qos
            
        if isinstance(payload, dict) or isinstance(payload, list):
            payload = json.dumps(payload)
        
        # Use the message queue system for publishing
        if self.message_queue.running:
            # Check flow control first
            if self.flow_controller.is_throttling() and qos == 0:
                logger.warning("Flow control active, dropping QoS 0 message")
                return None
            
            # Queue the message with priority based on QoS
            priority = max(1, 3 - qos)  # QoS 2 = priority 1, QoS 1 = priority 2, QoS 0 = priority 3
            message_id = self.message_queue.enqueue(
                topic=topic,
                payload=payload,
                qos=qos,
                retain=retain,
                priority=priority
            )
            return message_id
        
        # Direct publishing if queue is not available
        return self._publish_direct(topic, payload, qos, retain)

    def _publish_direct(self, topic: str, payload: Any, qos: int, retain: bool, message_id: int = None) -> Any:
        """Directly publish a message to the broker"""
        if not self.connected:
            logger.warning("Not connected to MQTT broker. Attempting to reconnect...")
            self.connect()
            if not self.connected:
                logger.error("Failed to publish message: Not connected to MQTT broker")
                
                # Store message if persistence is enabled
                if self.persistence_enabled and self.message_store:
                    store_id = message_id or str(uuid.uuid4())
                    self.message_store.store_message(
                        message_id=store_id,
                        topic=topic,
                        payload=payload,
                        qos=qos,
                        retain=retain
                    )
                    logger.info(f"Message stored for later delivery: {store_id}")
                
                return None
        
        try:
            # Prepare message with QoS handling
            mid, processed_payload = self.qos_handler.prepare_publish(topic, payload, qos, retain)
            
            # Publish the message
            result = self.client.publish(topic, processed_payload, qos=qos, retain=retain)
            
            # Store message if persistence is enabled and not QoS 0
            if self.persistence_enabled and self.message_store and qos > 0:
                self.message_store.store_message(
                    message_id=str(mid),
                    topic=topic,
                    payload=payload,
                    qos=qos,
                    retain=retain
                )
            
            if result.rc != mqtt.MQTT_ERR_SUCCESS:
                logger.error(f"Failed to publish message: {mqtt.error_string(result.rc)}")
                return None
                
            logger.debug(f"Published message to topic {topic} with QoS {qos}")
            return mid
        except Exception as e:
            logger.error(f"Error publishing message: {e}")
            return None
    
    def _retry_publish(self, topic: str, payload: Any, qos: int, retain: bool, message_id: int):
        """Retry publishing a message (used by delivery tracker)"""
        try:
            result = self.client.publish(topic, payload, qos=qos, retain=retain, mid=message_id)
            logger.debug(f"Retrying message {message_id} on topic {topic}")
            return result
        except Exception as e:
            logger.error(f"Error retrying message: {e}")
            return None

    def subscribe(self, topic: str, qos: int = None):
        """Subscribe to a topic"""
        if qos is None:
            qos = self.default_qos
            
        if not self.connected:
            logger.warning("Not connected to MQTT broker")
            return None
        
        try:
            result = self.client.subscribe(topic, qos=qos)
            if result[0] == mqtt.MQTT_ERR_SUCCESS:
                self.subscribed_topics[topic] = qos
                logger.info(f"Subscribed to topic {topic} with QoS {qos}")
            else:
                logger.error(f"Failed to subscribe to topic {topic}: {mqtt.error_string(result[0])}")
            return result
        except Exception as e:
            logger.error(f"Error subscribing to topic {topic}: {e}")
            return None

    def unsubscribe(self, topic: str):
        """Unsubscribe from a topic"""
        if not self.connected:
            logger.warning("Not connected to MQTT broker")
            return None
        
        try:
            result = self.client.unsubscribe(topic)
            if result[0] == mqtt.MQTT_ERR_SUCCESS and topic in self.subscribed_topics:
                del self.subscribed_topics[topic]
                logger.info(f"Unsubscribed from topic {topic}")
            else:
                logger.error(f"Failed to unsubscribe from topic {topic}: {mqtt.error_string(result[0])}")
            return result
        except Exception as e:
            logger.error(f"Error unsubscribing from topic {topic}: {e}")
            return None

    def get_subscribed_topics(self) -> List[str]:
        """Get list of subscribed topics"""
        return list(self.subscribed_topics.keys())

    def set_message_callback(self, callback: Callable):
        """Set callback for received messages"""
        self.message_callback = callback

    def is_connected(self) -> bool:
        """Check if connected to MQTT broker"""
        return self.connected
    
    def add_message_filter(self, topic_pattern: str, filter_chain: FilterChain):
        """Add a message filter chain for a specific topic pattern"""
        self.message_processor.add_filter_chain(topic_pattern, filter_chain)
    
    def add_default_filter(self, filter):
        """Add a filter to the default filter chain"""
        self.message_processor.add_default_filter(filter)
    
    def get_broker_status(self) -> Dict[str, Any]:
        """Get the broker monitoring status"""
        return self.broker_monitor.get_overall_status()
    
    def get_queue_stats(self) -> Dict[str, Any]:
        """Get statistics about the message queue"""
        return self.message_queue.get_stats()
    
    def get_delivery_stats(self) -> Dict[str, Any]:
        """Get statistics about message delivery"""
        return self.delivery_tracker.get_stats()
    
    def process_offline_messages(self):
        """Process messages that were stored while offline"""
        if not self.persistence_enabled or not self.message_store:
            logger.warning("Message persistence not enabled")
            return
        
        if not self.connected:
            logger.warning("Cannot process offline messages: Not connected to MQTT broker")
            return
        
        try:
            # Get pending messages from store
            pending_messages = self.message_store.get_pending_messages(limit=100)
            
            if not pending_messages:
                logger.debug("No offline messages to process")
                return
            
            logger.info(f"Processing {len(pending_messages)} offline messages")
            
            for message in pending_messages:
                # Check if max retries reached
                if message.retry_count >= self.message_store.max_retries:
                    logger.warning(f"Max retries reached for message {message.id}, marking as failed")
                    self.message_store.update_message_status(message.id, "failed")
                    continue
                
                # Try to publish the message
                result = self._publish_direct(
                    topic=message.topic,
                    payload=message.payload,
                    qos=message.qos,
                    retain=message.retain,
                    message_id=int(message.id) if message.id.isdigit() else None
                )
                
                if result:
                    # Message published successfully
                    self.message_store.update_message_status(message.id, "delivered")
                else:
                    # Update retry count and next retry time
                    next_retry = self.message_store.calculate_backoff_time(message.retry_count + 1)
                    self.message_store.update_message_status(
                        message.id,
                        "pending",
                        retry_count=message.retry_count + 1,
                        next_retry=next_retry
                    )
        except Exception as e:
            logger.error(f"Error processing offline messages: {e}")
    
    def _handle_connection_failure(self):
        """Handle connection failure with retry logic"""
        now = time.time()
        
        # Check if we should retry
        if self.max_reconnect_attempts is not None and self.reconnect_attempt_count >= self.max_reconnect_attempts:
            logger.error(f"Maximum reconnect attempts ({self.max_reconnect_attempts}) reached")
            return
            
        # Calculate backoff time
        if self.reconnect_attempt_count > 0:
            self.reconnect_interval = min(
                self.reconnect_interval * 2,
                self.max_reconnect_interval
            )
        
        self.last_reconnect_attempt = now
        self.reconnect_attempt_count += 1
        
        logger.info(
            f"Will attempt to reconnect in {self.reconnect_interval} seconds "
            f"(attempt {self.reconnect_attempt_count})"
        )
    
    def _on_connect(self, client, userdata, flags, rc):
        """Callback for when client connects to broker"""
        if rc == 0:
            logger.info("Connected to MQTT broker")
            self.connected = True
            self.reconnect_attempt_count = 0
            self.reconnect_interval = 5  # Reset backoff
            
            # Resubscribe to topics
            for topic, qos in self.subscribed_topics.items():
                logger.info(f"Resubscribing to topic {topic}")
                client.subscribe(topic, qos=qos)
                
            # Process any stored offline messages
            if self.persistence_enabled and self.message_store:
                self.process_offline_messages()
        else:
            logger.error(f"Failed to connect to MQTT broker: {mqtt.connack_string(rc)}")
            self.connected = False
            self._handle_connection_failure()
    
    def _on_disconnect(self, client, userdata, rc):
        """Callback for when client disconnects from broker"""
        if rc == 0:
            logger.info("Disconnected from MQTT broker")
        else:
            logger.warning(f"Unexpected disconnect from MQTT broker: {rc}")
            
        self.connected = False
        
        # Try to reconnect if it was an unexpected disconnect
        if rc != 0:
            now = time.time()
            # Only attempt reconnect if sufficient time has passed since last attempt
            if now - self.last_reconnect_attempt > self.reconnect_interval:
                logger.info("Attempting to reconnect after unexpected disconnect")
                self.connect()
    
    def _on_message(self, client, userdata, msg):
        """Callback for when a message is received from the broker"""
        try:
            topic = msg.topic
            payload = msg.payload
            qos = msg.qos
            
            # Try to parse JSON payload
            if isinstance(payload, bytes):
                try:
                    payload = payload.decode("utf-8")
                    # Try to parse as JSON
                    try:
                        payload = json.loads(payload)
                    except json.JSONDecodeError:
                        # Not JSON, keep as string
                        pass
                except UnicodeDecodeError:
                    # Binary data, keep as bytes
                    pass
            
            # Process message through filters
            processed = self.message_processor.process_message(topic, payload)
            if processed is None:
                return
            
            topic, payload = processed
            
            logger.debug(f"Received message on topic {topic} with QoS {qos}")
            
            # Call the external message callback if set
            if self.message_callback:
                if self._main_event_loop:
                    # If we have an event loop, run the callback in it
                    import asyncio
                    
                    async def _run_callback():
                        await self.message_callback(topic, payload)
                    
                    asyncio.run_coroutine_threadsafe(_run_callback(), self._main_event_loop)
                else:
                    # Otherwise call directly (non-async)
                    self.message_callback(topic, payload)
        except Exception as e:
            logger.error(f"Error handling received message: {e}")
    
    def _on_subscribe(self, client, userdata, mid, granted_qos):
        """Callback for when a subscription is confirmed"""
        logger.debug(f"Subscription confirmed with QoS {granted_qos}")
    
    def _on_unsubscribe(self, client, userdata, mid):
        """Callback for when an unsubscribe is confirmed"""
        logger.debug("Unsubscribe confirmed")
    
    def _on_publish(self, client, userdata, mid):
        """Callback for when a message is published"""
        logger.debug(f"Message {mid} published")
    
    def _on_puback(self, client, userdata, mid):
        """Callback for QoS 1 PUBACK received"""
        logger.debug(f"PUBACK received for message {mid}")
        self.qos_handler.handle_publish_ack(mid)
    
    def _on_pubrec(self, client, userdata, mid):
        """Callback for QoS 2 PUBREC received"""
        logger.debug(f"PUBREC received for message {mid}")
        self.qos_handler.handle_publish_received(mid)
    
    def _on_pubrel(self, client, userdata, mid):
        """Callback for QoS 2 PUBREL received"""
        logger.debug(f"PUBREL received for message {mid}")
    
    def _on_pubcomp(self, client, userdata, mid):
        """Callback for QoS 2 PUBCOMP received"""
        logger.debug(f"PUBCOMP received for message {mid}")
        self.qos_handler.handle_publish_complete(mid)
    
    def _process_queued_message(self, message):
        """Process a message from the queue"""
        try:
            logger.debug(f"Processing queued message {message.id} on topic {message.topic}")
            
            # Publish the message
            result = self._publish_direct(
                topic=message.topic,
                payload=message.payload,
                qos=message.qos,
                retain=message.retain
            )
            
            # If persistence is enabled, update message status
            if self.persistence_enabled and self.message_store and result:
                # Store may already have it from _publish_direct, this is a safeguard
                self.message_store.store_message(
                    message_id=str(result),
                    topic=message.topic,
                    payload=message.payload,
                    qos=message.qos,
                    retain=message.retain,
                    status="pending"
                )
                
        except Exception as e:
            logger.error(f"Error processing queued message: {e}")
    
    def _on_flow_control_change(self, throttling: bool):
        """Callback for flow control state changes"""
        if throttling:
            logger.warning("Flow control activated - throttling message publishing")
        else:
            logger.info("Flow control deactivated - normal message publishing resumed")