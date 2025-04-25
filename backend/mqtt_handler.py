import paho.mqtt.client as mqtt
from paho.mqtt.enums import CallbackAPIVersion
from typing import Dict, List, Callable, Any, Optional
import logging
import json
import time
import ssl
import uuid
import os
from .config import settings

# Import our new modules
from messaging.message_store import MessageStore
from messaging.message_processor import MessageProcessor, FilterChain, TopicFilter, FilterAction
from messaging.message_queue import MessageQueue, FlowController
from messaging.message_delivery import MessageDeliveryTracker, QoSHandler
from monitoring.broker_monitor import MQTTBrokerMonitor

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
        self._mqtt_client = mqtt.Client(
            CallbackAPIVersion.VERSION1,
            client_id=client_id, 
            clean_session=clean_session
        )
        
        # Set callbacks
        self._mqtt_client.on_connect = self._on_connect
        self._mqtt_client.on_disconnect = self._on_disconnect
        self._mqtt_client.on_message = self._on_message
        self._mqtt_client.on_subscribe = self._on_subscribe
        self._mqtt_client.on_unsubscribe = self._on_unsubscribe
        self._mqtt_client.on_publish = self._on_publish
        
        # Additional callbacks for QoS handling
        self._mqtt_client.on_publish = self._on_publish
        self._mqtt_client.on_puback = self._on_puback
        self._mqtt_client.on_pubrec = self._on_pubrec
        self._mqtt_client.on_pubrel = self._on_pubrel
        self._mqtt_client.on_pubcomp = self._on_pubcomp
        
        # Set authentication if provided
        if username and password:
            self._mqtt_client.username_pw_set(username, password)
            
        # Configure TLS if enabled
        if use_tls:
            self._mqtt_client.tls_set(
                certfile=None,
                keyfile=None,
                cert_reqs=ssl.CERT_REQUIRED,
                tls_version=ssl.PROTOCOL_TLS,
                ciphers=None,
            )
        
        # Store subscribed topics
        self.subscribed_topics: Dict[str, int] = {}
        
        # Message callback for external handlers
        self._message_callback: Optional[Callable] = None
        
        # Connection callback for external handlers
        self._connection_callback: Optional[Callable] = None
        
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
            self._mqtt_client.connect(self.broker_host, self.broker_port, keepalive=self.keepalive)
            self._mqtt_client.loop_start()
            
            # Start message queue and flow controller
            self.message_queue.start()
            self.flow_controller.start()
            
            # Add broker to monitor
            self.broker_monitor.add_broker(
                name="primary",
                broker_host=self.broker_host,
                broker_port=self.broker_port,
                mqtt_client=self._mqtt_client
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
            self._mqtt_client.loop_stop()
            self._mqtt_client.disconnect()
            self.connected = False
            logger.info("Disconnected from MQTT broker")
        except Exception as e:
            logger.error(f"Error disconnecting from MQTT broker: {e}")

    def publish(self, topic: str, payload: Any, qos: int = None, retain: bool = False) -> bool:
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
                return False
            
            # Queue the message with priority based on QoS
            priority = max(1, 3 - qos)  # QoS 2 = priority 1, QoS 1 = priority 2, QoS 0 = priority 3
            message_id = self.message_queue.enqueue(
                topic=topic,
                payload=payload,
                qos=qos,
                retain=retain,
                priority=priority
            )
            return True if message_id else False
        
        # Direct publishing if queue is not available
        result = self._publish_direct(topic, payload, qos, retain)
        return result is not None and result.rc == mqtt.MQTT_ERR_SUCCESS

    async def async_publish(self, topic: str, payload: Any, qos: int = None, retain: bool = False) -> bool:
        """Publish a message to a topic asynchronously"""
        # Simply call the synchronous publish method for now
        # In a real implementation, this would be properly async
        return self.publish(topic, payload, qos, retain)

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
                return None

        # Publish the message
        try:
            return self._mqtt_client.publish(topic, payload, qos, retain)
        except Exception as e:
            logger.error(f"Error publishing message: {e}")
            return None

    def _retry_publish(self, topic: str, payload: Any, qos: int, retain: bool, message_id: int):
        """Callback for retrying message publication"""
        try:
            logger.info(f"Retrying message {message_id} on topic {topic}")
            
            # Use the same message ID for retries
            info = self._mqtt_client.publish(topic, payload, qos, retain, message_id)
            
            if info.rc != mqtt.MQTT_ERR_SUCCESS:
                logger.error(f"Failed to retry message {message_id}: {mqtt.error_string(info.rc)}")
        except Exception as e:
            logger.error(f"Error retrying message {message_id}: {e}")

    def subscribe(self, topic: str, qos: int = None):
        """Subscribe to a topic"""
        if qos is None:
            qos = self.default_qos
            
        if not self.connected:
            logger.warning("Not connected to MQTT broker. Attempting to reconnect...")
            self.connect()
            if not self.connected:
                logger.error("Failed to subscribe: Not connected to MQTT broker")
                return None
        
        try:
            result = self._mqtt_client.subscribe(topic, qos)
            if result[0] == mqtt.MQTT_ERR_SUCCESS:
                self.subscribed_topics[topic] = qos
                logger.info(f"Subscribed to topic {topic} with QoS {qos}")
                return True
            else:
                logger.error(f"Failed to subscribe to topic {topic}: {mqtt.error_string(result[0])}")
                return False
        except Exception as e:
            logger.error(f"Error subscribing to topic {topic}: {e}")
            return None

    def unsubscribe(self, topic: str):
        """Unsubscribe from a topic"""
        if not self.connected:
            logger.warning("Not connected to MQTT broker")
            return None
        
        try:
            result = self._mqtt_client.unsubscribe(topic)
            if result[0] == mqtt.MQTT_ERR_SUCCESS and topic in self.subscribed_topics:
                del self.subscribed_topics[topic]
                logger.info(f"Unsubscribed from topic {topic}")
                return True
            else:
                logger.error(f"Failed to unsubscribe from topic {topic}: {mqtt.error_string(result[0])}")
                return False
        except Exception as e:
            logger.error(f"Error unsubscribing from topic {topic}: {e}")
            return None

    def get_subscribed_topics(self) -> List[str]:
        """Get list of subscribed topics"""
        return list(self.subscribed_topics.keys())

    def set_message_callback(self, callback: Callable):
        """Set callback for received messages"""
        self._message_callback = callback

    def set_connection_callback(self, callback: Callable):
        """Set callback for connection status changes"""
        self._connection_callback = callback
        
        # Call the callback immediately with the current status
        if callback and self._main_event_loop:
            import asyncio
            
            async def _run_callback():
                await callback(self.connected)
            
            asyncio.run_coroutine_threadsafe(_run_callback(), self._main_event_loop)

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
        """Process messages stored for offline operation"""
        if not self.persistence_enabled or not self.message_store:
            logger.warning("Message persistence not enabled, cannot process offline messages")
            return 0
        
        if not self.connected:
            logger.warning("Not connected to MQTT broker, cannot process offline messages")
            return 0
        
        # Get pending messages from the store
        pending_messages = self.message_store.get_pending_messages(limit=100)
        
        if not pending_messages:
            return 0
        
        count = 0
        for message in pending_messages:
            try:
                # Attempt to publish the message
                result = self._publish_direct(
                    topic=message.topic,
                    payload=message.payload,
                    qos=message.qos,
                    retain=message.retain
                )
                
                if result and result.rc == mqtt.MQTT_ERR_SUCCESS:
                    # Update message status
                    self.message_store.update_message_status(
                        message_id=message.id,
                        status="delivered"
                    )
                    count += 1
                else:
                    # Increment retry count
                    next_retry = self.message_store.calculate_backoff_time(message.retry_count + 1)
                    self.message_store.update_message_status(
                        message_id=message.id,
                        status="pending",
                        retry_count=message.retry_count + 1,
                        next_retry=next_retry
                    )
            except Exception as e:
                logger.error(f"Error processing offline message {message.id}: {e}")
        
        logger.info(f"Processed {count} of {len(pending_messages)} offline messages")
        return count

    def _handle_connection_failure(self):
        """Handle connection failure with exponential backoff"""
        self.reconnect_attempt_count += 1
        
        # Check if we've reached the maximum number of reconnect attempts
        if self.max_reconnect_attempts is not None and self.reconnect_attempt_count > self.max_reconnect_attempts:
            logger.error(f"Maximum reconnect attempts ({self.max_reconnect_attempts}) reached. Giving up.")
            return
        
        # Calculate backoff time with exponential increase and max limit
        backoff = min(
            self.reconnect_interval * (2 ** (self.reconnect_attempt_count - 1)),
            self.max_reconnect_interval
        )
        
        logger.info(f"Will attempt to reconnect in {backoff} seconds (attempt {self.reconnect_attempt_count})")
        time.sleep(backoff)
        self.connect()

    def _on_connect(self, client, userdata, flags, rc):
        """Callback for when the client connects to the broker"""
        if rc == 0:
            self.connected = True
            self.reconnect_attempt_count = 0  # Reset reconnect counter on successful connection
            logger.info("Connected to MQTT broker")
            
            # Resubscribe to topics on reconnect
            for topic, qos in self.subscribed_topics.items():
                self._mqtt_client.subscribe(topic, qos)
                logger.info(f"Resubscribed to topic {topic}")
            
            # Process any offline messages
            if self.persistence_enabled:
                logger.info("Processing offline messages...")
                self.process_offline_messages()
            
            # Trigger connection callback if set
            if self._connection_callback and self._main_event_loop:
                import asyncio
                
                async def _run_callback():
                    await self._connection_callback(True)
                
                asyncio.run_coroutine_threadsafe(_run_callback(), self._main_event_loop)
        else:
            self.connected = False
            logger.error(f"Failed to connect to MQTT broker with code {rc}: {mqtt.connack_string(rc)}")
            self._handle_connection_failure()

    def _on_disconnect(self, client, userdata, rc):
        """Callback for when the client disconnects from the broker"""
        self.connected = False
        
        # Trigger connection callback if set
        if self._connection_callback and self._main_event_loop:
            import asyncio
            
            async def _run_callback():
                await self._connection_callback(False)
            
            asyncio.run_coroutine_threadsafe(_run_callback(), self._main_event_loop)
        
        if rc != 0:
            logger.warning(f"Unexpected disconnection from MQTT broker with code {rc}")
            current_time = time.time()
            if current_time - self.last_reconnect_attempt > self.reconnect_interval:
                self.last_reconnect_attempt = current_time
                logger.info("Attempting to reconnect...")
                try:
                    self._mqtt_client.reconnect()
                except Exception as e:
                    logger.error(f"Reconnection failed: {e}")
                    self._handle_connection_failure()
        else:
            logger.info("Disconnected from MQTT broker")

    def _on_message(self, client, userdata, msg):
        """Callback for when a message is received from the broker"""
        try:
            logger.debug(f"Received message on topic {msg.topic}: {msg.payload}")
            
            # Process message through filters
            processed = self.message_processor.process_message(msg.topic, msg.payload)
            
            # Skip if message was filtered out
            if processed is None:
                logger.debug(f"Message on topic {msg.topic} filtered out")
                return
            
            # Get processed topic and payload
            processed_topic, processed_payload = processed
            
            # Convert payload to string if it's bytes
            if isinstance(processed_payload, bytes):
                try:
                    processed_payload = processed_payload.decode("utf-8")
                except UnicodeDecodeError:
                    # If not valid UTF-8, keep as bytes
                    pass
            
            # Pass to external callback if set
            if self._message_callback:
                try:
                    self._message_callback(processed_topic, processed_payload)
                except Exception as e:
                    logger.error(f"Error in message callback: {e}")
        except Exception as e:
            logger.error(f"Error processing received message: {e}")

    def _on_subscribe(self, client, userdata, mid, granted_qos):
        """Callback for when the client subscribes to a topic"""
        logger.debug(f"Subscribed with message ID {mid} and QoS {granted_qos}")

    def _on_unsubscribe(self, client, userdata, mid):
        """Callback for when the client unsubscribes from a topic"""
        logger.debug(f"Unsubscribed with message ID {mid}")

    def _on_publish(self, client, userdata, mid):
        """Callback for when a message is published"""
        logger.debug(f"Published message with ID {mid}")
    
    def _on_puback(self, client, userdata, mid):
        """Callback for PUBACK (QoS 1 delivery confirmation)"""
        self.qos_handler.handle_publish_ack(mid)
    
    def _on_pubrec(self, client, userdata, mid):
        """Callback for PUBREC (QoS 2 first part)"""
        self.qos_handler.handle_publish_received(mid)
    
    def _on_pubrel(self, client, userdata, mid):
        """Callback for PUBREL (QoS 2 second part)"""
        # This is typically handled by the paho client automatically
        pass
    
    def _on_pubcomp(self, client, userdata, mid):
        """Callback for PUBCOMP (QoS 2 final part)"""
        self.qos_handler.handle_publish_complete(mid)
    
    def _process_queued_message(self, message):
        """Process a message from the queue"""
        try:
            # Get message details
            topic = message.get('topic')
            payload = message.get('payload')
            qos = message.get('qos', self.default_qos)
            retain = message.get('retain', False)
            message_id = message.get('id')
            
            # Publish message
            result = self._publish_direct(
                topic=topic,
                payload=payload,
                qos=qos,
                retain=retain,
                message_id=message_id
            )
            
            # Track delivery for QoS 1 and 2 messages
            if qos > 0 and result and result.rc == mqtt.MQTT_ERR_SUCCESS:
                self.delivery_tracker.track_message(
                    mid=result.mid,
                    topic=topic,
                    payload=payload,
                    qos=qos,
                    retain=retain,
                    message_id=message_id
                )
            
            return result
        except Exception as e:
            logger.error(f"Error processing queued message: {e}")
            return None
    
    def _on_flow_control_change(self, throttling: bool):
        """Handle flow control state changes"""
        if throttling:
            logger.warning("Flow control activated - queue is near capacity")
        else:
            logger.info("Flow control deactivated - queue has capacity")
