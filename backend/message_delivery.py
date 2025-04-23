import time
import uuid
import threading
import logging
from typing import Dict, List, Any, Optional, Callable, Tuple
import json
import random

# Configure logger
logger = logging.getLogger(__name__)

class MessageDeliveryTracker:
    """Tracks message delivery status and handles retries for QoS 1 and 2"""
    
    def __init__(self, max_retries: int = 5, base_delay: float = 1.0, max_delay: float = 60.0,
                retry_jitter: float = 0.2, cleanup_interval: float = 300.0):
        """Initialize message delivery tracker
        
        Args:
            max_retries: Maximum number of retry attempts
            base_delay: Initial delay between retries in seconds
            max_delay: Maximum delay between retries in seconds
            retry_jitter: Random jitter factor to add to retry delay (0.0-1.0)
            cleanup_interval: Interval to clean up old tracking entries in seconds
        """
        self.max_retries = max_retries
        self.base_delay = base_delay
        self.max_delay = max_delay
        self.retry_jitter = retry_jitter
        self.cleanup_interval = cleanup_interval
        
        # Dictionary of in-flight messages by ID
        # { message_id: { sent_time, retry_count, next_retry, qos, topic, payload, ... } }
        self.in_flight: Dict[int, Dict[str, Any]] = {}
        
        # Dictionary of completed message deliveries
        # { message_id: { status, completion_time, retry_count, ... } }
        self.completed: Dict[int, Dict[str, Any]] = {}
        
        # Lock for thread safety
        self._lock = threading.RLock()
        
        # Message ID counter for QoS 1/2 messages
        self._message_id_counter = 0
        
        # Flag to control cleanup thread
        self._running = False
        self._cleanup_thread = None
        
        # Callbacks for publish/retry actions
        self._publish_callback: Optional[Callable[[str, Any, int, bool, int], None]] = None
        
        # Start the cleanup thread
        self._start_cleanup_thread()
    
    def _start_cleanup_thread(self):
        """Start the cleanup thread to remove old tracking entries"""
        self._running = True
        self._cleanup_thread = threading.Thread(
            target=self._cleanup_loop,
            name="MessageDeliveryCleanup",
            daemon=True
        )
        self._cleanup_thread.start()
    
    def stop(self):
        """Stop the cleanup thread"""
        self._running = False
        if self._cleanup_thread and self._cleanup_thread.is_alive():
            self._cleanup_thread.join(timeout=1.0)
    
    def set_publish_callback(self, callback: Callable[[str, Any, int, bool, int], None]):
        """Set callback function for message publishing/retry
        
        The callback should have the signature:
        callback(topic, payload, qos, retain, message_id)
        """
        self._publish_callback = callback
    
    def track_message(self, message_id: int, topic: str, payload: Any, qos: int, retain: bool) -> int:
        """Start tracking a message delivery
        
        Returns:
            int: The message ID
        """
        if qos == 0:
            # No tracking for QoS 0
            return message_id
        
        with self._lock:
            # Record in-flight message
            self.in_flight[message_id] = {
                "message_id": message_id,
                "topic": topic,
                "payload": payload,
                "qos": qos,
                "retain": retain,
                "sent_time": time.time(),
                "retry_count": 0,
                "next_retry": self._calculate_next_retry(0),
                "status": "in_flight"
            }
            
            logger.debug(f"Tracking message {message_id} with QoS {qos}")
            return message_id
    
    def next_message_id(self) -> int:
        """Get the next message ID for QoS 1/2 messages"""
        with self._lock:
            self._message_id_counter = (self._message_id_counter + 1) % 65536
            return self._message_id_counter
    
    def message_ack(self, message_id: int, status: str = "delivered"):
        """Mark a message as acknowledged"""
        with self._lock:
            if message_id in self.in_flight:
                msg_info = self.in_flight.pop(message_id)
                
                # Record completion
                self.completed[message_id] = {
                    "message_id": message_id,
                    "status": status,
                    "completion_time": time.time(),
                    "retry_count": msg_info["retry_count"],
                    "topic": msg_info["topic"],
                    "qos": msg_info["qos"]
                }
                
                logger.debug(f"Message {message_id} marked as {status}")
    
    def message_received(self, message_id: int):
        """Handle received message (for QoS 2)"""
        # For QoS 2: after receiving PUBREC, send PUBREL
        # This would be called when PUBREC is received
        with self._lock:
            if message_id in self.in_flight:
                msg_info = self.in_flight[message_id]
                msg_info["status"] = "received"
                logger.debug(f"Message {message_id} PUBREC received, sending PUBREL")
                
                # Implementation would call the MQTT client's pubrel method here
                # This is typically handled by the MQTT client library, so we
                # don't need a specific action here
    
    def get_pending_retries(self) -> List[Dict[str, Any]]:
        """Get list of messages that need to be retried"""
        current_time = time.time()
        pending_retries = []
        
        with self._lock:
            for message_id, msg_info in list(self.in_flight.items()):
                # Check if it's time to retry
                if (msg_info["next_retry"] <= current_time and 
                    msg_info["retry_count"] < self.max_retries):
                    # Increment retry count and update next retry time
                    msg_info["retry_count"] += 1
                    msg_info["next_retry"] = self._calculate_next_retry(msg_info["retry_count"])
                    
                    pending_retries.append(dict(msg_info))
        
        return pending_retries
    
    def process_retries(self):
        """Process pending message retries"""
        retries = self.get_pending_retries()
        
        if not retries:
            return
        
        if not self._publish_callback:
            logger.warning("No publish callback set for message retries")
            return
        
        for msg_info in retries:
            try:
                logger.info(
                    f"Retrying message {msg_info['message_id']} on topic {msg_info['topic']} "
                    f"(attempt {msg_info['retry_count']} of {self.max_retries})"
                )
                
                # Call publish callback to resend the message
                self._publish_callback(
                    msg_info["topic"],
                    msg_info["payload"],
                    msg_info["qos"],
                    msg_info["retain"],
                    msg_info["message_id"]
                )
            except Exception as e:
                logger.error(f"Error retrying message {msg_info['message_id']}: {e}")
    
    def check_expired_messages(self):
        """Check for messages that have exceeded max retries"""
        with self._lock:
            for message_id, msg_info in list(self.in_flight.items()):
                if msg_info["retry_count"] >= self.max_retries:
                    # Move to completed with failed status
                    self.completed[message_id] = {
                        "message_id": message_id,
                        "status": "failed",
                        "completion_time": time.time(),
                        "retry_count": msg_info["retry_count"],
                        "topic": msg_info["topic"],
                        "qos": msg_info["qos"]
                    }
                    
                    del self.in_flight[message_id]
                    logger.warning(
                        f"Message {message_id} on topic {msg_info['topic']} failed "
                        f"after {msg_info['retry_count']} retry attempts"
                    )
    
    def get_stats(self) -> Dict[str, Any]:
        """Get delivery statistics"""
        with self._lock:
            in_flight_count = len(self.in_flight)
            completed_count = len(self.completed)
            
            # Count by status
            status_counts = {"delivered": 0, "failed": 0}
            for msg_info in self.completed.values():
                status = msg_info.get("status", "unknown")
                if status in status_counts:
                    status_counts[status] += 1
                else:
                    status_counts[status] = 1
            
            return {
                "in_flight": in_flight_count,
                "completed": completed_count,
                "delivered": status_counts.get("delivered", 0),
                "failed": status_counts.get("failed", 0)
            }
    
    def _calculate_next_retry(self, retry_count: int) -> float:
        """Calculate the next retry time using exponential backoff with jitter"""
        # Exponential backoff: base_delay * 2^retry_count
        delay = min(self.base_delay * (2 ** retry_count), self.max_delay)
        
        # Add jitter
        jitter_amount = delay * self.retry_jitter
        delay += random.uniform(-jitter_amount, jitter_amount)
        
        return time.time() + max(delay, 0.1)  # Ensure positive delay
    
    def _cleanup_loop(self):
        """Cleanup thread to remove old tracking entries"""
        while self._running:
            time.sleep(self.cleanup_interval)
            self._cleanup_old_entries()
            self.check_expired_messages()
            self.process_retries()
    
    def _cleanup_old_entries(self, max_age: float = 3600.0):
        """Remove old completed entries"""
        with self._lock:
            current_time = time.time()
            cutoff_time = current_time - max_age
            
            # Remove old completed entries
            old_entries = [
                msg_id for msg_id, info in self.completed.items()
                if info.get("completion_time", 0) < cutoff_time
            ]
            
            for msg_id in old_entries:
                del self.completed[msg_id]
            
            if old_entries:
                logger.debug(f"Cleaned up {len(old_entries)} old delivery tracking entries")


class QoSHandler:
    """Handles MQTT QoS logic"""
    
    def __init__(self, delivery_tracker: MessageDeliveryTracker):
        """Initialize QoS handler
        
        Args:
            delivery_tracker: Message delivery tracker instance
        """
        self.delivery_tracker = delivery_tracker
        
        # Map of QoS levels to their names for logging
        self.qos_names = {
            0: "At most once",
            1: "At least once",
            2: "Exactly once"
        }
    
    def prepare_publish(self, topic: str, payload: Any, qos: int, retain: bool) -> Tuple[int, Any]:
        """Prepare a message for publishing with appropriate QoS handling
        
        Args:
            topic: Message topic
            payload: Message payload
            qos: QoS level (0, 1, or 2)
            retain: Retain flag
            
        Returns:
            Tuple[int, Any]: Message ID and payload
        """
        # Validate QoS level
        if qos not in (0, 1, 2):
            logger.warning(f"Invalid QoS level {qos}, defaulting to 0")
            qos = 0
        
        # Log QoS level
        logger.debug(f"Preparing message for topic {topic} with QoS {qos} ({self.qos_names.get(qos, 'Unknown')})")
        
        # For QoS 0, no tracking needed
        if qos == 0:
            return 0, payload
        
        # For QoS 1 and 2, generate message ID and track the message
        message_id = self.delivery_tracker.next_message_id()
        self.delivery_tracker.track_message(message_id, topic, payload, qos, retain)
        
        return message_id, payload
    
    def handle_publish_ack(self, message_id: int):
        """Handle PUBACK message (QoS 1 acknowledgment)"""
        logger.debug(f"Received PUBACK for message {message_id}")
        self.delivery_tracker.message_ack(message_id, "delivered")
    
    def handle_publish_received(self, message_id: int):
        """Handle PUBREC message (first part of QoS 2 flow)"""
        logger.debug(f"Received PUBREC for message {message_id}")
        self.delivery_tracker.message_received(message_id)
        # PUBREL will be sent by MQTT client
    
    def handle_publish_complete(self, message_id: int):
        """Handle PUBCOMP message (final part of QoS 2 flow)"""
        logger.debug(f"Received PUBCOMP for message {message_id}")
        self.delivery_tracker.message_ack(message_id, "delivered") 