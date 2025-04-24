import time
import uuid
import queue
import threading
import logging
from typing import Dict, List, Any, Optional, Callable
from dataclasses import dataclass
import json

# Configure logger
logger = logging.getLogger(__name__)

@dataclass
class QueueMessage:
    """Class representing a message in the queue"""
    id: str
    topic: str
    payload: Any
    qos: int
    retain: bool
    timestamp: float
    metadata: Dict[str, Any] = None


class MessageQueue:
    """Queue system for handling high volume of MQTT messages"""
    
    def __init__(self, max_size: int = 10000, worker_count: int = 1):
        """Initialize message queue
        
        Args:
            max_size: Maximum number of messages in the queue
            worker_count: Number of worker threads
        """
        self.queue = queue.PriorityQueue(maxsize=max_size)
        self.worker_count = worker_count
        self.workers = []
        self.running = False
        self.stats = {
            "enqueued": 0,
            "dequeued": 0,
            "dropped": 0,
            "rejected": 0,
            "processing_time": 0,
            "processing_count": 0
        }
        self.message_handlers: List[Callable[[QueueMessage], None]] = []
        self._last_stats_time = time.time()
        self._lock = threading.RLock()
    
    def start(self):
        """Start the worker threads"""
        with self._lock:
            if self.running:
                logger.warning("Message queue already running")
                return
            
            self.running = True
            
            for i in range(self.worker_count):
                worker = threading.Thread(
                    target=self._worker_loop,
                    name=f"MessageQueueWorker-{i}",
                    daemon=True
                )
                worker.start()
                self.workers.append(worker)
            
            # Start stats monitoring thread
            stats_thread = threading.Thread(
                target=self._stats_monitor,
                name="MessageQueueStats",
                daemon=True
            )
            stats_thread.start()
            self.workers.append(stats_thread)
            
            logger.info(f"Message queue started with {self.worker_count} workers")
    
    def stop(self):
        """Stop the worker threads"""
        with self._lock:
            if not self.running:
                logger.warning("Message queue already stopped")
                return
            
            self.running = False
            
            # Wait for workers to finish
            for worker in self.workers:
                if worker.is_alive():
                    worker.join(timeout=1.0)
            
            self.workers = []
            logger.info("Message queue stopped")
    
    def enqueue(self, topic: str, payload: Any, qos: int = 0, retain: bool = False, 
               priority: int = 1, metadata: Dict[str, Any] = None) -> Optional[str]:
        """Add a message to the queue
        
        Args:
            topic: Message topic
            payload: Message payload
            qos: Quality of Service level
            retain: Retain flag
            priority: Priority level (lower is higher priority)
            metadata: Additional metadata for the message
            
        Returns:
            str: Message ID if successfully enqueued, None otherwise
        """
        if not self.running:
            logger.warning("Cannot enqueue message: Queue is not running")
            with self._lock:
                self.stats["rejected"] += 1
            return None
        
        # Convert dict/list payload to JSON string
        if isinstance(payload, (dict, list)):
            payload = json.dumps(payload)
        
        # Create message object
        message_id = str(uuid.uuid4())
        message = QueueMessage(
            id=message_id,
            topic=topic,
            payload=payload,
            qos=qos,
            retain=retain,
            timestamp=time.time(),
            metadata=metadata or {}
        )
        
        try:
            # Try to add to the queue with timeout
            self.queue.put((priority, message), block=True, timeout=0.1)
            with self._lock:
                self.stats["enqueued"] += 1
            logger.debug(f"Message {message_id} enqueued on topic {topic} with priority {priority}")
            return message_id
        except queue.Full:
            logger.warning("Message queue is full, dropping message")
            with self._lock:
                self.stats["dropped"] += 1
            return None
    
    def add_message_handler(self, handler: Callable[[QueueMessage], None]):
        """Add a handler for processed messages"""
        with self._lock:
            self.message_handlers.append(handler)
            logger.debug(f"Added message handler: {handler.__name__}")
    
    def remove_message_handler(self, handler: Callable[[QueueMessage], None]):
        """Remove a message handler"""
        with self._lock:
            if handler in self.message_handlers:
                self.message_handlers.remove(handler)
                logger.debug(f"Removed message handler: {handler.__name__}")
    
    def get_stats(self) -> Dict[str, Any]:
        """Get queue statistics"""
        with self._lock:
            stats = dict(self.stats)
            stats["queue_size"] = self.queue.qsize()
            stats["queue_full"] = self.queue.full()
            
            # Calculate average processing time
            if stats["processing_count"] > 0:
                stats["avg_processing_time"] = stats["processing_time"] / stats["processing_count"]
            else:
                stats["avg_processing_time"] = 0
                
            return stats
    
    def clear(self):
        """Clear the queue"""
        if not self.running:
            logger.warning("Cannot clear queue: Queue is not running")
            return
        
        with self._lock:
            while not self.queue.empty():
                try:
                    self.queue.get(block=False)
                    self.queue.task_done()
                except queue.Empty:
                    break
            
            logger.info("Message queue cleared")
    
    def wait_until_empty(self, timeout: Optional[float] = None) -> bool:
        """Wait until the queue is empty
        
        Args:
            timeout: Maximum time to wait in seconds
            
        Returns:
            bool: True if queue is empty, False if timeout occurred
        """
        if not self.running:
            logger.warning("Cannot wait: Queue is not running")
            return True
        
        start_time = time.time()
        while not self.queue.empty():
            if timeout is not None and time.time() - start_time > timeout:
                return False
            time.sleep(0.1)
        
        return True
    
    def _worker_loop(self):
        """Main loop for worker threads"""
        while self.running:
            try:
                # Get message from queue with timeout
                priority, message = self.queue.get(block=True, timeout=0.5)
                
                start_time = time.time()
                self._process_message(message)
                processing_time = time.time() - start_time
                
                # Update stats
                with self._lock:
                    self.stats["dequeued"] += 1
                    self.stats["processing_time"] += processing_time
                    self.stats["processing_count"] += 1
                
                # Mark task as done
                self.queue.task_done()
                
            except queue.Empty:
                # Queue timeout, just continue
                pass
            except Exception as e:
                logger.error(f"Error in message queue worker: {e}")
    
    def _process_message(self, message: QueueMessage):
        """Process a message from the queue"""
        # Call all registered handlers
        handlers = []
        with self._lock:
            handlers = list(self.message_handlers)
        
        if not handlers:
            logger.warning("No message handlers registered")
            return
        
        for handler in handlers:
            try:
                handler(message)
            except Exception as e:
                logger.error(f"Error in message handler {handler.__name__}: {e}")
    
    def _stats_monitor(self):
        """Thread to periodically log and reset stats"""
        log_interval = 60  # Log stats every 60 seconds
        
        while self.running:
            time.sleep(1.0)
            
            current_time = time.time()
            if current_time - self._last_stats_time >= log_interval:
                with self._lock:
                    # Log stats
                    stats = self.get_stats()
                    logger.info(
                        f"Message queue stats: enqueued={stats['enqueued']}, "
                        f"dequeued={stats['dequeued']}, dropped={stats['dropped']}, "
                        f"queue_size={stats['queue_size']}, "
                        f"avg_processing_time={stats['avg_processing_time']:.6f}s"
                    )
                    
                    # Reset some stats
                    self.stats["processing_time"] = 0
                    self.stats["processing_count"] = 0
                    
                    self._last_stats_time = current_time


class FlowController:
    """Controls message flow based on queue size"""
    
    def __init__(self, message_queue: MessageQueue, high_watermark: float = 0.8, 
                low_watermark: float = 0.6, check_interval: float = 1.0):
        """Initialize flow controller
        
        Args:
            message_queue: The MessageQueue to monitor
            high_watermark: Ratio of queue capacity that triggers throttling (0.0-1.0)
            low_watermark: Ratio of queue capacity that disables throttling (0.0-1.0)
            check_interval: How often to check queue size in seconds
        """
        self.message_queue = message_queue
        self.high_watermark = high_watermark
        self.low_watermark = low_watermark
        self.check_interval = check_interval
        self.throttling = False
        self.running = False
        self.monitor_thread = None
        self.listeners: List[Callable[[bool], None]] = []
    
    def start(self):
        """Start flow control monitoring"""
        if self.running:
            logger.warning("Flow controller already running")
            return
        
        self.running = True
        self.monitor_thread = threading.Thread(
            target=self._monitor_loop,
            name="FlowControlMonitor",
            daemon=True
        )
        self.monitor_thread.start()
        logger.info("Flow controller started")
    
    def stop(self):
        """Stop flow control monitoring"""
        if not self.running:
            logger.warning("Flow controller already stopped")
            return
        
        self.running = False
        if self.monitor_thread and self.monitor_thread.is_alive():
            self.monitor_thread.join(timeout=1.0)
        self.monitor_thread = None
        logger.info("Flow controller stopped")
    
    def add_flow_listener(self, listener: Callable[[bool], None]):
        """Add a listener for flow control state changes
        
        The listener will be called with a boolean indicating if throttling is active
        """
        self.listeners.append(listener)
    
    def remove_flow_listener(self, listener: Callable[[bool], None]):
        """Remove a flow control listener"""
        if listener in self.listeners:
            self.listeners.remove(listener)
    
    def is_throttling(self) -> bool:
        """Check if flow control is currently throttling"""
        return self.throttling
    
    def _notify_listeners(self, throttling: bool):
        """Notify all listeners of flow control state change"""
        for listener in self.listeners:
            try:
                listener(throttling)
            except Exception as e:
                logger.error(f"Error in flow control listener: {e}")
    
    def _monitor_loop(self):
        """Monitor queue size and adjust flow control"""
        while self.running:
            try:
                stats = self.message_queue.get_stats()
                max_size = self.message_queue.queue.maxsize
                current_size = stats["queue_size"]
                
                # Calculate fill ratio
                fill_ratio = current_size / max_size if max_size > 0 else 0
                
                # Check watermarks
                if not self.throttling and fill_ratio >= self.high_watermark:
                    logger.warning(
                        f"Flow control activated: Queue at {fill_ratio:.1%} capacity "
                        f"(size: {current_size}/{max_size})"
                    )
                    self.throttling = True
                    self._notify_listeners(True)
                    
                elif self.throttling and fill_ratio <= self.low_watermark:
                    logger.info(
                        f"Flow control deactivated: Queue at {fill_ratio:.1%} capacity "
                        f"(size: {current_size}/{max_size})"
                    )
                    self.throttling = False
                    self._notify_listeners(False)
                    
            except Exception as e:
                logger.error(f"Error in flow control monitor: {e}")
                
            # Sleep until next check
            time.sleep(self.check_interval)