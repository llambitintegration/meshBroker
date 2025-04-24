import time
import asyncio
import logging
import statistics
import random
import string
import json
from typing import Dict, List, Any, Optional
from dataclasses import dataclass, field
import paho.mqtt.client as mqtt
import threading

# Configure logger
logger = logging.getLogger(__name__)

@dataclass
class BenchmarkResult:
    """Result of a benchmark run"""
    message_count: int = 0
    message_size: int = 0
    start_time: float = 0.0
    end_time: float = 0.0
    successful: int = 0
    failed: int = 0
    latencies: List[float] = field(default_factory=list)
    errors: List[str] = field(default_factory=list)
    qos: int = 0
    
    @property
    def duration(self) -> float:
        """Total duration in seconds"""
        return self.end_time - self.start_time
    
    @property
    def messages_per_second(self) -> float:
        """Calculate messages per second"""
        if self.duration == 0:
            return 0
        return self.successful / self.duration
    
    @property
    def avg_latency(self) -> float:
        """Average latency in ms"""
        if not self.latencies:
            return 0
        return statistics.mean(self.latencies) * 1000  # Convert to ms
    
    @property
    def p95_latency(self) -> float:
        """95th percentile latency in ms"""
        if not self.latencies:
            return 0
        return statistics.quantiles(self.latencies, n=20)[-1] * 1000  # Convert to ms
    
    @property
    def min_latency(self) -> float:
        """Minimum latency in ms"""
        if not self.latencies:
            return 0
        return min(self.latencies) * 1000  # Convert to ms
    
    @property
    def max_latency(self) -> float:
        """Maximum latency in ms"""
        if not self.latencies:
            return 0
        return max(self.latencies) * 1000  # Convert to ms
    
    @property
    def throughput_bytes_per_sec(self) -> float:
        """Calculate throughput in bytes per second"""
        if self.duration == 0:
            return 0
        return (self.successful * self.message_size) / self.duration
    
    @property
    def success_rate(self) -> float:
        """Calculate success rate as percentage"""
        total = self.successful + self.failed
        if total == 0:
            return 0
        return (self.successful / total) * 100
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary"""
        return {
            "message_count": self.message_count,
            "message_size": self.message_size,
            "qos": self.qos,
            "duration_seconds": round(self.duration, 3),
            "messages_per_second": round(self.messages_per_second, 2),
            "throughput_kbytes_per_sec": round(self.throughput_bytes_per_sec / 1024, 2),
            "success_rate": round(self.success_rate, 2),
            "successful_messages": self.successful,
            "failed_messages": self.failed,
            "latency_ms": {
                "avg": round(self.avg_latency, 2),
                "min": round(self.min_latency, 2),
                "max": round(self.max_latency, 2),
                "p95": round(self.p95_latency, 2)
            },
            "errors": self.errors[:10]  # Limit to 10 errors
        }


class MQTTBenchmark:
    """MQTT benchmarking tool"""
    
    def __init__(self, 
                 broker_host: str, 
                 broker_port: int, 
                 username: Optional[str] = None,
                 password: Optional[str] = None,
                 use_tls: bool = False):
        """Initialize the MQTT benchmark
        
        Args:
            broker_host: MQTT broker hostname
            broker_port: MQTT broker port
            username: Optional username for authentication
            password: Optional password for authentication  
            use_tls: Whether to use TLS for connection
        """
        self.broker_host = broker_host
        self.broker_port = broker_port
        self.username = username
        self.password = password
        self.use_tls = use_tls
        
        # For tracking received messages
        self._received_messages = 0
        self._received_lock = threading.Lock()
        self._message_times: Dict[str, float] = {}
        self._latencies: List[float] = []
        
        # For benchmark status
        self._benchmark_running = False
        self._errors: List[str] = []
        
    def _generate_payload(self, size: int) -> str:
        """Generate a random string payload of specified size
        
        Args:
            size: Size in bytes
        
        Returns:
            Random string of specified size
        """
        payload_data = {
            "benchmark": True,
            "timestamp": time.time(),
            "message_id": ''.join(random.choices(string.ascii_letters + string.digits, k=8)),
            "data": ''.join(random.choices(string.ascii_letters + string.digits, k=max(0, size - 100)))
        }
        
        payload = json.dumps(payload_data)
        
        # Ensure exact size
        if len(payload) < size:
            # Pad with random data if needed
            padding = ''.join(random.choices(string.ascii_letters, k=size - len(payload)))
            payload_data["padding"] = padding
            payload = json.dumps(payload_data)
        elif len(payload) > size:
            # Truncate if too large
            payload = payload[:size]
            
        return payload
    
    def _on_connect(self, client, userdata, flags, rc):
        """Callback for when the client connects to the broker"""
        if rc != 0:
            error_msg = f"Failed to connect to MQTT broker: {rc}"
            logger.error(error_msg)
            self._errors.append(error_msg)
        else:
            logger.debug("Connected to MQTT broker for benchmark")
            
            # Subscribe to benchmark topic
            client.subscribe("benchmark/response", qos=2)
    
    def _on_message(self, client, userdata, msg):
        """Callback for when a message is received from the broker"""
        try:
            # Parse the message
            payload = json.loads(msg.payload.decode('utf-8'))
            message_id = payload.get("message_id")
            
            if message_id and message_id in self._message_times:
                # Calculate latency
                sent_time = self._message_times[message_id]
                latency = time.time() - sent_time
                
                with self._received_lock:
                    self._latencies.append(latency)
                    self._received_messages += 1
                    
                # Clean up
                del self._message_times[message_id]
                
        except Exception as e:
            error_msg = f"Error processing benchmark response: {str(e)}"
            logger.error(error_msg)
            self._errors.append(error_msg)
    
    def _on_publish(self, client, userdata, mid):
        """Callback for when a message is published"""
        pass  # We're tracking sent messages by ID in the payload
    
    def run_benchmark(self, 
                      message_count: int = 100, 
                      message_size: int = 256,
                      qos: int = 1,
                      echo_topic: bool = True) -> BenchmarkResult:
        """Run an MQTT benchmark test
        
        Args:
            message_count: Number of messages to send
            message_size: Size of each message in bytes
            qos: MQTT QoS level (0, 1, or 2)
            echo_topic: Whether to use an echo topic to measure latency
            
        Returns:
            BenchmarkResult with benchmark data
        """
        if self._benchmark_running:
            raise RuntimeError("Benchmark already running")
            
        self._benchmark_running = True
        result = BenchmarkResult(
            message_count=message_count,
            message_size=message_size,
            qos=qos
        )
        
        # Reset tracking variables
        self._received_messages = 0
        self._message_times = {}
        self._latencies = []
        self._errors = []
        
        # Create MQTT client
        client_id = f"benchmark-{int(time.time())}"
        client = mqtt.Client(client_id=client_id)
        
        # Set callbacks
        client.on_connect = self._on_connect
        client.on_message = self._on_message
        client.on_publish = self._on_publish
        
        # Set authentication if provided
        if self.username and self.password:
            client.username_pw_set(self.username, self.password)
            
        # Set TLS if enabled
        if self.use_tls:
            client.tls_set()
        
        try:
            # Connect to broker
            client.connect(self.broker_host, self.broker_port, keepalive=60)
            
            # Start the loop in a separate thread
            client.loop_start()
            
            # Wait for connection
            time.sleep(1)
            
            # Start benchmark
            result.start_time = time.time()
            
            # Send messages
            successful = 0
            failed = 0
            
            for i in range(message_count):
                try:
                    # Generate message payload
                    payload = self._generate_payload(message_size)
                    message_data = json.loads(payload)
                    message_id = message_data["message_id"]
                    
                    # Record send time
                    self._message_times[message_id] = time.time()
                    
                    # Send to benchmark topic
                    info = client.publish("benchmark/test", payload, qos=qos)
                    
                    if info.rc == mqtt.MQTT_ERR_SUCCESS:
                        successful += 1
                    else:
                        failed += 1
                        error_msg = f"Failed to publish message {i}: {info.rc}"
                        logger.error(error_msg)
                        self._errors.append(error_msg)
                    
                    # If we're sending a lot of messages, avoid flooding
                    if message_count > 100 and i % 100 == 0:
                        time.sleep(0.01)
                        
                except Exception as e:
                    failed += 1
                    error_msg = f"Error sending message {i}: {str(e)}"
                    logger.error(error_msg)
                    self._errors.append(error_msg)
            
            # Wait for all messages to be received if using echo
            if echo_topic:
                # Wait for up to 10 seconds for all messages to be received
                wait_start = time.time()
                while (time.time() - wait_start < 10 and 
                       self._received_messages < successful):
                    time.sleep(0.1)
            
            # End benchmark
            result.end_time = time.time()
            
            # Update result
            result.successful = successful
            result.failed = failed
            result.latencies = self._latencies
            result.errors = self._errors
            
            return result
            
        except Exception as e:
            error_msg = f"Benchmark error: {str(e)}"
            logger.error(error_msg)
            self._errors.append(error_msg)
            
            result.end_time = time.time()
            result.successful = 0
            result.failed = message_count
            result.errors = self._errors
            
            return result
            
        finally:
            try:
                # Clean up
                client.loop_stop()
                client.disconnect()
            except:
                pass
                
            self._benchmark_running = False


class BenchmarkManager:
    """Manager for running and storing benchmark results"""
    
    _instance = None
    
    @classmethod
    def get_instance(cls):
        """Get singleton instance"""
        if cls._instance is None:
            cls._instance = BenchmarkManager()
        return cls._instance
    
    def __init__(self):
        """Initialize benchmark manager"""
        if BenchmarkManager._instance is not None:
            raise RuntimeError("BenchmarkManager is a singleton. Use get_instance()")
            
        self._results: List[Dict[str, Any]] = []
        self._max_results = 20  # Keep only the last 20 results
        self._lock = threading.Lock()
        self._current_benchmark = None
    
    def is_benchmark_running(self) -> bool:
        """Check if a benchmark is currently running"""
        return self._current_benchmark is not None
    
    def run_mqtt_benchmark(self, 
                          broker_host: str, 
                          broker_port: int,
                          message_count: int = 100,
                          message_size: int = 256,
                          qos: int = 1,
                          username: Optional[str] = None,
                          password: Optional[str] = None,
                          use_tls: bool = False) -> Dict[str, Any]:
        """Run an MQTT benchmark
        
        Args:
            broker_host: MQTT broker hostname
            broker_port: MQTT broker port
            message_count: Number of messages to send
            message_size: Size of each message in bytes
            qos: MQTT QoS level (0, 1, or 2)
            username: Optional username for authentication
            password: Optional password for authentication
            use_tls: Whether to use TLS for connection
            
        Returns:
            Dictionary with benchmark results
        """
        if self.is_benchmark_running():
            raise RuntimeError("A benchmark is already running")
            
        logger.info(f"Starting MQTT benchmark: {message_count} messages, {message_size} bytes each, QoS {qos}")
        
        benchmark = MQTTBenchmark(
            broker_host=broker_host,
            broker_port=broker_port,
            username=username,
            password=password,
            use_tls=use_tls
        )
        
        self._current_benchmark = benchmark
        
        try:
            result = benchmark.run_benchmark(
                message_count=message_count,
                message_size=message_size,
                qos=qos
            )
            
            # Convert to dict and add timestamp
            result_dict = result.to_dict()
            result_dict["timestamp"] = time.time()
            
            # Store result
            with self._lock:
                self._results.append(result_dict)
                # Trim if needed
                if len(self._results) > self._max_results:
                    self._results = self._results[-self._max_results:]
            
            logger.info(f"Benchmark complete: {result.successful} messages in {result.duration:.2f}s " +
                      f"({result.messages_per_second:.2f} msg/s)")
            
            return result_dict
            
        finally:
            self._current_benchmark = None
    
    def get_benchmark_results(self) -> List[Dict[str, Any]]:
        """Get all benchmark results"""
        with self._lock:
            return list(self._results)
    
    def get_latest_result(self) -> Optional[Dict[str, Any]]:
        """Get the most recent benchmark result"""
        with self._lock:
            if not self._results:
                return None
            return self._results[-1]
            

# Helper function to get the benchmark manager instance
def get_benchmark_manager() -> BenchmarkManager:
    return BenchmarkManager.get_instance() 