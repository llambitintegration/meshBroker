import time
import logging
import asyncio
import threading
import psutil
import os
from typing import Dict, List, Any, Optional, Callable
from datetime import datetime, timedelta
from enum import Enum
import json

# Configure logger
logger = logging.getLogger(__name__)

class MetricType(Enum):
    """Types of metrics that can be collected"""
    COUNTER = "counter"
    GAUGE = "gauge"
    HISTOGRAM = "histogram"
    SUMMARY = "summary"

class Metric:
    """Base class for all metrics"""
    def __init__(self, name: str, description: str, labels: Optional[List[str]] = None):
        self.name = name
        self.description = description
        self.labels = labels or []
        self.created = time.time()
        self.updated = self.created
        
    def get_type(self) -> MetricType:
        """Return the metric type"""
        raise NotImplementedError("Subclasses must implement get_type")
        
    def to_dict(self) -> Dict[str, Any]:
        """Convert metric to dictionary representation"""
        return {
            "name": self.name,
            "description": self.description,
            "type": self.get_type().value,
            "created": self.created,
            "updated": self.updated
        }

class Counter(Metric):
    """Counter metric that only increases"""
    def __init__(self, name: str, description: str, labels: Optional[List[str]] = None):
        super().__init__(name, description, labels)
        self.value = 0
        self.labeled_values: Dict[str, int] = {}
        
    def inc(self, amount: int = 1, label_values: Optional[Dict[str, str]] = None):
        """Increment counter by given amount"""
        if label_values and self.labels:
            # Create a label key by joining values in the order of self.labels
            label_key = ":".join([label_values.get(label, "") for label in self.labels])
            if label_key not in self.labeled_values:
                self.labeled_values[label_key] = 0
            self.labeled_values[label_key] += amount
        else:
            self.value += amount
        self.updated = time.time()
        
    def get_type(self) -> MetricType:
        return MetricType.COUNTER
        
    def to_dict(self) -> Dict[str, Any]:
        result = super().to_dict()
        result["value"] = self.value
        if self.labeled_values:
            result["labeled_values"] = self.labeled_values
        return result

class Gauge(Metric):
    """Gauge metric that can go up and down"""
    def __init__(self, name: str, description: str, labels: Optional[List[str]] = None):
        super().__init__(name, description, labels)
        self.value = 0
        self.labeled_values: Dict[str, float] = {}
        
    def set(self, value: float, label_values: Optional[Dict[str, str]] = None):
        """Set gauge to specific value"""
        if label_values and self.labels:
            # Create a label key by joining values in the order of self.labels
            label_key = ":".join([label_values.get(label, "") for label in self.labels])
            self.labeled_values[label_key] = value
        else:
            self.value = value
        self.updated = time.time()
        
    def inc(self, amount: float = 1, label_values: Optional[Dict[str, str]] = None):
        """Increment gauge by amount"""
        if label_values and self.labels:
            label_key = ":".join([label_values.get(label, "") for label in self.labels])
            if label_key not in self.labeled_values:
                self.labeled_values[label_key] = 0
            self.labeled_values[label_key] += amount
        else:
            self.value += amount
        self.updated = time.time()
        
    def dec(self, amount: float = 1, label_values: Optional[Dict[str, str]] = None):
        """Decrement gauge by amount"""
        if label_values and self.labels:
            label_key = ":".join([label_values.get(label, "") for label in self.labels])
            if label_key not in self.labeled_values:
                self.labeled_values[label_key] = 0
            self.labeled_values[label_key] -= amount
        else:
            self.value -= amount
        self.updated = time.time()
        
    def get_type(self) -> MetricType:
        return MetricType.GAUGE
        
    def to_dict(self) -> Dict[str, Any]:
        result = super().to_dict()
        result["value"] = self.value
        if self.labeled_values:
            result["labeled_values"] = self.labeled_values
        return result

class Histogram(Metric):
    """Histogram for measuring distributions of values"""
    def __init__(self, name: str, description: str, labels: Optional[List[str]] = None,
                 buckets: Optional[List[float]] = None):
        super().__init__(name, description, labels)
        self.buckets = buckets or [0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10]
        self.counts = {b: 0 for b in self.buckets}
        self.counts["inf"] = 0  # Always include an "inf" bucket
        self.sum = 0
        self.count = 0
        
        # For labeled histograms
        self.labeled_counts: Dict[str, Dict[str, int]] = {}
        self.labeled_sums: Dict[str, float] = {}
        self.labeled_counts_total: Dict[str, int] = {}
        
    def observe(self, value: float, label_values: Optional[Dict[str, str]] = None):
        """Add an observation to the histogram"""
        if label_values and self.labels:
            label_key = ":".join([label_values.get(label, "") for label in self.labels])
            
            # Initialize labeled buckets if needed
            if label_key not in self.labeled_counts:
                self.labeled_counts[label_key] = {b: 0 for b in self.buckets}
                self.labeled_counts[label_key]["inf"] = 0
                self.labeled_sums[label_key] = 0
                self.labeled_counts_total[label_key] = 0
                
            # Update buckets
            for bucket in self.buckets:
                if value <= bucket:
                    self.labeled_counts[label_key][bucket] += 1
            self.labeled_counts[label_key]["inf"] += 1
            
            # Update sum and count
            self.labeled_sums[label_key] += value
            self.labeled_counts_total[label_key] += 1
        else:
            # Update buckets
            for bucket in self.buckets:
                if value <= bucket:
                    self.counts[bucket] += 1
            self.counts["inf"] += 1
            
            # Update sum and count
            self.sum += value
            self.count += 1
            
        self.updated = time.time()
        
    def get_type(self) -> MetricType:
        return MetricType.HISTOGRAM
        
    def to_dict(self) -> Dict[str, Any]:
        result = super().to_dict()
        result["buckets"] = self.buckets
        result["counts"] = self.counts
        result["sum"] = self.sum
        result["count"] = self.count
        
        if self.labeled_counts:
            result["labeled_counts"] = self.labeled_counts
            result["labeled_sums"] = self.labeled_sums
            result["labeled_counts_total"] = self.labeled_counts_total
            
        return result

class MetricsManager:
    """Manager for system-wide metrics collection"""
    
    _instance = None
    
    @classmethod
    def get_instance(cls):
        """Get singleton instance"""
        if cls._instance is None:
            cls._instance = MetricsManager()
        return cls._instance
    
    def __init__(self):
        """Initialize metrics manager"""
        if MetricsManager._instance is not None:
            raise RuntimeError("MetricsManager is a singleton. Use get_instance()")
            
        self._metrics: Dict[str, Metric] = {}
        self._lock = threading.RLock()
        
        # Resource usage metrics (system)
        self.system_cpu_percent = self.gauge(
            "system_cpu_percent", 
            "System CPU usage in percent"
        )
        self.system_memory_percent = self.gauge(
            "system_memory_percent", 
            "System memory usage in percent"
        )
        self.process_cpu_percent = self.gauge(
            "process_cpu_percent", 
            "Process CPU usage in percent"
        )
        self.process_memory_mb = self.gauge(
            "process_memory_mb", 
            "Process memory usage in MB"
        )
        
        # MQTT metrics
        self.mqtt_messages_received = self.counter(
            "mqtt_messages_received", 
            "Number of MQTT messages received",
            ["topic"]
        )
        self.mqtt_messages_published = self.counter(
            "mqtt_messages_published", 
            "Number of MQTT messages published",
            ["topic"]
        )
        self.mqtt_connection_errors = self.counter(
            "mqtt_connection_errors", 
            "Number of MQTT connection errors"
        )
        self.mqtt_connection_latency = self.histogram(
            "mqtt_connection_latency", 
            "MQTT connection latency in seconds"
        )
        
        # WebSocket metrics
        self.ws_connections_current = self.gauge(
            "ws_connections_current", 
            "Current number of WebSocket connections"
        )
        self.ws_connections_total = self.counter(
            "ws_connections_total", 
            "Total number of WebSocket connections made"
        )
        self.ws_messages_sent = self.counter(
            "ws_messages_sent", 
            "Number of WebSocket messages sent"
        )
        self.ws_messages_received = self.counter(
            "ws_messages_received", 
            "Number of WebSocket messages received"
        )
        
        # Meshtastic metrics
        self.meshtastic_nodes_current = self.gauge(
            "meshtastic_nodes_current", 
            "Current number of Meshtastic nodes"
        )
        self.meshtastic_nodes_active = self.gauge(
            "meshtastic_nodes_active", 
            "Number of active Meshtastic nodes"
        )
        self.meshtastic_messages_processed = self.counter(
            "meshtastic_messages_processed", 
            "Number of Meshtastic messages processed",
            ["message_type"]
        )
        
        # API metrics
        self.api_requests = self.counter(
            "api_requests", 
            "Number of API requests",
            ["endpoint", "method", "status"]
        )
        self.api_request_duration = self.histogram(
            "api_request_duration", 
            "API request duration in seconds",
            ["endpoint", "method"]
        )
        
        # Start a background task for collecting system metrics
        self._running = True
        self._resource_thread = threading.Thread(
            target=self._collect_resource_metrics,
            daemon=True,
            name="MetricsCollector"
        )
        self._resource_thread.start()
        
    def counter(self, name: str, description: str, labels: Optional[List[str]] = None) -> Counter:
        """Create and register a counter"""
        with self._lock:
            if name in self._metrics:
                metric = self._metrics[name]
                if not isinstance(metric, Counter):
                    raise ValueError(f"Metric {name} already exists but is not a Counter")
                return metric
                
            counter = Counter(name, description, labels)
            self._metrics[name] = counter
            return counter
            
    def gauge(self, name: str, description: str, labels: Optional[List[str]] = None) -> Gauge:
        """Create and register a gauge"""
        with self._lock:
            if name in self._metrics:
                metric = self._metrics[name]
                if not isinstance(metric, Gauge):
                    raise ValueError(f"Metric {name} already exists but is not a Gauge")
                return metric
                
            gauge = Gauge(name, description, labels)
            self._metrics[name] = gauge
            return gauge
            
    def histogram(self, name: str, description: str, labels: Optional[List[str]] = None,
                  buckets: Optional[List[float]] = None) -> Histogram:
        """Create and register a histogram"""
        with self._lock:
            if name in self._metrics:
                metric = self._metrics[name]
                if not isinstance(metric, Histogram):
                    raise ValueError(f"Metric {name} already exists but is not a Histogram")
                return metric
                
            histogram = Histogram(name, description, labels, buckets)
            self._metrics[name] = histogram
            return histogram
            
    def get_metrics(self) -> Dict[str, Dict[str, Any]]:
        """Get all metrics as a dictionary"""
        with self._lock:
            return {name: metric.to_dict() for name, metric in self._metrics.items()}
            
    def get_metric(self, name: str) -> Optional[Dict[str, Any]]:
        """Get a specific metric"""
        with self._lock:
            metric = self._metrics.get(name)
            return metric.to_dict() if metric else None
            
    def _collect_resource_metrics(self):
        """Background thread to collect resource metrics"""
        process = psutil.Process(os.getpid())
        
        while self._running:
            try:
                # System metrics
                self.system_cpu_percent.set(psutil.cpu_percent(interval=None))
                self.system_memory_percent.set(psutil.virtual_memory().percent)
                
                # Process metrics
                with process.oneshot():
                    self.process_cpu_percent.set(process.cpu_percent(interval=None))
                    self.process_memory_mb.set(process.memory_info().rss / (1024 * 1024))
                
            except Exception as e:
                logger.error(f"Error collecting resource metrics: {e}")
                
            time.sleep(5)  # Collect every 5 seconds
    
    def stop(self):
        """Stop background collection"""
        self._running = False
        if self._resource_thread and self._resource_thread.is_alive():
            self._resource_thread.join(timeout=1.0)


# Helper function to get the metrics manager instance
def get_metrics_manager() -> MetricsManager:
    return MetricsManager.get_instance() 