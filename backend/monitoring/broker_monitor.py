import time
import threading
import logging
import socket
from typing import Dict, List, Any, Optional, Callable
from enum import Enum, auto
import json

# Configure logger
logger = logging.getLogger(__name__)

class BrokerStatus(Enum):
    """Enum for broker status"""
    UNKNOWN = auto()
    OFFLINE = auto()
    DEGRADED = auto()
    HEALTHY = auto()


class BrokerHealthCheck:
    """Tracks and monitors the health of the MQTT broker"""
    
    def __init__(self, broker_host: str, broker_port: int, check_interval: float = 30.0,
                ping_topic: str = "$SYS/broker/uptime", ping_timeout: float = 5.0):
        """Initialize broker health check
        
        Args:
            broker_host: Hostname or IP of the MQTT broker
            broker_port: Port of the MQTT broker
            check_interval: How often to check the broker health in seconds
            ping_topic: The system topic to subscribe to for ping checks
            ping_timeout: Timeout for ping response in seconds
        """
        self.broker_host = broker_host
        self.broker_port = broker_port
        self.check_interval = check_interval
        self.ping_topic = ping_topic
        self.ping_timeout = ping_timeout
        
        # Current status
        self.status = BrokerStatus.UNKNOWN
        
        # Metrics
        self.metrics = {
            "last_checked": 0,
            "last_successful_check": 0,
            "consecutive_failures": 0,
            "total_checks": 0,
            "successful_checks": 0,
            "failed_checks": 0,
            "uptime": None,
            "client_count": None,
            "message_rate": None
        }
        
        # Status history
        self.status_history: List[Dict[str, Any]] = []
        
        # Status change callbacks
        self._status_callbacks: List[Callable[[BrokerStatus, BrokerStatus], None]] = []
        
        # Control for the monitoring thread
        self._running = False
        self._monitor_thread = None
        
        # Reference to MQTT client for checks
        self._mqtt_client = None
        
        # Flag for ping response received
        self._ping_received = False
        self._last_ping_data = None
    
    def start_monitoring(self, mqtt_client=None):
        """Start the broker monitoring thread
        
        Args:
            mqtt_client: MQTT client instance to use for checks
        """
        self._mqtt_client = mqtt_client
        
        if self._running:
            logger.warning("Broker monitoring already running")
            return
        
        self._running = True
        self._monitor_thread = threading.Thread(
            target=self._monitor_loop,
            name="BrokerMonitor",
            daemon=True
        )
        self._monitor_thread.start()
        logger.info(f"Started MQTT broker monitoring for {self.broker_host}:{self.broker_port}")
    
    def stop_monitoring(self):
        """Stop the broker monitoring thread"""
        if not self._running:
            logger.warning("Broker monitoring already stopped")
            return
        
        self._running = False
        if self._monitor_thread and self._monitor_thread.is_alive():
            self._monitor_thread.join(timeout=1.0)
        
        logger.info("Stopped MQTT broker monitoring")
    
    def add_status_callback(self, callback: Callable[[BrokerStatus, BrokerStatus], None]):
        """Add a callback for broker status changes
        
        Args:
            callback: Function to call when status changes, receives old_status and new_status
        """
        self._status_callbacks.append(callback)
    
    def get_status(self) -> Dict[str, Any]:
        """Get the current broker status and metrics"""
        now = time.time()
        result = {
            "status": self.status.name,
            "host": self.broker_host,
            "port": self.broker_port,
            "last_check_seconds_ago": round(now - self.metrics["last_checked"]) if self.metrics["last_checked"] > 0 else None,
            "consecutive_failures": self.metrics["consecutive_failures"],
            "uptime": self.metrics["uptime"],
            "client_count": self.metrics["client_count"],
            "message_rate": self.metrics["message_rate"]
        }
        
        return result
    
    def get_status_history(self, limit: int = 10) -> List[Dict[str, Any]]:
        """Get the history of status changes
        
        Args:
            limit: Maximum number of history entries to return
        """
        return self.status_history[-limit:] if limit > 0 else self.status_history
    
    def set_ping_received(self, data: Any = None):
        """Set flag indicating that a ping response was received"""
        self._ping_received = True
        self._last_ping_data = data
    
    def _check_broker_connectivity(self) -> bool:
        """Check if broker is reachable at the TCP level"""
        try:
            # Try to create a socket connection to the broker
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(2.0)
            result = sock.connect_ex((self.broker_host, self.broker_port))
            sock.close()
            
            return result == 0
        except Exception as e:
            logger.error(f"Error checking broker connectivity: {e}")
            return False
    
    def _check_mqtt_connectivity(self) -> bool:
        """Check if MQTT protocol is responsive
        
        Requires the MQTT client to be set
        """
        if not self._mqtt_client:
            logger.warning("Cannot check MQTT connectivity: No MQTT client provided")
            return False
        
        try:
            # Reset ping flag
            self._ping_received = False
            
            # Check if client is connected
            if not self._mqtt_client.is_connected():
                logger.warning("MQTT client not connected during health check")
                return False
            
            # Try to subscribe to a system topic
            self._mqtt_client.subscribe(self.ping_topic, qos=0)
            
            # Wait for ping response
            start_time = time.time()
            while not self._ping_received and time.time() - start_time < self.ping_timeout:
                time.sleep(0.1)
            
            # Check if we received a ping response
            return self._ping_received
            
        except Exception as e:
            logger.error(f"Error checking MQTT connectivity: {e}")
            return False
        finally:
            # Unsubscribe from ping topic
            if self._mqtt_client and self._mqtt_client.is_connected():
                self._mqtt_client.unsubscribe(self.ping_topic)
    
    def _fetch_broker_metrics(self):
        """Fetch metrics from broker system topics"""
        if not self._mqtt_client or not self._mqtt_client.is_connected():
            return
        
        # Topics to gather metrics from
        system_topics = [
            "$SYS/broker/uptime",
            "$SYS/broker/clients/total",
            "$SYS/broker/messages/per-second/received"
        ]
        
        for topic in system_topics:
            try:
                # Subscribe to system topic
                self._mqtt_client.subscribe(topic, qos=0)
            except Exception as e:
                logger.error(f"Error subscribing to system topic {topic}: {e}")
        
        # Allow time for response
        time.sleep(1.0)
        
        # Unsubscribe from all system topics
        for topic in system_topics:
            try:
                self._mqtt_client.unsubscribe(topic)
            except Exception as e:
                logger.error(f"Error unsubscribing from system topic {topic}: {e}")
    
    def _update_status(self, new_status: BrokerStatus):
        """Update the broker status and notify callbacks if changed"""
        old_status = self.status
        
        if new_status != old_status:
            # Log the status change
            logger.info(f"MQTT broker status changed from {old_status.name} to {new_status.name}")
            
            # Record in history
            self.status_history.append({
                "timestamp": time.time(),
                "old_status": old_status.name,
                "new_status": new_status.name,
                "metrics": dict(self.metrics)
            })
            
            # Limit history size
            if len(self.status_history) > 100:
                self.status_history = self.status_history[-100:]
            
            # Set new status
            self.status = new_status
            
            # Notify callbacks
            for callback in self._status_callbacks:
                try:
                    callback(old_status, new_status)
                except Exception as e:
                    logger.error(f"Error in broker status callback: {e}")
    
    def _monitor_loop(self):
        """Main monitoring loop"""
        while self._running:
            try:
                self._perform_health_check()
            except Exception as e:
                logger.error(f"Error in broker monitoring loop: {e}")
            
            # Sleep until next check
            time.sleep(self.check_interval)
    
    def _perform_health_check(self):
        """Perform a complete health check"""
        # Update timing
        self.metrics["last_checked"] = time.time()
        self.metrics["total_checks"] += 1
        
        # Check connectivity
        tcp_check = self._check_broker_connectivity()
        
        if not tcp_check:
            # Broker is offline
            self.metrics["consecutive_failures"] += 1
            self.metrics["failed_checks"] += 1
            self._update_status(BrokerStatus.OFFLINE)
            return
        
        # Check MQTT protocol if client available
        mqtt_check = self._check_mqtt_connectivity() if self._mqtt_client else None
        
        if mqtt_check is False:  # Specifically False, not None
            # MQTT protocol is not responding
            self.metrics["consecutive_failures"] += 1
            self.metrics["failed_checks"] += 1
            self._update_status(BrokerStatus.DEGRADED)
            return
        
        # If we got here, the broker is responsive
        self.metrics["consecutive_failures"] = 0
        self.metrics["successful_checks"] += 1
        self.metrics["last_successful_check"] = time.time()
        
        # Fetch detailed metrics
        self._fetch_broker_metrics()
        
        # Update status to healthy
        self._update_status(BrokerStatus.HEALTHY)


class MQTTBrokerMonitor:
    """Monitors multiple MQTT brokers and provides aggregate status"""
    
    def __init__(self):
        """Initialize the broker monitor"""
        self.broker_checks = {}
        self.overall_status = BrokerStatus.UNKNOWN
    
    def add_broker(self, name: str, broker_host: str, broker_port: int,
                  check_interval: float = 30.0, mqtt_client=None) -> BrokerHealthCheck:
        """Add a broker to monitor
        
        Args:
            name: Name for this broker
            broker_host: Hostname or IP of the broker
            broker_port: Port of the broker
            check_interval: How often to check the broker
            mqtt_client: MQTT client to use for checks
            
        Returns:
            BrokerHealthCheck: The health check instance
        """
        if name in self.broker_checks:
            logger.warning(f"Broker {name} already being monitored, replacing")
        
        # Create health check
        check = BrokerHealthCheck(
            broker_host=broker_host,
            broker_port=broker_port,
            check_interval=check_interval
        )
        
        # Store in our dictionary
        self.broker_checks[name] = check
        
        # Add status callback
        check.add_status_callback(lambda old, new: self._update_overall_status())
        
        # Start monitoring
        check.start_monitoring(mqtt_client)
        
        return check
    
    def remove_broker(self, name: str):
        """Remove a broker from monitoring"""
        if name in self.broker_checks:
            # Stop monitoring
            self.broker_checks[name].stop_monitoring()
            # Remove from dictionary
            del self.broker_checks[name]
            # Update overall status
            self._update_overall_status()
    
    def get_broker_status(self, name: str) -> Optional[Dict[str, Any]]:
        """Get status for a specific broker"""
        if name in self.broker_checks:
            return self.broker_checks[name].get_status()
        return None
    
    def get_all_statuses(self) -> Dict[str, Dict[str, Any]]:
        """Get statuses for all monitored brokers"""
        return {name: check.get_status() for name, check in self.broker_checks.items()}
    
    def get_overall_status(self) -> Dict[str, Any]:
        """Get the overall broker monitoring status"""
        broker_count = len(self.broker_checks)
        healthy_count = sum(1 for check in self.broker_checks.values() 
                          if check.status == BrokerStatus.HEALTHY)
        degraded_count = sum(1 for check in self.broker_checks.values() 
                           if check.status == BrokerStatus.DEGRADED)
        offline_count = sum(1 for check in self.broker_checks.values() 
                          if check.status == BrokerStatus.OFFLINE)
        
        return {
            "status": self.overall_status.name,
            "broker_count": broker_count,
            "healthy_count": healthy_count,
            "degraded_count": degraded_count,
            "offline_count": offline_count
        }
    
    def _update_overall_status(self):
        """Update the overall status based on all broker statuses"""
        if not self.broker_checks:
            self.overall_status = BrokerStatus.UNKNOWN
            return
        
        # Check if any brokers are offline
        if any(check.status == BrokerStatus.OFFLINE for check in self.broker_checks.values()):
            self.overall_status = BrokerStatus.DEGRADED
            return
        
        # Check if all brokers are healthy
        if all(check.status == BrokerStatus.HEALTHY for check in self.broker_checks.values()):
            self.overall_status = BrokerStatus.HEALTHY
            return
        
        # Otherwise, some are degraded
        self.overall_status = BrokerStatus.DEGRADED