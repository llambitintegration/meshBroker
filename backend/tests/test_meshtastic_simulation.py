import pytest
import time
import json
import random
import threading
import string
import os
from unittest.mock import MagicMock, patch
import paho.mqtt.client as mqtt
from paho.mqtt.enums import CallbackAPIVersion
from typing import Dict, Any, List

# Create mocks for missing modules
sys_modules_patcher = patch.dict('sys.modules', {
    'messaging': MagicMock(),
    'messaging.message_store': MagicMock(),
    'messaging.message_processor': MagicMock(),
    'messaging.message_queue': MagicMock(),
    'messaging.message_delivery': MagicMock(),
    'monitoring': MagicMock(),
    'monitoring.broker_monitor': MagicMock()
})
sys_modules_patcher.start()

# Now import after mocking the modules
from backend.mqtt_handler import MQTTHandler
import backend.meshtastic_integration as meshtastic_integration

# Constants for testing
MESHTASTIC_PREFIX = "msh"
NODE_COUNT = 5
MESSAGE_COUNT = 10


class MockMeshtasticNode:
    """Simulate a Meshtastic node"""
    
    def __init__(self, node_id: str, name: str = None, lat: float = None, lon: float = None):
        """Initialize a mock Meshtastic node
        
        Args:
            node_id: The node ID (8 character hex)
            name: Optional node name
            lat: Optional latitude
            lon: Optional longitude
        """
        self.node_id = node_id
        self.name = name or f"Node-{node_id[:4]}"
        self.lat = lat or random.uniform(-90, 90)
        self.lon = lon or random.uniform(-180, 180)
        self.battery_level = random.randint(50, 100)
        self.snr = random.uniform(5, 15)
        self.last_seen = time.time()
        
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary representing the node"""
        return {
            "id": self.node_id,
            "name": self.name,
            "position": {
                "latitude": self.lat,
                "longitude": self.lon,
                "altitude": 0,
                "time": int(self.last_seen)
            },
            "battery": {
                "level": self.battery_level,
                "voltage": 3.7
            },
            "hardware": {
                "device_model": "TBEAM",
                "firmware_version": "2.1.17"
            },
            "user": {
                "short_name": self.name,
                "long_name": f"Test User {self.name}"
            },
            "metrics": {
                "snr": self.snr,
                "rssi": -65,
                "last_heard": int(self.last_seen)
            }
        }
        
    def generate_position_update(self) -> Dict[str, Any]:
        """Generate a position update message"""
        # Move slightly in a random direction
        self.lat += random.uniform(-0.001, 0.001)
        self.lon += random.uniform(-0.001, 0.001)
        self.last_seen = time.time()
        
        return {
            "type": "position",
            "from": self.node_id,
            "timestamp": int(self.last_seen),
            "position": {
                "latitude": self.lat,
                "longitude": self.lon,
                "altitude": 0,
                "time": int(self.last_seen)
            }
        }
        
    def generate_telemetry_update(self) -> Dict[str, Any]:
        """Generate a telemetry update message"""
        # Update battery randomly
        self.battery_level = max(1, min(100, self.battery_level + random.randint(-5, 2)))
        self.last_seen = time.time()
        
        return {
            "type": "telemetry",
            "from": self.node_id,
            "timestamp": int(self.last_seen),
            "telemetry": {
                "battery_level": self.battery_level,
                "voltage": 3.7,
                "channel_utilization": random.uniform(0, 0.5),
                "air_utilization_tx": random.uniform(0, 0.3)
            }
        }
        
    def generate_text_message(self, to_node_id: str = None) -> Dict[str, Any]:
        """Generate a text message
        
        Args:
            to_node_id: Optional recipient node ID, if None message is broadcast
        """
        self.last_seen = time.time()
        
        messages = [
            "Hello from the field!",
            "Testing 1 2 3",
            "Signal check, please respond",
            "Position confirmed",
            "Moving to next waypoint",
            "Weather is clear",
            "Need assistance",
            "All systems normal",
            "Battery status good",
            "Meeting at the base"
        ]
        
        return {
            "type": "text",
            "from": self.node_id,
            "to": to_node_id or "BROADCAST",
            "timestamp": int(self.last_seen),
            "text": random.choice(messages)
        }


class MeshtasticSimulator:
    """Simulate a network of Meshtastic nodes"""
    
    def __init__(self, mqtt_client, node_count: int = 5):
        """Initialize the simulator
        
        Args:
            mqtt_client: MQTT client for publishing messages
            node_count: Number of nodes to simulate
        """
        self.mqtt_client = mqtt_client
        self.nodes: Dict[str, MockMeshtasticNode] = {}
        
        # Create nodes
        for i in range(node_count):
            node_id = ''.join(random.choices(string.hexdigits.upper(), k=8))
            self.nodes[node_id] = MockMeshtasticNode(node_id)
            
        self._running = False
        self._thread = None
    
    def start(self, update_interval: float = 5.0):
        """Start the simulator
        
        Args:
            update_interval: Seconds between updates
        """
        if self._running:
            return
            
        self._running = True
        self._thread = threading.Thread(
            target=self._run_loop,
            args=(update_interval,),
            daemon=True
        )
        self._thread.start()
    
    def stop(self):
        """Stop the simulator"""
        self._running = False
        if self._thread:
            self._thread.join(timeout=2.0)
    
    def _run_loop(self, update_interval: float):
        """Run the simulator loop"""
        # First, publish all node info
        for node_id, node in self.nodes.items():
            self._publish_node_info(node)
            time.sleep(0.1)  # Small delay to avoid flooding
        
        # Then run the update loop
        while self._running:
            for node_id, node in self.nodes.items():
                # Select a random update type
                update_type = random.choice(["position", "telemetry", "text"])
                
                if update_type == "position":
                    message = node.generate_position_update()
                    self._publish_message(node.node_id, "position", message)
                elif update_type == "telemetry":
                    message = node.generate_telemetry_update()
                    self._publish_message(node.node_id, "telemetry", message)
                elif update_type == "text":
                    # 20% chance to send to a specific node, otherwise broadcast
                    to_node = random.choice(list(self.nodes.keys())) if random.random() < 0.2 else None
                    message = node.generate_text_message(to_node)
                    self._publish_message(node.node_id, "text", message)
                
                # Small delay between node updates
                time.sleep(0.5)
            
            # Wait for next update cycle
            time.sleep(update_interval)
    
    def _publish_node_info(self, node: MockMeshtasticNode):
        """Publish node info message"""
        message = node.to_dict()
        self._publish_message(node.node_id, "nodeinfo", message)
    
    def _publish_message(self, node_id: str, message_type: str, message: Dict[str, Any]):
        """Publish a message to MQTT
        
        Args:
            node_id: Source node ID
            message_type: Message type (nodeinfo, position, telemetry, text)
            message: Message data
        """
        topic = f"{MESHTASTIC_PREFIX}/rx/{node_id}/{message_type}"
        payload = json.dumps(message)
        
        try:
            self.mqtt_client.publish(topic, payload, qos=1)
        except Exception as e:
            print(f"Error publishing message: {e}")


@pytest.fixture
def mqtt_client():
    """Create MQTT client for testing"""
    client = mqtt.Client(CallbackAPIVersion.VERSION1, "simulation_test_client")
    client.connect("127.0.0.1", 1883)
    client.loop_start()
    
    yield client
    
    client.loop_stop()
    client.disconnect()


@pytest.mark.simulation
def test_meshtastic_simulation(mqtt_client):
    """Run a simulation of Meshtastic nodes"""
    # Skip if not running simulations
    if not os.getenv("RUN_SIMULATION_TESTS", "false").lower() == "true":
        pytest.skip("Simulation tests not enabled")
    
    # Create simulator
    simulator = MeshtasticSimulator(mqtt_client, node_count=NODE_COUNT)
    
    try:
        # Start simulation
        simulator.start(update_interval=2.0)
        
        # Run for some time
        print(f"Running Meshtastic simulation with {NODE_COUNT} nodes...")
        time.sleep(30)  # Run for 30 seconds
        
        # Test passes if we get here without exceptions
        assert True
        
    finally:
        # Clean up
        simulator.stop()


@pytest.mark.integration
def test_meshtastic_integration_with_simulation(mqtt_client):
    """Test Meshtastic integration with simulated nodes"""
    # Skip if not running integration tests
    if os.getenv("SKIP_INTEGRATION_TESTS", "true").lower() == "true":
        pytest.skip("Integration tests not enabled")
    
    # Create MQTT handler
    handler = MQTTHandler(
        broker_host="127.0.0.1",
        broker_port=1883,
        client_id="integration_test_client",
        persistence_enabled=False
    )
    
    # Connect
    handler.connect()
    
    # Messages received counter
    messages_received = 0
    message_event = threading.Event()
    
    # Message callback
    def on_message(topic, payload):
        nonlocal messages_received
        if topic.startswith(f"{MESHTASTIC_PREFIX}/rx/"):
            messages_received += 1
            if messages_received >= MESSAGE_COUNT:
                message_event.set()
    
    # Set callback
    handler.set_message_callback(on_message)
    
    # Subscribe to all Meshtastic messages
    handler.subscribe(f"{MESHTASTIC_PREFIX}/rx/#")
    
    # Create simulator with fewer nodes for this test
    simulator = MeshtasticSimulator(mqtt_client, node_count=3)
    
    try:
        # Start simulation
        simulator.start(update_interval=1.0)
        
        # Wait for messages
        message_event.wait(timeout=30)
        
        # Check if we received enough messages
        assert messages_received >= MESSAGE_COUNT, f"Only received {messages_received} messages"
        
    finally:
        # Clean up
        simulator.stop()
        handler.disconnect() 