import pytest
import time
import random
import string
from unittest.mock import MagicMock, patch, Mock
import threading

# Import the module to test
import backend.direct_meshtastic as direct_meshtastic


class MockSerialInterface:
    """Mock implementation of meshtastic.serial_interface.SerialInterface"""
    
    def __init__(self, port=None):
        self.port = port
        self.is_connected = True
        self._callbacks = {}
        self.node_id = ''.join(random.choices(string.hexdigits.upper(), k=8))
        self.nodes = {
            self.node_id: {
                "num": self.node_id,
                "user": {
                    "longName": f"Test Device {self.node_id[:4]}",
                    "shortName": f"T{self.node_id[:4]}"
                },
                "position": {
                    "latitude": random.uniform(-90, 90),
                    "longitude": random.uniform(-180, 180),
                    "altitude": 0,
                    "time": int(time.time())
                }
            }
        }
        
        # Set up callbacks
        self.onReceive = None
        self.onNodeUpdated = None
        self.onConnectionEstablished = None
    
    def getMyNodeInfo(self):
        """Return info about this node"""
        return self.nodes.get(self.node_id)
    
    def getNodes(self):
        """Return all nodes"""
        return self.nodes
    
    def close(self):
        """Close the connection"""
        self.is_connected = False
    
    def sendText(self, text, destinationId=None):
        """Simulate sending text message"""
        # If we have an onReceive callback, simulate receiving our own message
        if self.onReceive:
            packet = {
                "from": self.node_id,
                "to": destinationId or "BROADCAST",
                "decoded": {
                    "portnum": "TEXT_MESSAGE_APP",
                    "payload": text,
                    "text": text
                }
            }
            # Call in a separate thread to simulate async behavior
            threading.Timer(0.1, lambda: self.onReceive(packet, self)).start()
            
        return True
    
    def getConfig(self):
        """Get mock device configuration"""
        return {
            "device": {
                "role": "CLIENT",
                "serial": "mock-serial-123",
                "firmware_version": "2.1.17"
            },
            "position": {
                "gps_mode": 0,
                "position_broadcast_secs": 900,
                "smart_position": True
            },
            "power": {
                "ls_secs": 300,
                "mesh_sds_timeout_secs": 2147483647,
                "min_wake_secs": 10,
                "sds_secs": 4294967295,
                "wait_bluetooth_secs": 60
            }
        }
    
    def setConfig(self, key, value):
        """Set config value (mock method)"""
        # In a real implementation, this would update the config dict
        return True
    
    def getChannels(self):
        """Get mock channel settings"""
        return [{
            "index": 0,
            "settings": {
                "name": "Default",
                "psk": "AQ==",
                "downlink_enabled": False,
                "uplink_enabled": False
            }
        }]
    
    def setChannelSettings(self, settings, channelIndex=0):
        """Set channel settings (mock method)"""
        # In a real implementation, this would update the channel settings
        return True


@pytest.fixture
def mock_serial_interface():
    """Create a mock serial interface"""
    with patch('meshtastic.serial_interface.SerialInterface', MockSerialInterface):
        yield


@pytest.fixture
def mock_findPorts():
    """Mock the findPorts function to return simulated device ports"""
    mock_ports = [
        '/dev/mock_port_1',
        '/dev/mock_port_2'
    ]
    
    with patch('meshtastic.util.findPorts', return_value=mock_ports):
        yield mock_ports


class TestDeviceInterface:
    """Test the DeviceInterface class"""
    
    def test_connect_serial_with_specified_port(self, mock_serial_interface):
        """Test connecting to a device with a specified port"""
        # Create a device interface with a specific port
        device = direct_meshtastic.DeviceInterface("serial", {"port": "/dev/mock_port_1"})
        
        # Connect to the device
        success = device.connect()
        
        # Verify the connection was successful
        assert success is True
        assert device.connected is True
        assert device.interface is not None
        assert device.node_id is not None
    
    def test_connect_serial_autodiscovery(self, mock_serial_interface):
        """Test connecting to a device with autodiscovery"""
        # Create a device interface without a specific port
        device = direct_meshtastic.DeviceInterface("serial", {})
        
        # Connect to the device
        success = device.connect()
        
        # Verify the connection was successful
        assert success is True
        assert device.connected is True
        assert device.interface is not None
    
    def test_disconnect(self, mock_serial_interface):
        """Test disconnecting from a device"""
        # Create and connect a device interface
        device = direct_meshtastic.DeviceInterface("serial", {"port": "/dev/mock_port_1"})
        device.connect()
        
        # Disconnect from the device
        device.disconnect()
        
        # Verify the disconnection was successful
        assert device.connected is False
        assert device.interface is None
    
    def test_connect_already_connected(self, mock_serial_interface):
        """Test connecting when already connected"""
        # Create and connect a device interface
        device = direct_meshtastic.DeviceInterface("serial", {"port": "/dev/mock_port_1"})
        device.connect()
        
        # Try connecting again
        success = device.connect()
        
        # Verify it returns True but doesn't recreate the interface
        assert success is True
        assert device.connected is True
    
    def test_event_handlers(self, mock_serial_interface):
        """Test that event handlers are properly set up"""
        # Create event handler mocks
        receive_handler = MagicMock()
        node_updated_handler = MagicMock()
        connection_handler = MagicMock()
        
        # Create a device interface
        device = direct_meshtastic.DeviceInterface("serial", {"port": "/dev/mock_port_1"})
        
        # Add event handlers
        device.add_event_handler("onReceive", receive_handler)
        device.add_event_handler("onNodeUpdated", node_updated_handler)
        device.add_event_handler("onConnectionEstablished", connection_handler)
        
        # Connect to the device
        device.connect()
        
        # Verify the handlers were registered
        assert "onReceive" in device.event_handlers
        assert "onNodeUpdated" in device.event_handlers
        assert "onConnectionEstablished" in device.event_handlers
        
        # Simulate a message received event
        packet = {"from": "TEST", "decoded": {"text": "Hello"}}
        device.interface.onReceive(packet, device.interface)
        
        # Verify the handler was called
        receive_handler.assert_called_once_with(packet)
    
    def test_send_text(self, mock_serial_interface):
        """Test sending a text message"""
        # Create a device interface
        device = direct_meshtastic.DeviceInterface("serial", {"port": "/dev/mock_port_1"})
        device.connect()
        
        # Send a text message
        success = device.send_text("Hello, world!")
        
        # Verify the message was sent
        assert success is True
    
    def test_send_text_to_destination(self, mock_serial_interface):
        """Test sending a text message to a specific destination"""
        # Create a device interface
        device = direct_meshtastic.DeviceInterface("serial", {"port": "/dev/mock_port_1"})
        device.connect()
        
        # Send a text message to a specific destination
        destination_id = "ABCDEF"
        success = device.send_text("Hello, specific device!", destination_id)
        
        # Verify the message was sent
        assert success is True
    
    def test_get_config(self, mock_serial_interface):
        """Test getting device configuration"""
        # Create a device interface
        device = direct_meshtastic.DeviceInterface("serial", {"port": "/dev/mock_port_1"})
        device.connect()
        
        # Get the configuration
        config = device.get_config()
        
        # Verify the configuration was retrieved
        assert config is not None
        assert "device" in config
        assert "power" in config
    
    def test_set_config(self, mock_serial_interface):
        """Test setting device configuration"""
        # Create a device interface
        device = direct_meshtastic.DeviceInterface("serial", {"port": "/dev/mock_port_1"})
        device.connect()
        
        # Set a configuration value
        success = device.set_config("device.role", "ROUTER")
        
        # Verify the value was set
        assert success is True


class TestDeviceManager:
    """Test the DeviceManager class"""
    
    @pytest.mark.asyncio
    async def test_discover_serial_devices(self, mock_findPorts):
        """Test discovering serial devices"""
        # Create a device manager
        manager = direct_meshtastic.DeviceManager()
        
        # Discover serial devices
        devices = await manager.discover_serial_devices()
        
        # Verify devices were found
        assert len(devices) == 2
        assert '/dev/mock_port_1' in devices
        assert '/dev/mock_port_2' in devices
    
    @pytest.mark.asyncio
    async def test_connect_device(self, mock_serial_interface, mock_findPorts):
        """Test connecting to a device through the manager"""
        # Create a device manager
        manager = direct_meshtastic.DeviceManager()
        
        # Connect to a device
        device_id = await manager.connect_device("serial", {"port": "/dev/mock_port_1"})
        
        # Verify the connection was successful
        assert device_id is not None
        assert device_id in manager.devices
        assert manager.devices[device_id].connected is True
    
    @pytest.mark.asyncio
    async def test_disconnect_device(self, mock_serial_interface, mock_findPorts):
        """Test disconnecting from a device through the manager"""
        # Create a device manager
        manager = direct_meshtastic.DeviceManager()
        
        # Connect to a device
        device_id = await manager.connect_device("serial", {"port": "/dev/mock_port_1"})
        
        # Disconnect from the device
        success = await manager.disconnect_device(device_id)
        
        # Verify the disconnection was successful
        assert success is True
        assert device_id in manager.devices
        assert manager.devices[device_id].connected is False
    
    @pytest.mark.asyncio
    async def test_get_device(self, mock_serial_interface, mock_findPorts):
        """Test getting a device from the manager"""
        # Create a device manager
        manager = direct_meshtastic.DeviceManager()
        
        # Connect to a device
        device_id = await manager.connect_device("serial", {"port": "/dev/mock_port_1"})
        
        # Get the device
        device = manager.get_device(device_id)
        
        # Verify the device was retrieved
        assert device is not None
        assert device.connected is True
        assert device.node_id is not None
    
    @pytest.mark.asyncio
    async def test_get_device_info(self, mock_serial_interface, mock_findPorts):
        """Test getting device information from the manager"""
        # Create a device manager
        manager = direct_meshtastic.DeviceManager()
        
        # Connect to a device
        device_id = await manager.connect_device("serial", {"port": "/dev/mock_port_1"})
        
        # Get the device information
        info = manager.get_device_info(device_id)
        
        # Verify the information was retrieved
        assert info is not None
        assert "id" in info
        assert "connected" in info
        assert "type" in info
        assert info["connected"] is True
        assert info["type"] == "serial" 