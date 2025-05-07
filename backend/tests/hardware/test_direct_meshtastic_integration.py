import pytest
import os
import time
import logging
import asyncio
import meshtastic
import meshtastic.util
from typing import List, Optional
from contextlib import asynccontextmanager
import pytest_asyncio

# Import the module to test
import backend.direct_meshtastic as direct_meshtastic

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

# Environment variable to control hardware testing
ENABLE_HARDWARE_TESTS = os.environ.get("ENABLE_HARDWARE_TESTS", "false").lower() == "true"
# Timeout for hardware operations (in seconds)
HARDWARE_TIMEOUT = int(os.environ.get("HARDWARE_TIMEOUT", "5"))


def check_hardware_available() -> List[str]:
    """Check if Meshtastic hardware is available for testing
    
    Returns:
        List of available port names, or empty list if none found
    """
    try:
        return meshtastic.util.findPorts()
    except Exception as e:
        logger.warning(f"Error detecting Meshtastic devices: {e}")
        return []


@pytest.fixture(scope="session")
def hardware_fixture():
    """Fixture to check for hardware and skip tests if not available"""
    if not ENABLE_HARDWARE_TESTS:
        pytest.skip("Hardware tests disabled. Set ENABLE_HARDWARE_TESTS=true to enable.")
        
    ports = check_hardware_available()
    if not ports:
        pytest.skip("No Meshtastic devices found for hardware testing")
        
    # Return the first available port
    return ports[0]


@asynccontextmanager
async def _manage_device_connection(port):
    """Context manager for device connection"""
    # Create a device manager
    manager = direct_meshtastic.DeviceManager()
    device_id = None
    
    try:
        # Connect to the device
        logger.info(f"Connecting to device on port {port}")
        device_id = await manager.connect_device("serial", {"port": port})
        
        if not device_id:
            pytest.skip(f"Failed to connect to device on port {port}")
            
        # Get the device interface
        device = manager.get_device(device_id)
        if not device or not device.connected:
            pytest.skip(f"Device not properly connected on port {port}")
            
        logger.info(f"Successfully connected to device with ID {device_id}")
        
        # Wait a moment for the connection to stabilize
        await asyncio.sleep(1)
        
        # Yield control back to the test function
        yield (manager, device_id)
        
    finally:
        # Clean up: disconnect the device
        if device_id:
            logger.info(f"Disconnecting device with ID {device_id}")
            try:
                await manager.disconnect_device(device_id)
                # Brief delay to ensure disconnection completes
                await asyncio.sleep(0.5)
            except Exception as e:
                logger.error(f"Error disconnecting device: {e}")


@pytest_asyncio.fixture
async def connected_device(hardware_fixture):
    """Fixture that provides a connected device, ensuring cleanup
    
    This fixture yields (manager, device_id, device) to the test and ensures
    the device is properly disconnected after the test completes.
    """
    # Create a device manager
    manager = direct_meshtastic.DeviceManager()
    device_id = None
    
    try:
        # Connect to the device
        logger.info(f"Connecting to device on port {hardware_fixture}")
        device_id = await manager.connect_device("serial", {"port": hardware_fixture})
        
        if not device_id:
            pytest.skip(f"Failed to connect to device on port {hardware_fixture}")
            
        # Get the device interface
        device = manager.get_device(device_id)
        if not device or not device.connected:
            pytest.skip(f"Device not properly connected on port {hardware_fixture}")
            
        logger.info(f"Successfully connected to device with ID {device_id}")
        
        # Wait a moment for the connection to stabilize
        await asyncio.sleep(1)
        
        # Yield the necessary objects to the test
        # The test will run here and complete before continuing
        yield manager, device_id, device
        
    finally:
        # Clean up: disconnect the device AFTER test completes
        if device_id:
            logger.info(f"Disconnecting device with ID {device_id}")
            try:
                await manager.disconnect_device(device_id)
                # Brief delay to ensure disconnection completes
                await asyncio.sleep(0.5)
            except Exception as e:
                logger.error(f"Error disconnecting device: {e}")


@pytest.mark.hardware
@pytest.mark.skipif(not ENABLE_HARDWARE_TESTS, reason="Hardware tests disabled")
class TestHardwareDeviceConnectivity:
    """Tests that require actual Meshtastic hardware"""
    
    @pytest.mark.asyncio
    async def test_discover_devices(self, hardware_fixture):
        """Test that devices can be discovered"""
        # Create a device manager
        manager = direct_meshtastic.DeviceManager()
        
        # Discover devices
        devices = await manager.discover_serial_devices()
        
        # Verify at least one device was found
        assert len(devices) > 0
        assert hardware_fixture in devices
        
    @pytest.mark.asyncio
    async def test_connect_disconnect_device(self, hardware_fixture):
        """Test basic connection and disconnection"""
        # Create a device manager
        manager = direct_meshtastic.DeviceManager()
        
        # Connect to the device
        device_id = await manager.connect_device("serial", {"port": hardware_fixture})
        assert device_id is not None
        
        # Verify the device is connected
        device = manager.get_device(device_id)
        assert device is not None
        assert device.connected is True
        
        # Disconnect the device
        success = await manager.disconnect_device(device_id)
        assert success is True
        
        # Verify the device is disconnected
        device = manager.get_device(device_id)
        assert device is not None
        assert device.connected is False


@pytest.mark.hardware
@pytest.mark.skipif(not ENABLE_HARDWARE_TESTS, reason="Hardware tests disabled")
class TestHardwareDeviceOperations:
    """Tests for operations on a connected device"""
    
    @pytest.mark.asyncio
    async def test_get_device_info(self, connected_device):
        """Test retrieving device information"""
        manager, device_id, device = connected_device
        
        # Get device info
        info = manager.get_device_info(device_id)
        
        # Verify the information was retrieved
        assert info is not None
        assert "id" in info
        assert "connected" in info
        assert "type" in info
        assert info["connected"] is True
        assert info["type"] == "serial"
    
    @pytest.mark.asyncio
    async def test_get_device_config(self, connected_device):
        """Test retrieving device configuration"""
        manager, device_id, device = connected_device
        
        # Get device configuration
        config = device.get_config()
        
        # Verify the configuration was retrieved
        assert config is not None
        assert "device" in config
    
    @pytest.mark.asyncio
    async def test_get_device_channels(self, connected_device):
        """Test retrieving device channels"""
        manager, device_id, device = connected_device
        
        # Get device channels
        channels = device.get_channels()
        
        # Verify channels were retrieved
        assert channels is not None
        assert len(channels) > 0
        assert "settings" in channels[0]
    
    @pytest.mark.asyncio
    async def test_send_text_message(self, connected_device):
        """Test sending a text message"""
        manager, device_id, device = connected_device
        
        # Just test that we can send a message without errors
        # Don't wait for receipt, which can be unreliable on real hardware
        test_message = f"Test message {time.time()}"
        success = device.send_text(test_message)
        
        # Verify the message was sent without error
        assert success is True
        
        # Wait a short time for any background operations
        await asyncio.sleep(1)


@pytest.mark.hardware
@pytest.mark.skipif(not ENABLE_HARDWARE_TESTS, reason="Hardware tests disabled")
class TestHardwareErrorHandling:
    """Tests for error handling with hardware devices"""
    
    @pytest.mark.asyncio
    async def test_connect_invalid_port(self):
        """Test connecting to an invalid port"""
        # Create a device manager
        manager = direct_meshtastic.DeviceManager()
        
        # Try to connect to an invalid port
        device_id = await manager.connect_device("serial", {"port": "/dev/nonexistent"})
        
        # Verify the connection failed
        assert device_id is None
    
    @pytest.mark.asyncio
    async def test_disconnect_nonexistent_device(self):
        """Test disconnecting a device that doesn't exist"""
        # Create a device manager
        manager = direct_meshtastic.DeviceManager()
        
        # Try to disconnect a device that doesn't exist
        success = await manager.disconnect_device("nonexistent")
        
        # Verify the disconnection failed
        assert success is False 