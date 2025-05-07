"""
Integration tests for Meshtastic router with actual device connections
"""
import pytest
import os
import time
import asyncio
import logging
import meshtastic.util
from fastapi.testclient import TestClient
from fastapi import FastAPI

from backend.routers.meshtastic import router
from backend.auth.auth_dependencies import get_api_key
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
HARDWARE_TIMEOUT = int(os.environ.get("HARDWARE_TIMEOUT", "30"))

# Override the API key dependency for testing
async def override_get_api_key():
    return "test_api_key"

# Create test app with the router
@pytest.fixture
def test_app():
    app = FastAPI()
    app.include_router(router)
    app.dependency_overrides[get_api_key] = override_get_api_key
    return app

@pytest.fixture
def test_client(test_app):
    return TestClient(test_app)

def check_hardware_available() -> list:
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

@pytest.mark.hardware
@pytest.mark.skipif(not ENABLE_HARDWARE_TESTS, reason="Hardware tests disabled")
class TestMeshtasticRouterWithHardware:
    """Integration tests for Meshtastic router with actual hardware"""
    
    def test_discover_devices(self, test_client, hardware_fixture):
        """Test device discovery with actual hardware"""
        # Start a discovery task
        response = test_client.post("/meshtastic/discover?connection_type=serial")
        assert response.status_code == 200
        data = response.json()
        assert data["success"] is True
        task_id = data["task_id"]
        
        # Poll for discovery completion
        start_time = time.time()
        while time.time() - start_time < HARDWARE_TIMEOUT:
            status_response = test_client.get(f"/meshtastic/discover/{task_id}")
            assert status_response.status_code == 200
            status_data = status_response.json()
            
            if status_data["status"] in ["complete", "error"]:
                break
                
            # Wait before polling again
            time.sleep(1)
        
        # Verify discovery completed and found devices
        assert status_data["status"] == "complete", f"Discovery failed: {status_data.get('error', 'Unknown error')}"
        assert "serial" in status_data["result"]
        assert len(status_data["result"]["serial"]) > 0
        
        # Verify our test port is in the results
        port_found = False
        for device in status_data["result"]["serial"]:
            if device["port"] == hardware_fixture:
                port_found = True
                break
        
        assert port_found, f"Expected port {hardware_fixture} not found in discovery results"
    
    def test_connect_disconnect_device(self, test_client, hardware_fixture):
        """Test connecting to and disconnecting from a device"""
        # Connect to the device
        connection_data = {
            "connection_type": "serial",
            "connection_params": {"port": hardware_fixture}
        }
        
        connect_response = test_client.post(
            "/meshtastic/connect",
            json=connection_data
        )
        
        assert connect_response.status_code == 200
        connect_data = connect_response.json()
        assert connect_data["success"] is True
        device_id = connect_data["device_id"]
        
        # Verify the device is in connected devices list
        devices_response = test_client.get("/meshtastic/devices")
        assert devices_response.status_code == 200
        devices_data = devices_response.json()
        
        device_found = False
        for device in devices_data:
            if device["id"] == device_id:
                device_found = True
                assert device["connected"] is True
                assert device["connection_type"] == "serial"
                break
        
        assert device_found, f"Device {device_id} not found in connected devices list"
        
        # Disconnect from the device
        disconnect_response = test_client.delete(f"/meshtastic/devices/{device_id}")
        assert disconnect_response.status_code == 200
        disconnect_data = disconnect_response.json()
        assert disconnect_data["success"] is True
        
        # Verify the device is disconnected
        devices_response = test_client.get("/meshtastic/devices")
        assert devices_response.status_code == 200
        devices_data = devices_response.json()
        
        if devices_data:  # If there are still devices in the list
            for device in devices_data:
                if device["id"] == device_id:
                    assert device["connected"] is False, "Device should be disconnected"
    
    def test_get_nodes_from_device(self, test_client, hardware_fixture):
        """Test getting nodes from a connected device"""
        # First connect to the device
        connection_data = {
            "connection_type": "serial",
            "connection_params": {"port": hardware_fixture}
        }
        
        connect_response = test_client.post(
            "/meshtastic/connect",
            json=connection_data
        )
        
        assert connect_response.status_code == 200
        device_id = connect_response.json()["device_id"]
        
        try:
            # Wait a moment for node information to be synced
            time.sleep(2)
            
            # Get all nodes
            nodes_response = test_client.get("/meshtastic/nodes")
            assert nodes_response.status_code == 200
            nodes_data = nodes_response.json()
            
            # There should be at least one node (the connected device itself)
            assert len(nodes_data) > 0
            
            # The node should have a node_id and name
            assert "node_id" in nodes_data[0]
            assert "name" in nodes_data[0]
            
        finally:
            # Clean up: disconnect the device
            test_client.delete(f"/meshtastic/devices/{device_id}")

@pytest.mark.asyncio
@pytest.mark.hardware
@pytest.mark.skipif(not ENABLE_HARDWARE_TESTS, reason="Hardware tests disabled")
class TestDeviceDiscoveryWithHardware:
    """Tests for device discovery with actual hardware using direct calls"""
    
    async def test_device_discovery_with_timeout(self, hardware_fixture):
        """Test device discovery with timeout handling"""
        from backend.routers.meshtastic import run_discovery_in_background
        
        # Create a test task ID
        task_id = "test_hardware_discovery"
        from backend.routers.meshtastic import discovery_tasks
        discovery_tasks[task_id] = {
            "status": "in_progress",
            "progress": 0,
            "result": None,
            "error": None
        }
        
        # Run the background task
        await run_discovery_in_background(task_id, "serial")
        
        # Check that the task was updated correctly
        assert discovery_tasks[task_id]["status"] == "complete"
        assert discovery_tasks[task_id]["progress"] == 100
        assert discovery_tasks[task_id]["result"] is not None
        assert "serial" in discovery_tasks[task_id]["result"]
        
        # Verify our test port is in the results
        port_found = False
        for device in discovery_tasks[task_id]["result"]["serial"]:
            if device["port"] == hardware_fixture:
                port_found = True
                break
        
        assert port_found, f"Expected port {hardware_fixture} not found in discovery results"

@pytest.mark.hardware
@pytest.mark.skipif(not ENABLE_HARDWARE_TESTS, reason="Hardware tests disabled")
class TestErrorHandlingWithHardware:
    """Tests for error handling with actual hardware"""
    
    def test_connect_invalid_device(self, test_client):
        """Test connecting to an invalid device"""
        connection_data = {
            "connection_type": "serial",
            "connection_params": {"port": "/dev/nonexistent"}
        }
        
        response = test_client.post(
            "/meshtastic/connect",
            json=connection_data
        )
        
        # Check that we get a valid error response
        assert response.status_code in [404, 500], f"Got unexpected status code {response.status_code}"
        assert "detail" in response.json()
    
    def test_disconnect_nonexistent_device(self, test_client):
        """Test disconnecting a nonexistent device"""
        response = test_client.delete("/meshtastic/devices/nonexistent_device_id")
        
        assert response.status_code == 500
        assert "detail" in response.json()
    
    def test_get_nonexistent_node(self, test_client):
        """Test getting a node that doesn't exist"""
        response = test_client.get("/meshtastic/nodes/nonexistent_node_id")
        
        assert response.status_code == 404
        assert "detail" in response.json()