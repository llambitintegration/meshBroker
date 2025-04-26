"""
Tests for Meshtastic router using simulated devices
"""
import pytest
import time
import json
import asyncio
from unittest.mock import patch, AsyncMock, MagicMock
from fastapi import FastAPI
from fastapi.testclient import TestClient

from backend.routers.meshtastic import router, discovery_tasks
from backend.auth.auth_dependencies import get_api_key
import backend.direct_meshtastic as direct_meshtastic
from backend.tests.test_direct_meshtastic import MockSerialInterface

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

# Fixture to clear discovery tasks between tests
@pytest.fixture(autouse=True)
def clear_discovery_tasks():
    discovery_tasks.clear()
    yield
    discovery_tasks.clear()

# Fixture to mock findPorts to return simulated device ports
@pytest.fixture
def mock_findPorts():
    mock_ports = [
        '/dev/mock_port_1',
        '/dev/mock_port_2'
    ]
    
    with patch('meshtastic.util.findPorts', return_value=mock_ports):
        yield mock_ports

# Fixture to mock SerialInterface with our custom implementation
@pytest.fixture
def mock_serial_interface():
    with patch('meshtastic.serial_interface.SerialInterface', MockSerialInterface):
        yield

# Combined fixture to simulate device discovery and connection
@pytest.fixture
def simulated_device_environment(mock_findPorts, mock_serial_interface):
    yield

@pytest.mark.simulation
class TestMeshtasticRouterSimulation:
    """Tests for the Meshtastic router using simulated devices"""
    
    def test_discover_simulated_devices(self, test_client, simulated_device_environment):
        """Test device discovery with simulated devices"""
        # Start discovery
        response = test_client.post("/meshtastic/discover?connection_type=serial")
        assert response.status_code == 200
        data = response.json()
        task_id = data["task_id"]
        
        # Wait for the background task to complete (should be quick with mocks)
        start_time = time.time()
        status_data = None
        
        while time.time() - start_time < 5:  # Wait up to 5 seconds
            status_response = test_client.get(f"/meshtastic/discover/{task_id}")
            status_data = status_response.json()
            
            if status_data["status"] in ["complete", "error"]:
                break
                
            # Wait before polling again
            time.sleep(0.2)
        
        # Verify discovery completed
        assert status_data is not None
        assert status_data["status"] == "complete"
        assert "serial" in status_data["result"]
        assert len(status_data["result"]["serial"]) == 2
        
        # Verify the expected ports were found
        ports = [device["port"] for device in status_data["result"]["serial"]]
        assert "/dev/mock_port_1" in ports
        assert "/dev/mock_port_2" in ports
    
    def test_connect_to_simulated_device(self, test_client, simulated_device_environment):
        """Test connecting to a simulated device"""
        # Connect to the simulated device
        connection_data = {
            "connection_type": "serial",
            "connection_params": {"port": "/dev/mock_port_1"}
        }
        
        response = test_client.post(
            "/meshtastic/connect",
            json=connection_data
        )
        
        assert response.status_code == 200
        data = response.json()
        assert data["success"] is True
        assert "device_id" in data
        device_id = data["device_id"]
        
        # Get the connected devices to verify
        devices_response = test_client.get("/meshtastic/devices")
        assert devices_response.status_code == 200
        devices = devices_response.json()
        
        # Verify our device is in the list
        device_found = False
        for device in devices:
            if device["id"] == device_id:
                device_found = True
                assert device["connected"] is True
                assert device["connection_type"] == "serial"
                break
        
        assert device_found, f"Device {device_id} not found in connected devices list"
        
        # Now disconnect from the device
        disconnect_response = test_client.delete(f"/meshtastic/devices/{device_id}")
        assert disconnect_response.status_code == 200
        disconnect_data = disconnect_response.json()
        assert disconnect_data["success"] is True
    
    def test_get_simulated_nodes(self, test_client, simulated_device_environment):
        """Test getting nodes from simulated device"""
        # First connect to the simulated device
        connection_data = {
            "connection_type": "serial",
            "connection_params": {"port": "/dev/mock_port_1"}
        }
        
        connect_response = test_client.post(
            "/meshtastic/connect",
            json=connection_data
        )
        
        assert connect_response.status_code == 200
        device_id = connect_response.json()["device_id"]
        
        try:
            # Get nodes (should include the simulated device node)
            nodes_response = test_client.get("/meshtastic/nodes")
            assert nodes_response.status_code == 200
            nodes = nodes_response.json()
            
            # The mock device creates at least one node (itself)
            assert len(nodes) > 0
            
            # Verify node data structure
            assert "node_id" in nodes[0]
            assert "name" in nodes[0]
            
        finally:
            # Clean up by disconnecting
            test_client.delete(f"/meshtastic/devices/{device_id}")

    def test_error_handling_nonexistent_port(self, test_client, simulated_device_environment):
        """Test error handling when connecting to a nonexistent port"""
        # Try to connect to a port that doesn't exist
        connection_data = {
            "connection_type": "serial",
            "connection_params": {"port": "/dev/nonexistent"}
        }
        
        # Override the connect method to fail for nonexistent ports
        original_connect = direct_meshtastic.DeviceInterface.connect
        
        def mock_connect(self):
            if self.connection_params.get("port") == "/dev/nonexistent":
                return False
            return original_connect(self)
        
        with patch('backend.direct_meshtastic.DeviceInterface.connect', mock_connect):
            response = test_client.post(
                "/meshtastic/connect",
                json=connection_data
            )
            
            assert response.status_code == 500
            assert "detail" in response.json()
    
    def test_multiple_devices_management(self, test_client, simulated_device_environment):
        """Test managing multiple connected devices"""
        device_ids = []
        
        try:
            # Connect to first device
            connection_data1 = {
                "connection_type": "serial",
                "connection_params": {"port": "/dev/mock_port_1"}
            }
            
            response1 = test_client.post(
                "/meshtastic/connect",
                json=connection_data1
            )
            
            assert response1.status_code == 200
            device_ids.append(response1.json()["device_id"])
            
            # Connect to second device
            connection_data2 = {
                "connection_type": "serial",
                "connection_params": {"port": "/dev/mock_port_2"}
            }
            
            response2 = test_client.post(
                "/meshtastic/connect",
                json=connection_data2
            )
            
            assert response2.status_code == 200
            device_ids.append(response2.json()["device_id"])
            
            # Verify both devices are connected
            devices_response = test_client.get("/meshtastic/devices")
            assert devices_response.status_code == 200
            devices = devices_response.json()
            
            assert len(devices) >= 2
            
            connected_ids = [device["id"] for device in devices if device["connected"]]
            for device_id in device_ids:
                assert device_id in connected_ids
                
        finally:
            # Clean up by disconnecting all devices
            for device_id in device_ids:
                test_client.delete(f"/meshtastic/devices/{device_id}")

@pytest.mark.simulation
@pytest.mark.asyncio
class TestRouterBackgroundTasksSimulation:
    """Tests for router background tasks using simulated devices"""
    
    async def test_run_discovery_background_with_simulated_devices(self, mock_findPorts, mock_serial_interface):
        """Test the background discovery task with simulated devices"""
        from backend.routers.meshtastic import run_discovery_in_background
        
        # Create a task
        task_id = "test_simulated_discovery"
        discovery_tasks[task_id] = {
            "status": "in_progress",
            "progress": 0,
            "result": None,
            "error": None
        }
        
        # Run the discovery task
        await run_discovery_in_background(task_id, "serial")
        
        # Verify the task completed successfully
        assert discovery_tasks[task_id]["status"] == "complete"
        assert discovery_tasks[task_id]["progress"] == 100
        
        # Verify the expected ports were found
        result = discovery_tasks[task_id]["result"]
        assert "serial" in result
        
        ports = [device["port"] for device in result["serial"]]
        assert "/dev/mock_port_1" in ports
        assert "/dev/mock_port_2" in ports