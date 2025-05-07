"""
Tests for the Meshtastic router API endpoints
"""
import pytest
import asyncio
import json
import time
from unittest.mock import patch, MagicMock, AsyncMock
from fastapi import FastAPI, Depends, BackgroundTasks
from fastapi.testclient import TestClient

from backend.routers.meshtastic import router, discovery_tasks
from backend.auth.auth_dependencies import get_api_key

# Sample mock data for testing
MOCK_NODES = [
    {
        "node_id": "node1",
        "name": "Test Node 1",
        "short_name": "TN1",
        "hardware": "TBEAM",
        "group": "test",
        "category": "device",
        "is_active": True,
        "last_seen": int(time.time()),
        "message_count": 5,
        "position": {
            "latitude": 37.7749,
            "longitude": -122.4194,
            "altitude": 0,
            "timestamp": int(time.time())
        }
    },
    {
        "node_id": "node2",
        "name": "Test Node 2",
        "short_name": "TN2",
        "hardware": "TLORA",
        "group": "test",
        "category": "device",
        "is_active": False,
        "last_seen": int(time.time()) - 7200,  # 2 hours ago
        "message_count": 2
    }
]

MOCK_DEVICES = [
    {
        "id": "serial_1234567890",
        "connection_type": "serial",
        "type": "serial",
        "connection_params": {"port": "/dev/mock_port_1"},
        "connected": True,
        "node_id": "abc123",
        "is_default": True
    },
    {
        "id": "ble_1234567890",
        "connection_type": "ble",
        "type": "ble",
        "connection_params": {"address": "00:11:22:33:44:55"},
        "connected": True,
        "node_id": "def456",
        "is_default": False
    }
]

MOCK_SERIAL_DISCOVERY = {
    "success": True,
    "serial": [
        {"port": "/dev/mock_port_1"},
        {"port": "/dev/mock_port_2"}
    ]
}

MOCK_BLE_DISCOVERY = {
    "success": True,
    "ble": [
        {"name": "Meshtastic_abc123", "address": "00:11:22:33:44:55"},
        {"name": "Meshtastic_def456", "address": "66:77:88:99:AA:BB"}
    ]
}

MOCK_EMPTY_DISCOVERY = {
    "success": True,
    "serial": [],
    "ble": [],
    "message": "No devices found. Please check your connections and try again."
}

MOCK_ERROR_DISCOVERY = {
    "success": False,
    "error": "Device discovery failed: Permission denied"
}


# Override the API key dependency for testing
async def override_get_api_key():
    return "test_api_key"

# We'll patch this in the test instead of globally to avoid breaking other tests


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


# Patch meshtastic_integration for all tests in this module
@pytest.fixture
def mock_meshtastic_integration():
    with patch("backend.routers.meshtastic.meshtastic_integration") as mock_integration:
        # Mock the get_nodes function
        mock_integration.get_nodes.return_value = MOCK_NODES
        
        # Mock the get_node function
        def mock_get_node(node_id):
            for node in MOCK_NODES:
                if node["node_id"] == node_id:
                    return node
            return None
        mock_integration.get_node.side_effect = mock_get_node
        
        # Mock the get_connected_devices function
        mock_integration.get_connected_devices.return_value = MOCK_DEVICES
        
        # Mock the discover_devices function as an AsyncMock
        mock_discover = AsyncMock()
        mock_discover.return_value = {**MOCK_SERIAL_DISCOVERY, **MOCK_BLE_DISCOVERY}
        mock_integration.discover_devices = mock_discover
        
        # Mock the connect_device function as an AsyncMock
        mock_connect = AsyncMock()
        mock_connect.return_value = "test_device_id"
        mock_integration.connect_device = mock_connect
        
        # Mock the disconnect_device function as an AsyncMock
        mock_disconnect = AsyncMock()
        mock_disconnect.return_value = True
        mock_integration.disconnect_device = mock_disconnect
        
        yield mock_integration


class TestMeshtasticRouter:
    """Tests for the Meshtastic router endpoints"""
    
    # Remove the setup and teardown methods since they're causing issues
    
    def test_get_nodes(self, test_client, mock_meshtastic_integration):
        """Test getting all nodes"""
        response = test_client.get("/meshtastic/nodes")
        assert response.status_code == 200
        assert response.json() == MOCK_NODES
        mock_meshtastic_integration.get_nodes.assert_called_once_with(
            active_only=True, group=None, category=None
        )
    
    def test_get_nodes_with_filters(self, test_client, mock_meshtastic_integration):
        """Test getting nodes with filters"""
        response = test_client.get(
            "/meshtastic/nodes?active_only=false&group=test&category=device"
        )
        assert response.status_code == 200
        mock_meshtastic_integration.get_nodes.assert_called_once_with(
            active_only=False, group="test", category="device"
        )
    
    def test_get_node_exists(self, test_client, mock_meshtastic_integration):
        """Test getting a specific node that exists"""
        response = test_client.get("/meshtastic/nodes/node1")
        assert response.status_code == 200
        assert response.json() == MOCK_NODES[0]
        mock_meshtastic_integration.get_node.assert_called_once_with("node1")
    
    def test_get_node_not_found(self, test_client, mock_meshtastic_integration):
        """Test getting a node that doesn't exist"""
        response = test_client.get("/meshtastic/nodes/nonexistent")
        assert response.status_code == 404
        assert "detail" in response.json()
        mock_meshtastic_integration.get_node.assert_called_once_with("nonexistent")
    
    def test_get_connected_devices(self, test_client, mock_meshtastic_integration):
        """Test getting all connected devices"""
        response = test_client.get("/meshtastic/devices")
        assert response.status_code == 200
        assert response.json() == MOCK_DEVICES
        mock_meshtastic_integration.get_connected_devices.assert_called_once()
    
    def test_discover_devices_start(self, test_client, mock_meshtastic_integration):
        """Test starting device discovery"""
        # Patch the BackgroundTasks.add_task to prevent it from executing
        with patch('backend.routers.meshtastic.BackgroundTasks.add_task') as mock_add_task:
            response = test_client.post("/meshtastic/discover?connection_type=all")
            assert response.status_code == 200
            data = response.json()
            assert data["success"] is True
            assert "task_id" in data
            assert data["status"] == "in_progress"
            
            # Verify mock was called
            mock_add_task.assert_called_once()
            
            # Get the task ID for checking
            task_id = data["task_id"]
            assert task_id in discovery_tasks
        
        # The actual discover_devices function shouldn't be called yet
        # because it runs in the background
        mock_meshtastic_integration.discover_devices.assert_not_called()
    
    def test_discover_devices_status_in_progress(self, test_client):
        """Test getting discovery status while in progress"""
        # Create a mock task
        task_id = "discovery_test_in_progress"
        discovery_tasks[task_id] = {
            "status": "in_progress",
            "progress": 50,
            "result": None,
            "error": None
        }
        
        response = test_client.get(f"/meshtastic/discover/{task_id}")
        assert response.status_code == 200
        data = response.json()
        assert data["status"] == "in_progress"
        assert data["progress"] == 50
        assert data["result"] is None
        assert data["error"] is None
    
    def test_discover_devices_status_complete(self, test_client):
        """Test getting discovery status when complete"""
        # Create a mock completed task
        task_id = "discovery_test_complete"
        discovery_tasks[task_id] = {
            "status": "complete",
            "progress": 100,
            "result": {**MOCK_SERIAL_DISCOVERY, **MOCK_BLE_DISCOVERY},
            "error": None
        }
        
        response = test_client.get(f"/meshtastic/discover/{task_id}")
        assert response.status_code == 200
        data = response.json()
        assert data["status"] == "complete"
        assert data["progress"] == 100
        assert data["result"] == {**MOCK_SERIAL_DISCOVERY, **MOCK_BLE_DISCOVERY}
        assert data["error"] is None
    
    def test_discover_devices_status_error(self, test_client):
        """Test getting discovery status when error occurred"""
        # Create a mock task with error
        task_id = "discovery_test_error"
        discovery_tasks[task_id] = {
            "status": "error",
            "progress": 30,
            "result": None,
            "error": "Discovery operation timed out after 30 seconds"
        }
        
        response = test_client.get(f"/meshtastic/discover/{task_id}")
        assert response.status_code == 200
        data = response.json()
        assert data["status"] == "error"
        assert data["progress"] == 30
        assert data["result"] is None
        assert data["error"] == "Discovery operation timed out after 30 seconds"
    
    def test_discover_devices_status_not_found(self, test_client):
        """Test getting discovery status for non-existent task"""
        response = test_client.get("/meshtastic/discover/nonexistent_task")
        assert response.status_code == 404
        assert "detail" in response.json()
    
    def test_connect_device_success(self, test_client, mock_meshtastic_integration):
        """Test connecting to a device successfully"""
        # Pass the parameters in the request body as expected by Pydantic model
        response = test_client.post(
            "/meshtastic/connect",
            json={
                "connection_type": "serial",
                "connection_params": {"port": "/dev/mock_port_1"}
            }
        )
        
        assert response.status_code == 200
        data = response.json()
        assert data["success"] is True
        assert data["device_id"] == "test_device_id"
        
        # Verify the integration function was called correctly
        mock_meshtastic_integration.connect_device.assert_called_once()
        
        # Check the parameters were passed correctly
        call_args = mock_meshtastic_integration.connect_device.call_args[1]
        assert call_args["connection_type"] == "serial"
        assert "connection_params" in call_args
        assert call_args["connection_params"]["port"] == "/dev/mock_port_1"
    
    def test_connect_device_failure(self, test_client, mock_meshtastic_integration):
        """Test connecting to a device with failure"""
        # Make the connect_device function return None to simulate failure
        mock_meshtastic_integration.connect_device.return_value = None
        
        # Pass the parameters in the request body as expected by Pydantic model
        response = test_client.post(
            "/meshtastic/connect",
            json={
                "connection_type": "serial",
                "connection_params": {"port": "/dev/nonexistent"}
            }
        )
        
        assert response.status_code == 500
        assert "detail" in response.json()
        
        # Verify the integration function was called correctly
        mock_meshtastic_integration.connect_device.assert_called_once()
        
        # Check the parameters were passed correctly
        call_args = mock_meshtastic_integration.connect_device.call_args[1]
        assert call_args["connection_type"] == "serial"
        assert "connection_params" in call_args
        assert call_args["connection_params"]["port"] == "/dev/nonexistent"
    
    def test_disconnect_device_success(self, test_client, mock_meshtastic_integration):
        """Test disconnecting from a device successfully"""
        response = test_client.delete("/meshtastic/devices/test_device_id")
        
        assert response.status_code == 200
        data = response.json()
        assert data["success"] is True
        
        # Verify the integration function was called correctly
        mock_meshtastic_integration.disconnect_device.assert_called_once_with("test_device_id")
    
    def test_disconnect_device_failure(self, test_client, mock_meshtastic_integration):
        """Test disconnecting from a device with failure"""
        # Make the disconnect_device function return False to simulate failure
        mock_meshtastic_integration.disconnect_device.return_value = False
        
        response = test_client.delete("/meshtastic/devices/nonexistent")
        
        assert response.status_code == 500
        assert "detail" in response.json()
        
        # Verify the integration function was called correctly
        mock_meshtastic_integration.disconnect_device.assert_called_once_with("nonexistent")


@pytest.mark.asyncio
class TestBackgroundTasks:
    """Tests for background task functionality"""
    
    async def test_run_discovery_in_background_success(self, mock_meshtastic_integration):
        """Test running discovery in background with success"""
        from backend.routers.meshtastic import run_discovery_in_background
        
        # Set up the mock result
        mock_meshtastic_integration.discover_devices.return_value = {
            **MOCK_SERIAL_DISCOVERY, 
            **MOCK_BLE_DISCOVERY
        }
        
        # Create a test task ID
        task_id = "test_background_success"
        discovery_tasks[task_id] = {
            "status": "in_progress",
            "progress": 0,
            "result": None,
            "error": None
        }
        
        # Run the background task
        await run_discovery_in_background(task_id, "all")
        
        # Check that the task was updated correctly
        assert discovery_tasks[task_id]["status"] == "complete"
        assert discovery_tasks[task_id]["progress"] == 100
        assert discovery_tasks[task_id]["result"] == {
            **MOCK_SERIAL_DISCOVERY, 
            **MOCK_BLE_DISCOVERY
        }
        assert discovery_tasks[task_id]["error"] is None
        
        # Verify the integration function was called correctly
        mock_meshtastic_integration.discover_devices.assert_called_once_with("all")
    
    async def test_run_discovery_in_background_error(self, mock_meshtastic_integration):
        """Test running discovery in background with error"""
        from backend.routers.meshtastic import run_discovery_in_background
        
        # Set up the mock to raise an exception
        mock_meshtastic_integration.discover_devices.side_effect = Exception("Test error")
        
        # Create a test task ID
        task_id = "test_background_error"
        discovery_tasks[task_id] = {
            "status": "in_progress",
            "progress": 0,
            "result": None,
            "error": None
        }
        
        # Run the background task
        await run_discovery_in_background(task_id, "all")
        
        # Check that the task was updated correctly
        assert discovery_tasks[task_id]["status"] == "error"
        assert "Test error" in discovery_tasks[task_id]["error"]
        
        # Verify the integration function was called correctly
        mock_meshtastic_integration.discover_devices.assert_called_once_with("all")
    
    async def test_run_discovery_in_background_timeout(self, mock_meshtastic_integration):
        """Test running discovery in background with timeout"""
        from backend.routers.meshtastic import run_discovery_in_background

        # Set up a mock directly for asyncio.wait_for that always raises TimeoutError
        # for the specific call in run_discovery_in_background
        async def mock_wait_for_timeout(coro, timeout):
            # Just raise TimeoutError
            raise asyncio.TimeoutError()

        # Create a test task ID
        task_id = "test_background_timeout"
        discovery_tasks[task_id] = {
            "status": "in_progress",
            "progress": 0,
            "result": None,
            "error": None
        }

        # Patch asyncio.wait_for to return TimeoutError
        with patch('backend.routers.meshtastic.asyncio.wait_for', side_effect=mock_wait_for_timeout):
            # Run the background task
            await run_discovery_in_background(task_id, "all")
        
        # Check that the task was updated correctly
        assert discovery_tasks[task_id]["status"] == "error"
        assert "timed out" in discovery_tasks[task_id]["error"]
        
        # Verify the integration function was called
        mock_meshtastic_integration.discover_devices.assert_called_once_with("all")