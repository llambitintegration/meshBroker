"""
Meshtastic device API endpoints for mesh broker
"""
from fastapi import APIRouter, Depends, HTTPException, BackgroundTasks, Body
from fastapi.responses import JSONResponse
from typing import Dict, List, Optional, Any
import asyncio
import logging
from pydantic import BaseModel
from ..models.database import get_db
from .. import meshtastic_integration
from ..auth.auth_dependencies import get_api_key

# Pydantic models for request validation
class ConnectionRequest(BaseModel):
    connection_type: str
    connection_params: Dict[str, Any]
    device_id: Optional[str] = None

# Configure logging
logger = logging.getLogger(__name__)

router = APIRouter(
    prefix="/meshtastic",
    tags=["meshtastic"],
    responses={404: {"description": "Not found"}},
)

discovery_tasks = {}

@router.get("/nodes", response_model=List[Dict[str, Any]])
async def get_nodes(
    active_only: bool = True,
    group: Optional[str] = None,
    category: Optional[str] = None,
    api_key: str = Depends(get_api_key)
):
    """Get all Meshtastic nodes with optional filtering"""
    nodes = meshtastic_integration.get_nodes(
        active_only=active_only,
        group=group,
        category=category
    )
    return nodes

@router.get("/nodes/{node_id}", response_model=Dict[str, Any])
async def get_node(
    node_id: str,
    api_key: str = Depends(get_api_key)
):
    """Get a specific Meshtastic node by ID"""
    node = meshtastic_integration.get_node(node_id)
    if not node:
        raise HTTPException(status_code=404, detail=f"Node with ID {node_id} not found")
    return node

@router.post("/discover", response_model=Dict[str, Any])
async def discover_devices(
    background_tasks: BackgroundTasks,
    connection_type: str = "all",
    api_key: str = Depends(get_api_key)
):
    """
    Discover available Meshtastic devices
    
    This is an async endpoint that runs discovery in the background with logging
    and proper timeout handling
    """
    logger.info(f"Starting device discovery for {connection_type}")
    
    # Create a unique task ID for this discovery operation
    import time
    task_id = f"discovery_{int(time.time())}"
    
    # Initialize the result dictionary
    discovery_tasks[task_id] = {
        "status": "in_progress",
        "progress": 0,
        "result": None,
        "error": None
    }
    
    # Start the background discovery task
    background_tasks.add_task(
        run_discovery_in_background,
        task_id,
        connection_type
    )
    
    # Return the task ID and initial status
    return {
        "success": True, 
        "message": "Device discovery started",
        "task_id": task_id,
        "status": "in_progress"
    }

@router.get("/discover/{task_id}", response_model=Dict[str, Any])
async def get_discovery_status(
    task_id: str,
    api_key: str = Depends(get_api_key)
):
    """Get the status and result of a device discovery task"""
    if task_id not in discovery_tasks:
        raise HTTPException(status_code=404, detail=f"Discovery task {task_id} not found")
    
    task_info = discovery_tasks[task_id]
    
    # Clean up old tasks that are complete
    for old_task_id in list(discovery_tasks.keys()):
        if old_task_id != task_id and discovery_tasks[old_task_id]["status"] in ["complete", "error"]:
            # Keep the 5 most recent completed tasks
            if len([t for t in discovery_tasks.values() if t["status"] in ["complete", "error"]]) > 5:
                del discovery_tasks[old_task_id]
    
    return {
        "task_id": task_id,
        "status": task_info["status"],
        "progress": task_info["progress"],
        "result": task_info["result"],
        "error": task_info["error"]
    }

@router.get("/devices", response_model=List[Dict[str, Any]])
async def get_connected_devices(
    api_key: str = Depends(get_api_key)
):
    """Get all connected Meshtastic devices"""
    devices = meshtastic_integration.get_connected_devices()
    return devices

@router.post("/connect", response_model=Dict[str, Any])
async def connect_device(
    request: ConnectionRequest = Body(...),
    api_key: str = Depends(get_api_key)
):
    """Connect to a Meshtastic device"""
    device_id = await meshtastic_integration.connect_device(
        connection_type=request.connection_type,
        connection_params=request.connection_params,
        device_id=request.device_id
    )
    
    if device_id:
        return {
            "success": True,
            "device_id": device_id,
            "message": f"Successfully connected to device {device_id}"
        }
    else:
        raise HTTPException(
            status_code=500,
            detail="Failed to connect to device. Check the connection parameters and try again."
        )

@router.delete("/devices/{device_id}", response_model=Dict[str, Any])
async def disconnect_device(
    device_id: str,
    api_key: str = Depends(get_api_key)
):
    """Disconnect from a Meshtastic device"""
    success = await meshtastic_integration.disconnect_device(device_id)
    
    if success:
        return {
            "success": True,
            "message": f"Successfully disconnected from device {device_id}"
        }
    else:
        raise HTTPException(
            status_code=500,
            detail=f"Failed to disconnect from device {device_id}"
        )

async def run_discovery_in_background(task_id: str, connection_type: str):
    """Run device discovery in the background and update the task status"""
    logger.info(f"Background discovery task {task_id} started for {connection_type}")
    
    try:
        # Update progress
        discovery_tasks[task_id]["progress"] = 10
        
        # Run the discovery with a timeout of 30 seconds to match frontend
        try:
            result = await asyncio.wait_for(
                meshtastic_integration.discover_devices(connection_type),
                timeout=30.0
            )
            
            # Update task with the result
            discovery_tasks[task_id]["status"] = "complete"
            discovery_tasks[task_id]["progress"] = 100
            discovery_tasks[task_id]["result"] = result
            
            logger.info(f"Discovery task {task_id} completed successfully")
            
        except asyncio.TimeoutError:
            logger.error(f"Discovery task {task_id} timed out after 30 seconds")
            discovery_tasks[task_id]["status"] = "error"
            discovery_tasks[task_id]["error"] = "Discovery operation timed out after 30 seconds"
            
    except Exception as e:
        logger.error(f"Error in discovery task {task_id}: {str(e)}")
        discovery_tasks[task_id]["status"] = "error"
        discovery_tasks[task_id]["error"] = f"Error during discovery: {str(e)}" 