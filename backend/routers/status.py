from fastapi import APIRouter, Depends
from typing import Dict, Any
import os
import sys
import platform

# Add a broker status check
from .. import meshtastic_integration

router = APIRouter(
    prefix="/status",
    tags=["status"],
    responses={404: {"description": "Not found"}},
)

# MQTT handler reference
mqtt_handler = None

def set_mqtt_handler(handler):
    """Set the MQTT handler reference for status checks"""
    global mqtt_handler
    mqtt_handler = handler

@router.get("/")
def get_status() -> Dict[str, Any]:
    """Get the current status of the API and MQTT broker"""
    # Determine if MQTT is connected based on the meshtastic integration
    mqtt_status = "disconnected"
    mqtt_details = {}
    
    try:
        # Check if meshtastic integration is initialized
        if meshtastic_integration.message_router is not None:
            mqtt_status = "connected"
            mqtt_details = {
                "connection_mode": meshtastic_integration.CONNECTION_MODE,
                "node_count": meshtastic_integration.get_node_count()
            }
    except Exception as e:
        mqtt_status = "error"
        mqtt_details = {"error": str(e)}
    
    # Get system information
    sys_info = {
        "python": sys.version.split()[0],
        "os": platform.system(),
        "platform": platform.platform(),
        "hostname": platform.node()
    }
    
    # Get environment variables (filter sensitive ones)
    env_info = {
        "MESHTASTIC_CONNECTION_MODE": os.environ.get("MESHTASTIC_CONNECTION_MODE", "hybrid"),
        "NODE_EXPIRATION_SECONDS": os.environ.get("NODE_EXPIRATION_SECONDS", "3600")
    }
    
    return {
        "api_status": "ok",
        "mqtt_status": mqtt_status,
        "mqtt_details": mqtt_details,
        "system_info": sys_info,
        "environment": env_info,
        "message": "API is running normally"
    }

@router.get("/broker")
def get_broker_status() -> Dict[str, Any]:
    """Get the MQTT broker connection status"""
    mqtt_status = "disconnected"
    mqtt_details = {}
    
    try:
        # Check if meshtastic integration is initialized
        if meshtastic_integration.message_router is not None:
            mqtt_status = "connected"
            mqtt_details = {
                "connection_mode": meshtastic_integration.CONNECTION_MODE,
                "node_count": meshtastic_integration.get_node_count()
            }
    except Exception as e:
        mqtt_status = "error"
        mqtt_details = {"error": str(e)}
    
    return {
        "status": mqtt_status,
        "details": mqtt_details
    } 