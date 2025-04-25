from fastapi import APIRouter, Depends, HTTPException, Query, Request
from typing import Dict, List, Optional, Any
import logging
import time
from ..auth import JWTBearer, APIKeyAuth
from ..mqtt.mqtt_handler import MQTTHandler  # Import MQTT handler

router = APIRouter(
    prefix="/status",
    tags=["status"],
    responses={404: {"description": "Not found"}},
)

logger = logging.getLogger(__name__)

# Variable to store MQTT handler reference
mqtt_handler = None

def set_mqtt_handler(handler: MQTTHandler):
    """Set the MQTT handler reference"""
    global mqtt_handler
    mqtt_handler = handler
    logger.info("MQTT handler reference set in status router")

@router.get("/", response_model=Dict[str, Any])
async def get_api_status():
    """
    Get the current status of the API
    
    Returns a status object with connection information and timestamps.
    """
    mqtt_connected = False
    if mqtt_handler:
        mqtt_connected = mqtt_handler.is_connected()
    
    # Build the status response
    status = {
        "status": "connected",
        "timestamp": int(time.time()),
        "version": "1.0.0",
        "services": {
            "mqtt": {
                "status": "connected" if mqtt_connected else "disconnected"
            }
        }
    }
    
    return status

@router.get("/health", response_model=Dict[str, str])
async def health_check():
    """
    Simple health check endpoint for monitoring systems
    
    Returns a simple status message.
    """
    return {"status": "healthy"} 