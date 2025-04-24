"""
MQTT broker settings management API
"""
from fastapi import APIRouter, Depends, HTTPException, status, BackgroundTasks
from pydantic import BaseModel
from typing import Optional, Dict, Any, List

import logging

from backend.models.user import User
from backend.auth.auth_handler import get_admin_user
from backend.auth.rate_limiter import RateLimiter
from backend.config import settings
from backend.mqtt_handler import MQTTHandler

# Configure logger
logger = logging.getLogger(__name__)

# Rate limiter
rate_limiter = RateLimiter(rate=20, per=60)  # 20 requests per minute

# Create router
router = APIRouter(prefix="/mqtt", tags=["MQTT Settings"])

# Global MQTT handler reference
mqtt_handler = None


def set_mqtt_handler(handler: MQTTHandler):
    """Set the global MQTT handler reference"""
    global mqtt_handler
    mqtt_handler = handler


# Models
class BrokerSettings(BaseModel):
    """MQTT broker settings model"""
    host: str
    port: int
    client_id: str
    username: Optional[str] = None
    password: Optional[str] = None
    use_tls: bool = False
    keepalive: int = 60
    default_qos: int = 1


class BrokerProfile(BaseModel):
    """MQTT broker profile model"""
    name: str
    settings: BrokerSettings
    is_active: bool = False


class ConnectionTestResult(BaseModel):
    """Connection test result model"""
    success: bool
    message: str
    details: Optional[Dict[str, Any]] = None


# Routes
@router.get("/broker", response_model=BrokerSettings)
async def get_broker_settings(
    current_user: User = Depends(get_admin_user),
    _: None = Depends(rate_limiter)
):
    """Get current MQTT broker settings (admin only)"""
    if not mqtt_handler:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="MQTT handler not initialized"
        )
    
    # Get current settings from handler
    return BrokerSettings(
        host=mqtt_handler.broker_host,
        port=mqtt_handler.broker_port,
        client_id=mqtt_handler.client_id,
        username=mqtt_handler.username,
        password="********" if mqtt_handler.password else None,  # Mask password for security
        use_tls=mqtt_handler.use_tls,
        keepalive=mqtt_handler.keepalive,
        default_qos=mqtt_handler.default_qos
    )


@router.put("/broker", response_model=BrokerSettings)
async def update_broker_settings(
    broker_settings: BrokerSettings,
    background_tasks: BackgroundTasks,
    current_user: User = Depends(get_admin_user),
    _: None = Depends(rate_limiter)
):
    """Update MQTT broker settings and reconnect (admin only)"""
    if not mqtt_handler:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="MQTT handler not initialized"
        )
    
    # Save current settings for potential rollback
    old_settings = {
        "broker_host": mqtt_handler.broker_host,
        "broker_port": mqtt_handler.broker_port,
        "client_id": mqtt_handler.client_id,
        "username": mqtt_handler.username,
        "password": mqtt_handler.password,
        "use_tls": mqtt_handler.use_tls,
        "keepalive": mqtt_handler.keepalive,
        "default_qos": mqtt_handler.default_qos
    }
    
    # Update settings
    mqtt_handler.broker_host = broker_settings.host
    mqtt_handler.broker_port = broker_settings.port
    mqtt_handler.client_id = broker_settings.client_id
    mqtt_handler.username = broker_settings.username
    mqtt_handler.password = broker_settings.password if broker_settings.password != "********" else mqtt_handler.password
    mqtt_handler.use_tls = broker_settings.use_tls
    mqtt_handler.keepalive = broker_settings.keepalive
    mqtt_handler.default_qos = broker_settings.default_qos
    
    # Reconnect in background
    logger.info(f"Admin {current_user.username} updated MQTT broker settings, reconnecting...")
    background_tasks.add_task(mqtt_handler.reconnect)
    
    # Return updated settings
    return BrokerSettings(
        host=mqtt_handler.broker_host,
        port=mqtt_handler.broker_port,
        client_id=mqtt_handler.client_id,
        username=mqtt_handler.username,
        password="********" if mqtt_handler.password else None,  # Mask password for security
        use_tls=mqtt_handler.use_tls,
        keepalive=mqtt_handler.keepalive,
        default_qos=mqtt_handler.default_qos
    )


@router.post("/broker/test", response_model=ConnectionTestResult)
async def test_broker_connection(
    broker_settings: Optional[BrokerSettings] = None,
    current_user: User = Depends(get_admin_user),
    _: None = Depends(rate_limiter)
):
    """Test connection to MQTT broker with current or provided settings (admin only)"""
    if not mqtt_handler:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="MQTT handler not initialized"
        )
    
    # If no settings provided, use current settings
    test_settings = broker_settings or BrokerSettings(
        host=mqtt_handler.broker_host,
        port=mqtt_handler.broker_port,
        client_id=f"{mqtt_handler.client_id}_test",
        username=mqtt_handler.username,
        password=mqtt_handler.password,
        use_tls=mqtt_handler.use_tls,
        keepalive=mqtt_handler.keepalive,
        default_qos=mqtt_handler.default_qos
    )
    
    # Test connection
    result = mqtt_handler.test_connection(
        host=test_settings.host,
        port=test_settings.port,
        client_id=test_settings.client_id,
        username=test_settings.username,
        password=test_settings.password if test_settings.password != "********" else mqtt_handler.password,
        use_tls=test_settings.use_tls,
        keepalive=test_settings.keepalive
    )
    
    if result["success"]:
        logger.info(f"Admin {current_user.username} tested broker connection: success")
        return ConnectionTestResult(
            success=True,
            message="Successfully connected to MQTT broker",
            details=result.get("details")
        )
    else:
        logger.warning(f"Admin {current_user.username} tested broker connection: failed")
        return ConnectionTestResult(
            success=False,
            message=f"Failed to connect to MQTT broker: {result.get('error')}",
            details=result.get("details")
        )


@router.post("/broker/reconnect", status_code=status.HTTP_202_ACCEPTED)
async def reconnect_broker(
    background_tasks: BackgroundTasks,
    current_user: User = Depends(get_admin_user),
    _: None = Depends(rate_limiter)
):
    """Reconnect to MQTT broker with current settings (admin only)"""
    if not mqtt_handler:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="MQTT handler not initialized"
        )
    
    # Reconnect in background
    logger.info(f"Admin {current_user.username} requested MQTT broker reconnection")
    background_tasks.add_task(mqtt_handler.reconnect)
    
    return {"message": "Reconnection initiated"} 