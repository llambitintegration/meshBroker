"""
MQTT publishing router for mesh broker
"""
from fastapi import APIRouter, HTTPException, Request, Depends
from pydantic import BaseModel
from typing import Optional, Any
import logging
from ..mqtt.mqtt_handler import get_mqtt_handler
from ..auth.rate_limiter import get_limiter
from ..models.responses import StatusResponse

# Configure logging
logger = logging.getLogger(__name__)

router = APIRouter(
    prefix="/mqtt",
    tags=["mqtt"],
    responses={404: {"description": "Not found"}}
)

limiter = get_limiter()

class MQTTMessage(BaseModel):
    topic: str
    payload: str
    qos: Optional[int] = None
    retain: Optional[bool] = False

@router.post("/publish", response_model=StatusResponse)
@limiter.limit("20/minute")
async def publish_message(
    message: MQTTMessage, 
    request: Request,
    mqtt_handler = Depends(get_mqtt_handler)
):
    """Publish a message to an MQTT topic"""
    try:
        logger.info(f"Publishing message to topic {message.topic}")
        message_id = mqtt_handler.publish(
            message.topic,
            message.payload,
            qos=message.qos,
            retain=message.retain
        )
        if message_id is None:
            error_msg = "Failed to publish message"
            logger.error(error_msg)
            raise HTTPException(status_code=500, detail=error_msg)
        
        return StatusResponse(
            success=True, 
            message="Message published successfully", 
            data={"message_id": message_id}
        )
    except Exception as e:
        logger.error(f"Error publishing message: {e}")
        raise HTTPException(status_code=500, detail=str(e))