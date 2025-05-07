"""
MQTT subscription router for mesh broker
"""
from fastapi import APIRouter, HTTPException, Request, Depends
from pydantic import BaseModel
from typing import Optional, List
import logging
from ..mqtt.mqtt_handler import get_mqtt_handler
from ..auth.rate_limiter import get_limiter
from ..models.responses import StatusResponse, DataResponse

# Configure logging
logger = logging.getLogger(__name__)

router = APIRouter(
    prefix="/mqtt",
    tags=["mqtt"],
    responses={404: {"description": "Not found"}}
)

limiter = get_limiter()

class TopicSubscription(BaseModel):
    topic: str
    qos: Optional[int] = None

@router.post("/subscribe", response_model=StatusResponse)
@limiter.limit("10/minute")
async def subscribe_topic(
    subscription: TopicSubscription, 
    request: Request,
    mqtt_handler = Depends(get_mqtt_handler)
):
    """Subscribe to an MQTT topic"""
    try:
        logger.info(f"Subscribing to topic {subscription.topic}")
        result = mqtt_handler.subscribe(subscription.topic, subscription.qos)
        
        # Handle different return types - could be bool or list based on MQTT client
        if result is None:
            error_msg = "Failed to subscribe: Not connected"
            logger.error(error_msg)
            raise HTTPException(status_code=500, detail=error_msg)
        elif isinstance(result, bool):
            if not result:
                error_msg = "Failed to subscribe to topic"
                logger.error(error_msg)
                raise HTTPException(status_code=500, detail=error_msg)
        elif isinstance(result, list) and (not result or result[0] != 0):
            error_msg = f"Failed to subscribe: {result[0] if result else 'Not connected'}"
            logger.error(error_msg)
            raise HTTPException(status_code=500, detail=error_msg)
        
        return StatusResponse(
            success=True, 
            message=f"Subscribed to {subscription.topic}"
        )
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error subscribing to topic: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/unsubscribe", response_model=StatusResponse)
@limiter.limit("10/minute")
async def unsubscribe_topic(
    subscription: TopicSubscription, 
    request: Request,
    mqtt_handler = Depends(get_mqtt_handler)
):
    """Unsubscribe from an MQTT topic"""
    try:
        logger.info(f"Unsubscribing from topic {subscription.topic}")
        result = mqtt_handler.unsubscribe(subscription.topic)
        
        # Handle different return types - could be bool or list based on MQTT client
        if result is None:
            error_msg = "Failed to unsubscribe: Not connected"
            logger.error(error_msg)
            raise HTTPException(status_code=500, detail=error_msg)
        elif isinstance(result, bool):
            if not result:
                error_msg = "Failed to unsubscribe from topic"
                logger.error(error_msg)
                raise HTTPException(status_code=500, detail=error_msg)
        elif isinstance(result, list) and (not result or result[0] != 0):
            error_msg = f"Failed to unsubscribe: {result[0] if result else 'Not connected'}"
            logger.error(error_msg)
            raise HTTPException(status_code=500, detail=error_msg)
        
        return StatusResponse(
            success=True, 
            message=f"Unsubscribed from {subscription.topic}"
        )
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error unsubscribing from topic: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/topics", response_model=DataResponse[List[str]])
@limiter.limit("30/minute")
async def get_topics(
    request: Request,
    mqtt_handler = Depends(get_mqtt_handler)
):
    """Get list of currently subscribed topics"""
    topics = mqtt_handler.get_subscribed_topics()
    logger.debug(f"Retrieved {len(topics)} subscribed topics")
    return DataResponse(data=topics)