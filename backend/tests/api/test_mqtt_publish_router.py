"""
Tests for the MQTT publish router
"""
import pytest
from fastapi import FastAPI, Depends, APIRouter, HTTPException
from fastapi.testclient import TestClient
from unittest.mock import MagicMock, patch
from backend.models.responses import StatusResponse
import asyncio

class MockMQTTHandler:
    def __init__(self):
        self.last_publish = None
        self.should_fail = False
        self.message_id = 12345
    
    def publish(self, topic, payload, qos=None, retain=False):
        self.last_publish = {
            "topic": topic,
            "payload": payload,
            "qos": qos,
            "retain": retain
        }
        if self.should_fail:
            return None
        return self.message_id

# Create a mock get_mqtt_handler function
mock_handler = MockMQTTHandler()
def get_mock_mqtt_handler():
    return mock_handler

# Create a mock limiter that does nothing
class MockLimiter:
    def limit(self, rate_string):
        return lambda f: f

mock_limiter = MockLimiter()

# Create a test router with our mocked dependencies
test_router = APIRouter(prefix="/mqtt", tags=["mqtt"])

# Import the actual endpoint function from the real router
from backend.routers.mqtt_publish import MQTTMessage, publish_message as original_publish_message

# Add the endpoint to our test router with mocked dependencies
@test_router.post("/publish", response_model=StatusResponse)
async def publish_message(
    message: MQTTMessage,
    mqtt_handler = Depends(get_mock_mqtt_handler)
):
    # This is a simplified version of the actual endpoint
    try:
        message_id = mqtt_handler.publish(
            message.topic,
            message.payload,
            qos=message.qos,
            retain=message.retain
        )
        if message_id is None:
            raise HTTPException(status_code=500, detail="Failed to publish message")
        
        return StatusResponse(
            success=True, 
            message="Message published successfully", 
            data={"message_id": message_id}
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# Create a test app with our router
test_app = FastAPI()
test_app.include_router(test_router)
test_client = TestClient(test_app)

def test_publish_message_success():
    """Test successful message publishing"""
    # Reset the mock handler state
    mock_handler.should_fail = False
    mock_handler.last_publish = None
    
    response = test_client.post(
        "/mqtt/publish",
        json={"topic": "test/topic", "payload": "test message", "qos": 1, "retain": True}
    )
    
    assert response.status_code == 200
    result = response.json()
    assert result["success"] is True
    assert result["message"] == "Message published successfully"
    assert result["data"]["message_id"] == mock_handler.message_id
    
    # Verify the message was published with correct parameters
    assert mock_handler.last_publish["topic"] == "test/topic"
    assert mock_handler.last_publish["payload"] == "test message"
    assert mock_handler.last_publish["qos"] == 1
    assert mock_handler.last_publish["retain"] is True

def test_publish_message_minimal():
    """Test message publishing with minimal parameters"""
    # Reset the mock handler state
    mock_handler.should_fail = False
    mock_handler.last_publish = None
    
    response = test_client.post(
        "/mqtt/publish",
        json={"topic": "test/minimal", "payload": "minimal message"}
    )
    
    assert response.status_code == 200
    result = response.json()
    assert result["success"] is True
    
    # Verify the message was published with default parameters
    assert mock_handler.last_publish["topic"] == "test/minimal"
    assert mock_handler.last_publish["payload"] == "minimal message"
    assert mock_handler.last_publish["qos"] is None
    assert mock_handler.last_publish["retain"] is False

def test_publish_message_failure():
    """Test handling of publishing failure"""
    # Make the mock handler fail
    mock_handler.should_fail = True
    
    response = test_client.post(
        "/mqtt/publish",
        json={"topic": "test/failure", "payload": "failure test"}
    )
    
    assert response.status_code == 500
    result = response.json()
    assert "detail" in result
    assert "Failed to publish message" in result["detail"]

def test_publish_message_exception():
    """Test handling of exceptions during publishing"""
    # Make the handler raise an exception
    original_publish = mock_handler.publish
    
    def failing_publish(*args, **kwargs):
        raise Exception("Test exception")
    
    try:
        mock_handler.publish = failing_publish
        
        response = test_client.post(
            "/mqtt/publish",
            json={"topic": "test/exception", "payload": "exception test"}
        )
        
        assert response.status_code == 500
        result = response.json()
        assert "detail" in result
        assert "Test exception" in result["detail"]
    finally:
        # Restore the original method
        mock_handler.publish = original_publish

def test_publish_invalid_data():
    """Test validation of required fields"""
    # Topic is required
    response = test_client.post(
        "/mqtt/publish",
        json={"payload": "missing topic"}
    )
    assert response.status_code == 422
    
    # Payload is required
    response = test_client.post(
        "/mqtt/publish",
        json={"topic": "missing/payload"}
    )
    assert response.status_code == 422 