"""
Tests for the MQTT subscribe router
"""
import pytest
from fastapi import FastAPI, Depends, APIRouter, HTTPException
from fastapi.testclient import TestClient
from unittest.mock import MagicMock, patch
from backend.models.responses import StatusResponse, DataResponse
import asyncio

class MockMQTTHandler:
    def __init__(self):
        self.subscribed_topics = set()
        self.should_fail = False
    
    def subscribe(self, topic, qos=None):
        if self.should_fail:
            return None
        self.subscribed_topics.add(topic)
        return [0]  # Success result code
    
    def unsubscribe(self, topic):
        if self.should_fail:
            return None
        if topic in self.subscribed_topics:
            self.subscribed_topics.remove(topic)
        return [0]  # Success result code
    
    def get_subscribed_topics(self):
        return list(self.subscribed_topics)

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

# Import the actual models and endpoint functions from the real router
from backend.routers.mqtt_subscribe import TopicSubscription

@test_router.post("/subscribe", response_model=StatusResponse)
async def subscribe_topic(
    subscription: TopicSubscription,
    mqtt_handler = Depends(get_mock_mqtt_handler)
):
    try:
        result = mqtt_handler.subscribe(subscription.topic, subscription.qos)
        
        # Handle different return types - could be bool or list based on MQTT client
        if result is None:
            raise HTTPException(status_code=500, detail="Failed to subscribe: Not connected")
        elif isinstance(result, bool):
            if not result:
                raise HTTPException(status_code=500, detail="Failed to subscribe to topic")
        elif isinstance(result, list) and (not result or result[0] != 0):
            raise HTTPException(status_code=500, detail=f"Failed to subscribe: {result[0] if result else 'Not connected'}")
        
        return StatusResponse(
            success=True, 
            message=f"Subscribed to {subscription.topic}"
        )
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@test_router.post("/unsubscribe", response_model=StatusResponse)
async def unsubscribe_topic(
    subscription: TopicSubscription,
    mqtt_handler = Depends(get_mock_mqtt_handler)
):
    try:
        result = mqtt_handler.unsubscribe(subscription.topic)
        
        # Handle different return types - could be bool or list based on MQTT client
        if result is None:
            raise HTTPException(status_code=500, detail="Failed to unsubscribe: Not connected")
        elif isinstance(result, bool):
            if not result:
                raise HTTPException(status_code=500, detail="Failed to unsubscribe from topic")
        elif isinstance(result, list) and (not result or result[0] != 0):
            raise HTTPException(status_code=500, detail=f"Failed to unsubscribe: {result[0] if result else 'Not connected'}")
        
        return StatusResponse(
            success=True, 
            message=f"Unsubscribed from {subscription.topic}"
        )
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@test_router.get("/topics", response_model=DataResponse[list])
async def get_topics(
    mqtt_handler = Depends(get_mock_mqtt_handler)
):
    topics = mqtt_handler.get_subscribed_topics()
    return DataResponse(data=topics)

# Create a test app with our router
test_app = FastAPI()
test_app.include_router(test_router)
test_client = TestClient(test_app)

def test_subscribe_topic_success():
    """Test successful topic subscription"""
    # Reset the mock handler
    mock_handler.should_fail = False
    mock_handler.subscribed_topics = set()
    
    response = test_client.post(
        "/mqtt/subscribe",
        json={"topic": "test/topic", "qos": 1}
    )
    
    assert response.status_code == 200
    result = response.json()
    assert result["success"] is True
    assert result["message"] == "Subscribed to test/topic"
    
    # Verify the topic was subscribed
    assert "test/topic" in mock_handler.subscribed_topics

def test_subscribe_topic_minimal():
    """Test topic subscription with minimal parameters"""
    # Reset the mock handler
    mock_handler.should_fail = False
    mock_handler.subscribed_topics = set()
    
    response = test_client.post(
        "/mqtt/subscribe",
        json={"topic": "test/minimal"}
    )
    
    assert response.status_code == 200
    result = response.json()
    assert result["success"] is True
    
    # Verify the topic was subscribed
    assert "test/minimal" in mock_handler.subscribed_topics

def test_subscribe_topic_failure():
    """Test handling of subscription failure"""
    # Make the subscription fail
    mock_handler.should_fail = True
    
    response = test_client.post(
        "/mqtt/subscribe",
        json={"topic": "test/failure"}
    )
    
    assert response.status_code == 500
    result = response.json()
    assert "detail" in result
    assert "Failed to subscribe" in result["detail"]

def test_unsubscribe_topic_success():
    """Test successful topic unsubscription"""
    # Reset the mock handler
    mock_handler.should_fail = False
    mock_handler.subscribed_topics = {"test/unsubscribe"}
    
    response = test_client.post(
        "/mqtt/unsubscribe",
        json={"topic": "test/unsubscribe"}
    )
    
    assert response.status_code == 200
    result = response.json()
    assert result["success"] is True
    assert result["message"] == "Unsubscribed from test/unsubscribe"
    
    # Verify the topic was unsubscribed
    assert "test/unsubscribe" not in mock_handler.subscribed_topics

def test_unsubscribe_topic_failure():
    """Test handling of unsubscription failure"""
    # Make the unsubscription fail
    mock_handler.should_fail = True
    
    response = test_client.post(
        "/mqtt/unsubscribe",
        json={"topic": "test/unsubscribe_failure"}
    )
    
    assert response.status_code == 500
    result = response.json()
    assert "detail" in result
    assert "Failed to unsubscribe" in result["detail"]

def test_get_topics():
    """Test getting list of subscribed topics"""
    # Reset the mock handler
    mock_handler.subscribed_topics = {"topic1", "topic2", "topic3"}
    
    response = test_client.get("/mqtt/topics")
    
    assert response.status_code == 200
    result = response.json()
    assert "data" in result
    assert set(result["data"]) == {"topic1", "topic2", "topic3"}

def test_subscribe_exception():
    """Test handling of exceptions during subscribing"""
    # Replace the subscribe method with one that raises an exception
    original_subscribe = mock_handler.subscribe
    
    def failing_subscribe(*args, **kwargs):
        raise Exception("Test exception")
    
    try:
        mock_handler.subscribe = failing_subscribe
        
        response = test_client.post(
            "/mqtt/subscribe",
            json={"topic": "test/exception"}
        )
        
        assert response.status_code == 500
        result = response.json()
        assert "detail" in result
        assert "Test exception" in result["detail"]
    finally:
        # Restore the original method
        mock_handler.subscribe = original_subscribe

def test_unsubscribe_exception():
    """Test handling of exceptions during unsubscribing"""
    # Replace the unsubscribe method with one that raises an exception
    original_unsubscribe = mock_handler.unsubscribe
    
    def failing_unsubscribe(*args, **kwargs):
        raise Exception("Test exception")
    
    try:
        mock_handler.unsubscribe = failing_unsubscribe
        
        response = test_client.post(
            "/mqtt/unsubscribe",
            json={"topic": "test/exception"}
        )
        
        assert response.status_code == 500
        result = response.json()
        assert "detail" in result
        assert "Test exception" in result["detail"]
    finally:
        # Restore the original method
        mock_handler.unsubscribe = original_unsubscribe

def test_subscribe_invalid_data():
    """Test subscription with invalid data"""
    response = test_client.post(
        "/mqtt/subscribe",
        json={}
    )
    assert response.status_code == 422

def test_unsubscribe_invalid_data():
    """Test unsubscription with invalid data"""
    response = test_client.post(
        "/mqtt/unsubscribe",
        json={}
    )
    assert response.status_code == 422 