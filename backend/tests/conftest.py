import os
import sys
import pytest
import logging
from unittest.mock import Mock, patch, MagicMock

# Add the parent directory to the path so that imports work correctly
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))) 

@pytest.fixture
def mock_mqtt_client():
    with patch('paho.mqtt.client.Client') as mock_client:
        client_instance = Mock()
        mock_client.return_value = client_instance
        
        # Mock successful connection
        client_instance.connect.return_value = 0
        client_instance.loop_start.return_value = None
        
        # Configure publish return value with proper 'rc' attribute
        publish_result = Mock()
        publish_result.rc = 0  # MQTT_ERR_SUCCESS
        client_instance.publish.return_value = publish_result
        
        # Configure subscribe to return a proper tuple (result_code, message_id)
        client_instance.subscribe.return_value = (0, 1)  # (MQTT_ERR_SUCCESS, message_id)
        
        # Configure unsubscribe to return a proper tuple as well
        client_instance.unsubscribe.return_value = (0, 1)  # (MQTT_ERR_SUCCESS, message_id)
        
        yield client_instance

@pytest.fixture
def mock_logger():
    with patch('logging.getLogger') as mock_log:
        logger = Mock(spec=logging.Logger)
        mock_log.return_value = logger
        yield logger

@pytest.fixture
def mqtt_client_instance(mock_mqtt_client, mock_logger):
    from meshtastic_mqtt_cli import MQTTClient
    client = MQTTClient('localhost', 1883, mock_logger)
    yield client
    if client.connected:
        client.disconnect()

@pytest.fixture(autouse=True)
def mock_broker_monitor():
    """Mock the broker monitor to prevent real connection attempts during tests"""
    # Mock the BrokerHealthCheck class methods that try to connect to a real broker
    with patch("backend.monitoring.broker_monitor.BrokerHealthCheck._check_mqtt_connectivity", return_value=True):
        with patch("backend.monitoring.broker_monitor.BrokerHealthCheck._check_broker_connectivity", return_value=True):
            # Mock the actual implementation of MQTT handler methods that interact with real MQTT broker
            with patch("backend.mqtt_handler.MQTTHandler.connect") as mock_connect:
                with patch("backend.mqtt_handler.MQTTHandler.is_connected", return_value=True):
                    with patch("backend.mqtt_handler.mqtt.Client", autospec=True) as mock_mqtt_client:
                        # Set up mock client behavior
                        mock_instance = mock_mqtt_client.return_value
                        mock_instance.is_connected.return_value = True
                        
                        # Mock publish result
                        publish_result = MagicMock()
                        publish_result.rc = 0
                        publish_result.mid = 1234
                        mock_instance.publish.return_value = publish_result
                        
                        # Mock subscribe result (rc, mid)
                        mock_instance.subscribe.return_value = (0, 1)
                        
                        # Mock functions don't need to do anything
                        mock_connect.return_value = None
                        yield 