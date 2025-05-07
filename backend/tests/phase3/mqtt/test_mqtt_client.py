import unittest
import sys
import os
import logging
from unittest.mock import MagicMock, patch, Mock
import json
import socket

# Add parent directory to path to import modules
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from meshtastic_mqtt_cli import MQTTClient

class TestMQTTClient(unittest.TestCase):
    def setUp(self):
        # Set up logger with a custom handler to capture logs
        self.log_capture = []
        
        class TestLogHandler(logging.Handler):
            def __init__(self, log_list):
                super().__init__()
                self.log_list = log_list
                
            def emit(self, record):
                self.log_list.append(self.format(record))
        
        self.logger = logging.getLogger("test_mqtt_client")
        self.logger.setLevel(logging.DEBUG)
        self.handler = TestLogHandler(self.log_capture)
        self.handler.setFormatter(logging.Formatter('%(levelname)s:%(message)s'))
        self.logger.addHandler(self.handler)
        
        # Create client with mocked methods
        self.client = MQTTClient("localhost", 1883, self.logger)
        self.client._client = MagicMock()
        self.client.connected = True
        
        # Mock the _write_output method to capture output directly rather than printing
        self.client._write_output = Mock(side_effect=self._mock_write_output)
        
    def _mock_write_output(self, content):
        # Add the content to our log capture without printing
        self.log_capture.append(f"OUTPUT:{content}")
    
    def test_init(self):
        """Test client initialization"""
        # Test with valid parameters
        client = MQTTClient("test.mosquitto.org", 1883)
        self.assertEqual(client.broker, "test.mosquitto.org")
        self.assertEqual(client.port, 1883)
        self.assertFalse(client.connected)
        
        # Test with invalid parameters
        with self.assertRaises(ValueError):
            MQTTClient("", 1883)
        
        with self.assertRaises(ValueError):
            MQTTClient("localhost", 0)
        
        with self.assertRaises(ValueError):
            MQTTClient("localhost", 65536)
    
    @patch('socket.gethostbyname')
    def test_connect(self, mock_gethostbyname):
        """Test connection to broker"""
        client = MQTTClient("test.mosquitto.org", 1883, self.logger)
        
        # Mock client connect to return success
        client._client.connect = MagicMock(return_value=0)
        client._client.loop_start = MagicMock()
        
        # Test successful connection
        client.connect()
        self.assertTrue(client.connected)
        client._client.connect.assert_called_once_with("test.mosquitto.org", 1883)
        client._client.loop_start.assert_called_once()
        
        # Reset mocks
        client._client.connect.reset_mock()
        client._client.loop_start.reset_mock()
        
        # Test connection failure
        client._client.connect = MagicMock(return_value=1)
        with self.assertRaises(ConnectionError):
            client.connect()
        
        # Test invalid host
        mock_gethostbyname.side_effect = socket.gaierror()
        with self.assertRaises(ValueError):
            client.connect()
    
    def test_on_message_text(self):
        """Test handling of text messages"""
        # Create a mock message with text payload
        mock_message = MagicMock()
        mock_message.topic = "test/topic"
        mock_message.payload = "Hello, world!".encode('utf-8')
        
        # Call the message handler
        self.client._on_message(None, None, mock_message)
        
        # Check that the message was output (now using our mocked _write_output)
        found = False
        for log in self.log_capture:
            if "OUTPUT:" in log and "Hello, world!" in log:
                found = True
                break
        self.assertTrue(found, "Text message output not found in logs")
    
    def test_on_message_json(self):
        """Test handling of JSON messages"""
        # Create a mock message with JSON payload
        mock_message = MagicMock()
        mock_message.topic = "test/topic"
        mock_message.payload = json.dumps({"key": "value"}).encode('utf-8')
        
        # Call the message handler
        self.client._on_message(None, None, mock_message)
        
        # Check that the message was output
        found = False
        for log in self.log_capture:
            if "OUTPUT:" in log and '"key": "value"' in log:
                found = True
                break
        self.assertTrue(found, "JSON message output not found in logs")
    
    def test_on_message_binary(self):
        """Test handling of binary messages"""
        # Create a mock message with binary payload
        mock_message = MagicMock()
        mock_message.topic = "msh/test/binary"
        mock_message.payload = bytes([0x01, 0x02, 0xE0, 0xA5, 0xFF])
        
        # Call the message handler
        self.client._on_message(None, None, mock_message)
        
        # Check that the message was logged correctly
        found = False
        for log in self.log_capture:
            if "OUTPUT:" in log and "binary message" in log and "0102e0a5ff" in log:
                found = True
                break
        self.assertTrue(found, "Binary message output not found in logs")
    
    def test_on_message_exception_handling(self):
        """Test exception handling in message processing"""
        # Create a mock message that will cause an exception
        mock_message = MagicMock()
        mock_message.topic = "test/topic"
        
        # Mock the payload property to raise an exception when accessed
        class ExceptionRaisingProperty:
            def __get__(self, obj, objtype=None):
                raise Exception("Test exception")
                
        type(mock_message).payload = ExceptionRaisingProperty()
        
        # Call the message handler
        self.client._on_message(None, None, mock_message)
        
        # Check that the error was logged but didn't crash the handler
        self.assertIn("ERROR:Error processing message: Test exception", self.log_capture)
    
    def test_send_message(self):
        """Test sending messages"""
        # Mock successful publish
        mock_result = MagicMock()
        mock_result.rc = 0  # MQTT_ERR_SUCCESS
        self.client._client.publish = MagicMock(return_value=mock_result)
        
        # Test sending a message
        self.client.send_message("test/topic", "Hello", 1, False)
        self.client._client.publish.assert_called_once_with("test/topic", "Hello", 1, False)
        
        # Test with invalid QoS
        with self.assertRaises(ValueError):
            self.client.send_message("test/topic", "Hello", 3, False)
        
        # Test with empty topic
        with self.assertRaises(ValueError):
            self.client.send_message("", "Hello", 0, False)
        
        # Test when not connected
        self.client.connected = False
        with self.assertRaises(RuntimeError):
            self.client.send_message("test/topic", "Hello", 0, False)
    
    def test_subscribe(self):
        """Test subscribing to topics"""
        # Mock successful subscribe
        self.client._client.subscribe = MagicMock(return_value=(0, 1))  # Success, QoS 1
        
        # Test subscribing to a topic
        self.client.subscribe("test/topic", 1)
        self.client._client.subscribe.assert_called_once_with("test/topic", 1)
        
        # Test with invalid QoS
        with self.assertRaises(ValueError):
            self.client.subscribe("test/topic", 3)
        
        # Test with empty topic
        with self.assertRaises(ValueError):
            self.client.subscribe("", 0)
        
        # Test when not connected
        self.client.connected = False
        with self.assertRaises(RuntimeError):
            self.client.subscribe("test/topic", 0)
    
    def test_disconnect(self):
        """Test disconnection from broker"""
        # Set up mocks
        self.client._client.disconnect = MagicMock()
        self.client._client.loop_stop = MagicMock()
        
        # Make sure output_stream is None to avoid file close issues
        self.client.output_stream = None
        
        # Test disconnection
        self.client.disconnect()
        self.client._client.disconnect.assert_called_once()
        self.client._client.loop_stop.assert_called_once()
        self.assertFalse(self.client.connected)
        
        # Test disconnection when already disconnected
        self.client._client.disconnect.reset_mock()
        self.client._client.loop_stop.reset_mock()
        self.client.disconnect()
        self.client._client.disconnect.assert_not_called()
        self.client._client.loop_stop.assert_not_called()
    
    def test_on_connect(self):
        """Test connection callback"""
        # Test successful connection
        self.client._on_connect(None, None, None, 0)
        self.assertTrue(self.client.connected)
        
        # Test connection failure
        self.client._on_connect(None, None, None, 1)
        self.assertFalse(self.client.connected)
        self.assertIn("ERROR:Connection refused - incorrect protocol version", self.log_capture)
        
        # Test unknown failure code
        self.client._on_connect(None, None, None, 99)
        self.assertFalse(self.client.connected)
        self.assertIn("ERROR:Connection failed with code 99", self.log_capture)
    
    def test_on_disconnect(self):
        """Test disconnection callback"""
        # Test normal disconnection
        self.client._on_disconnect(None, None, 0)
        self.assertFalse(self.client.connected)
        self.assertIn("INFO:Disconnected from broker", self.log_capture)
        
        # Test unexpected disconnection
        self.client._on_disconnect(None, None, 1)
        self.assertFalse(self.client.connected)
        self.assertIn("WARNING:Unexpected disconnection from broker: 1", self.log_capture)

    def test_meshtastic_topic_detection(self):
        """Test Meshtastic topic pattern detection"""
        # Test Meshtastic topic detection
        self.assertTrue(self.client._is_meshtastic_topic("msh/somenode/data"))
        self.assertTrue(self.client._is_meshtastic_topic("msh/us/tx/llam/bit/2/e/Public/mqttStudio"))
        self.assertFalse(self.client._is_meshtastic_topic("test/topic"))
        
        # Test Meshtastic binary topic detection
        self.assertTrue(self.client._is_meshtastic_binary_topic("msh/somenode/binary"))
        self.assertFalse(self.client._is_meshtastic_binary_topic("msh/somenode/data"))
        self.assertFalse(self.client._is_meshtastic_binary_topic("test/topic"))

if __name__ == "__main__":
    unittest.main()