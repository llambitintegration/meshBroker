"""
Direct Meshtastic device communication using the Python library
"""
import logging
import time
import asyncio
from typing import Dict, List, Optional, Any, Callable, Union
import threading
import json

import meshtastic
import meshtastic.serial_interface
import meshtastic.tcp_interface
import meshtastic.ble_interface
from meshtastic.node import Node
from meshtastic.__init__ import LOCAL_ADDR

from backend.models.meshtastic_node import MeshtasticNode, Position, Message

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


class DeviceInterface:
    """Interface for communicating with a specific Meshtastic device"""
    
    def __init__(self, connection_type: str, connection_params: Dict[str, Any]):
        """
        Initialize a device interface
        
        Args:
            connection_type: Type of connection ('serial', 'tcp', 'ble')
            connection_params: Parameters for the connection
        """
        self.connection_type = connection_type
        self.connection_params = connection_params
        self.interface = None
        self.connected = False
        self.node_id = None
        self.event_handlers = {}
        self._lock = threading.RLock()
        
    def connect(self) -> bool:
        """Connect to the device"""
        try:
            with self._lock:
                if self.connected:
                    logger.warning("Already connected to a device")
                    return True
                
                logger.info(f"Connecting to Meshtastic device via {self.connection_type}")
                
                if self.connection_type == "serial":
                    port = self.connection_params.get("port")
                    if port:
                        self.interface = meshtastic.serial_interface.SerialInterface(port)
                    else:
                        # Auto-discover
                        self.interface = meshtastic.serial_interface.SerialInterface()
                
                elif self.connection_type == "tcp":
                    host = self.connection_params.get("host", "localhost")
                    self.interface = meshtastic.tcp_interface.TCPInterface(host)
                
                elif self.connection_type == "ble":
                    address = self.connection_params.get("address")
                    name = self.connection_params.get("name")
                    if address:
                        self.interface = meshtastic.ble_interface.BLEInterface(address)
                    elif name:
                        self.interface = meshtastic.ble_interface.BLEInterface(name)
                    else:
                        logger.error("BLE connection requires address or name")
                        return False
                
                else:
                    logger.error(f"Unsupported connection type: {self.connection_type}")
                    return False
                
                # Set up event handlers
                self._setup_event_handlers()
                
                # Get our node ID
                node_info = self.interface.getMyNodeInfo()
                if node_info:
                    self.node_id = node_info.get("num", None)
                    logger.info(f"Connected to node with ID: {self.node_id}")
                
                self.connected = True
                return True
                
        except Exception as e:
            logger.error(f"Error connecting to device: {e}")
            self.interface = None
            return False

    def disconnect(self):
        """Disconnect from the device"""
        with self._lock:
            if not self.connected:
                return
            
            try:
                if self.interface:
                    self.interface.close()
                    self.interface = None
                
                self.connected = False
                logger.info("Disconnected from Meshtastic device")
                
            except Exception as e:
                logger.error(f"Error disconnecting from device: {e}")

    def _setup_event_handlers(self):
        """Set up event handlers for the interface"""
        if not self.interface:
            return
        
        def on_receive(packet, interface):
            if "onReceive" in self.event_handlers:
                for handler in self.event_handlers["onReceive"]:
                    try:
                        handler(packet)
                    except Exception as e:
                        logger.error(f"Error in onReceive handler: {e}")
        
        def on_node_updated(node, interface):
            if "onNodeUpdated" in self.event_handlers:
                for handler in self.event_handlers["onNodeUpdated"]:
                    try:
                        handler(node)
                    except Exception as e:
                        logger.error(f"Error in onNodeUpdated handler: {e}")
        
        def on_connection_established(interface):
            if "onConnectionEstablished" in self.event_handlers:
                for handler in self.event_handlers["onConnectionEstablished"]:
                    try:
                        handler()
                    except Exception as e:
                        logger.error(f"Error in onConnectionEstablished handler: {e}")
        
        self.interface.onReceive = on_receive
        self.interface.onNodeUpdated = on_node_updated
        self.interface.onConnectionEstablished = on_connection_established

    def add_event_handler(self, event_type: str, handler: Callable):
        """
        Add an event handler
        
        Args:
            event_type: Type of event ('onReceive', 'onNodeUpdated', etc.)
            handler: Handler function
        """
        if event_type not in self.event_handlers:
            self.event_handlers[event_type] = []
        
        self.event_handlers[event_type].append(handler)

    def get_nodes(self) -> Dict[str, Dict[str, Any]]:
        """Get all nodes from the device"""
        if not self.connected or not self.interface:
            logger.warning("Not connected to a device")
            return {}
        
        return self.interface.nodes

    def get_config(self) -> Dict[str, Any]:
        """Get device configuration"""
        if not self.connected or not self.interface:
            logger.warning("Not connected to a device")
            return {}
        
        return self.interface.getConfig()

    def set_config(self, key: str, value: Any) -> bool:
        """
        Set a configuration value
        
        Args:
            key: Configuration key (e.g., 'device.role')
            value: Value to set
        """
        if not self.connected or not self.interface:
            logger.warning("Not connected to a device")
            return False
        
        try:
            self.interface.setConfig(key, value)
            return True
        except Exception as e:
            logger.error(f"Error setting config {key}={value}: {e}")
            return False

    def get_channels(self) -> List[Dict[str, Any]]:
        """Get channel settings"""
        if not self.connected or not self.interface:
            logger.warning("Not connected to a device")
            return []
        
        try:
            return self.interface.getChannels()
        except Exception as e:
            logger.error(f"Error getting channels: {e}")
            return []

    def set_channel_settings(self, settings: Dict[str, Any], channel_index: int = 0) -> bool:
        """
        Set channel settings
        
        Args:
            settings: Channel settings to update
            channel_index: Channel index (default: 0 for primary channel)
        """
        if not self.connected or not self.interface:
            logger.warning("Not connected to a device")
            return False
        
        try:
            self.interface.setChannelSettings(settings, channelIndex=channel_index)
            return True
        except Exception as e:
            logger.error(f"Error setting channel settings: {e}")
            return False

    def send_text(self, text: str, destination_id: Optional[str] = None) -> bool:
        """
        Send a text message
        
        Args:
            text: Message text
            destination_id: Destination node ID (None for broadcast)
        """
        if not self.connected or not self.interface:
            logger.warning("Not connected to a device")
            return False
        
        try:
            self.interface.sendText(text, destinationId=destination_id)
            return True
        except Exception as e:
            logger.error(f"Error sending text message: {e}")
            return False


class DeviceManager:
    """Manages connections to Meshtastic devices"""
    
    def __init__(self):
        """Initialize the device manager"""
        self.devices: Dict[str, DeviceInterface] = {}
        self.default_device = None
        self._lock = threading.RLock()
    
    async def discover_serial_devices(self) -> List[str]:
        """Discover available serial devices"""
        try:
            # Define the timeout for port discovery (in seconds)
            timeout_seconds = 15
            
            # Log that we're starting discovery
            logger.info("Starting serial port discovery with timeout of %s seconds", timeout_seconds)
            
            # Use asyncio.wait_for to add a timeout
            try:
                # This can be blocking, so run in a thread with timeout
                ports = await asyncio.wait_for(
                    asyncio.to_thread(meshtastic.util.findPorts),
                    timeout=timeout_seconds
                )
                
                if not ports:
                    logger.info("No serial ports found that match Meshtastic devices")
                    return []
                    
                logger.info(f"Discovered serial ports: {ports}")
                return ports
                
            except asyncio.TimeoutError:
                logger.error(f"Serial port discovery timed out after {timeout_seconds} seconds")
                # Re-raise with more helpful message
                raise RuntimeError(f"Serial port discovery timed out after {timeout_seconds} seconds. "
                                   "Check if there are many USB devices connected or system is busy.")
                                   
        except PermissionError as e:
            # Common on Linux and macOS when running without proper permissions
            error_msg = str(e)
            logger.error(f"Permission error during serial port discovery: {error_msg}")
            
            # Provide platform-specific advice
            import platform
            system = platform.system().lower()
            
            if system == 'linux':
                advice = ("You may need to add your user to the 'dialout' group with: "
                         "sudo usermod -a -G dialout $USER\n"
                         "Then log out and back in for changes to take effect.")
            elif system == 'darwin':  # macOS
                advice = "You may need to grant permission to access USB devices in System Preferences."
            elif system == 'windows':
                advice = "Try running the application as Administrator."
            else:
                advice = "Check your user permissions for accessing serial ports."
                
            raise PermissionError(f"Permission denied accessing serial ports. {advice}")
            
        except (ImportError, ModuleNotFoundError) as e:
            # Missing dependencies
            logger.error(f"Missing dependencies for serial port discovery: {e}")
            raise RuntimeError(f"Missing required dependencies for serial port access: {e}. "
                              "Please ensure pyserial is installed.")
                              
        except FileNotFoundError as e:
            # Serial port was specified but doesn't exist
            logger.error(f"Serial port not found: {e}")
            raise FileNotFoundError(f"Specified serial port not found: {e}. "
                                   "Please verify the device is connected.")
                                   
        except Exception as e:
            # Handle general exceptions with helpful information
            error_msg = str(e)
            logger.error(f"Error discovering serial devices: {error_msg}")
            
            if "could not open port" in error_msg.lower():
                # Port might be in use by another application
                raise RuntimeError(f"Could not open serial port: {error_msg}. "
                                  "The port may be in use by another application.")
            elif "no such file or directory" in error_msg.lower():
                # Device was likely disconnected
                raise FileNotFoundError("No serial devices found. "
                                      "Please verify the device is connected and powered on.")
            elif "access denied" in error_msg.lower():
                # Permission issue
                raise PermissionError(f"Access denied to serial port: {error_msg}. "
                                     "You may not have sufficient permissions.")
            else:
                # General error case
                raise RuntimeError(f"Error discovering serial devices: {error_msg}")
    
    async def discover_ble_devices(self) -> List[Dict[str, str]]:
        """Discover available BLE devices"""
        try:
            # This can be blocking, so run in a thread
            devices = await asyncio.to_thread(meshtastic.ble_interface.scanForDevices)
            
            result = []
            for device in devices:
                result.append({
                    "name": device.name,
                    "address": device.addr
                })
            
            logger.info(f"Discovered BLE devices: {result}")
            return result
        except Exception as e:
            logger.error(f"Error discovering BLE devices: {e}")
            return []
    
    async def connect_device(self, connection_type: str, connection_params: Dict[str, Any], 
                            device_id: Optional[str] = None) -> Optional[str]:
        """
        Connect to a device
        
        Args:
            connection_type: Type of connection ('serial', 'tcp', 'ble')
            connection_params: Parameters for the connection
            device_id: Optional ID for the device (auto-generated if not provided)
            
        Returns:
            Device ID if successful, None otherwise
        """
        # Generate a device ID if not provided
        if not device_id:
            device_id = f"{connection_type}_{int(time.time())}"
        
        with self._lock:
            # Check if device already exists
            if device_id in self.devices:
                logger.warning(f"Device {device_id} already exists")
                return device_id
            
            # Create and connect the device
            device = DeviceInterface(connection_type, connection_params)
            
            # This can be blocking, so run in a thread
            connected = await asyncio.to_thread(device.connect)
            
            if connected:
                self.devices[device_id] = device
                
                # Set up event handlers
                setup_event_handlers(device)
                
                # Set as default if it's the first device
                if not self.default_device:
                    self.default_device = device_id
                
                return device_id
            else:
                return None
    
    async def disconnect_device(self, device_id: str) -> bool:
        """
        Disconnect from a device
        
        Args:
            device_id: Device ID to disconnect
            
        Returns:
            True if successful, False otherwise
        """
        with self._lock:
            if device_id not in self.devices:
                logger.warning(f"Device {device_id} not found")
                return False
            
            device = self.devices[device_id]
            
            # This can be blocking, so run in a thread
            await asyncio.to_thread(device.disconnect)
            
            # Remove from devices
            del self.devices[device_id]
            
            # Update default device if needed
            if self.default_device == device_id:
                self.default_device = next(iter(self.devices)) if self.devices else None
            
            return True
    
    def get_device(self, device_id: Optional[str] = None) -> Optional[DeviceInterface]:
        """
        Get a device interface
        
        Args:
            device_id: Device ID (uses default if None)
            
        Returns:
            DeviceInterface if found, None otherwise
        """
        with self._lock:
            # Use default device if not specified
            if device_id is None:
                device_id = self.default_device
            
            if not device_id or device_id not in self.devices:
                return None
            
            return self.devices[device_id]
    
    def get_device_info(self, device_id: Optional[str] = None) -> Dict[str, Any]:
        """
        Get information about a device
        
        Args:
            device_id: Device ID (uses default if None)
            
        Returns:
            Dict with device information
        """
        device = self.get_device(device_id)
        if not device:
            return {}
        
        return {
            "id": device_id or self.default_device,
            "connection_type": device.connection_type,
            "connection_params": device.connection_params,
            "connected": device.connected,
            "node_id": device.node_id
        }
    
    def get_all_devices(self) -> List[Dict[str, Any]]:
        """Get information about all connected devices"""
        with self._lock:
            result = []
            for device_id, device in self.devices.items():
                result.append({
                    "id": device_id,
                    "connection_type": device.connection_type,
                    "connection_params": device.connection_params,
                    "connected": device.connected,
                    "node_id": device.node_id,
                    "is_default": device_id == self.default_device
                })
            return result
    
    def set_default_device(self, device_id: str) -> bool:
        """
        Set the default device
        
        Args:
            device_id: Device ID to set as default
            
        Returns:
            True if successful, False otherwise
        """
        with self._lock:
            if device_id not in self.devices:
                logger.warning(f"Device {device_id} not found")
                return False
            
            self.default_device = device_id
            return True


# Singleton instance of DeviceManager
device_manager = DeviceManager()


def convert_node_to_model(node: Node, source: str = "direct") -> MeshtasticNode:
    """
    Convert a Meshtastic Node to our MeshtasticNode model
    
    Args:
        node: Meshtastic Node object
        source: Source of the node data ('direct', 'mqtt', etc.)
        
    Returns:
        MeshtasticNode instance
    """
    try:
        node_id = str(node.id)
        user = node.user
        position_data = node.position
        
        # Create position object if available
        position = None
        if position_data and 'latitude' in position_data and 'longitude' in position_data:
            position = Position(
                latitude=position_data.get('latitude', 0.0),
                longitude=position_data.get('longitude', 0.0),
                altitude=position_data.get('altitude', 0.0),
                time=position_data.get('time', int(time.time()))
            )
        
        # Create MeshtasticNode instance
        meshtastic_node = MeshtasticNode(
            node_id=node_id,
            name=user.get('longName', '') if user else '',
            user_short_name=user.get('shortName', '') if user else '',
            position=position,
            last_seen=int(time.time()),
            battery_level=node.device_metrics.get('batteryLevel', 0) if hasattr(node, 'device_metrics') else 0,
            voltage=node.device_metrics.get('voltage', 0.0) if hasattr(node, 'device_metrics') else 0.0,
            snr=node.snr if hasattr(node, 'snr') else 0.0,
            rssi=node.rssi if hasattr(node, 'rssi') else 0,
            group="direct",  # Default group for directly connected nodes
            category="node"
        )
        
        return meshtastic_node
    
    except Exception as e:
        logger.error(f"Error converting node to model: {e}")
        # Create a minimal node with the ID
        return MeshtasticNode(
            node_id=str(node.id) if hasattr(node, 'id') else "unknown",
            last_seen=int(time.time())
        )


async def sync_nodes_to_database(device_id: Optional[str] = None):
    """
    Sync nodes from a device to the database
    
    Args:
        device_id: Device ID (uses default if None)
    """
    device = device_manager.get_device(device_id)
    if not device:
        logger.warning("No device available for syncing nodes")
        return
    
    try:
        # Get nodes from device
        nodes = device.get_nodes()
        if not nodes:
            logger.warning("No nodes found in device")
            return
        
        # Convert and save each node
        for node_id, node in nodes.items():
            if node_id == LOCAL_ADDR:
                continue  # Skip our own node
            
            meshtastic_node = convert_node_to_model(node)
            meshtastic_node.save()
        
        logger.info(f"Synced {len(nodes) - 1} nodes to database")
    
    except Exception as e:
        logger.error(f"Error syncing nodes to database: {e}")


async def initialize(mqtt_handler=None):
    """
    Initialize the direct Meshtastic integration
    
    Args:
        mqtt_handler: Optional MQTT handler for hybrid operation
    """
    logger.info("Initializing direct Meshtastic integration")
    
    # You could add auto-discovery and connection code here if desired
    
    return True


# Event handlers for direct device interface

async def handle_node_updated(node, device):
    """
    Handle node updates from a device
    
    Args:
        node: Updated node
        device: Source device interface
    """
    try:
        # Convert the node to our model
        meshtastic_node = convert_node_to_model(node)
        
        # Save to database
        meshtastic_node.save()
        
        logger.debug(f"Updated node {meshtastic_node.node_id} from direct connection")
    except Exception as e:
        logger.error(f"Error handling node update: {e}")

async def handle_message_received(packet, device):
    """
    Handle received message from a device
    
    Args:
        packet: Message packet
        device: Source device interface
    """
    try:
        # Check if it's a text message
        if hasattr(packet, 'decoded') and packet.decoded.portnum == 1:  # TEXT_MESSAGE_APP
            # Extract message details
            text = packet.decoded.payload.decode('utf-8', errors='replace')
            from_id = packet.from_node
            to_id = packet.to_node
            timestamp = packet.rx_time
            
            # Create message object
            message = Message(
                text=text,
                from_id=str(from_id),
                to_id=str(to_id),
                time=timestamp or int(time.time())
            )
            
            logger.info(f"Received text message from {from_id} to {to_id}: {text}")
            
            # Here you could add code to store the message, notify clients, etc.
    except Exception as e:
        logger.error(f"Error handling received message: {e}")

async def handle_telemetry_received(packet, device):
    """
    Handle telemetry data from a device
    
    Args:
        packet: Telemetry packet
        device: Source device interface
    """
    try:
        if hasattr(packet, 'decoded') and packet.decoded.portnum == 8:  # TELEMETRY_APP
            from_id = str(packet.from_node)
            
            # Get the node from database
            node = MeshtasticNode.get(from_id)
            if not node:
                logger.warning(f"Received telemetry for unknown node: {from_id}")
                return
            
            # TODO: Extract telemetry data and update the node
            # This would require parsing the protobuf message
            
            # Update last seen time
            node.last_seen = int(time.time())
            node.save()
            
            logger.debug(f"Updated telemetry for node {from_id}")
    except Exception as e:
        logger.error(f"Error handling telemetry: {e}")

async def handle_position_received(packet, device):
    """
    Handle position data from a device
    
    Args:
        packet: Position packet
        device: Source device interface
    """
    try:
        if hasattr(packet, 'decoded') and packet.decoded.portnum == 3:  # POSITION_APP
            from_id = str(packet.from_node)
            
            # Get the node from database
            node = MeshtasticNode.get(from_id)
            if not node:
                logger.warning(f"Received position for unknown node: {from_id}")
                return
            
            # TODO: Extract position data and update the node
            # This would require parsing the protobuf message
            
            # Update last seen time
            node.last_seen = int(time.time())
            node.save()
            
            logger.debug(f"Updated position for node {from_id}")
    except Exception as e:
        logger.error(f"Error handling position: {e}")

def setup_event_handlers(device):
    """
    Set up event handlers for a device
    
    Args:
        device: DeviceInterface to set up handlers for
    """
    if not device or not device.connected:
        return
    
    # Handler for received messages
    async def on_receive(packet):
        await handle_message_received(packet, device)
        
        # Check for specific message types
        if hasattr(packet, 'decoded'):
            if packet.decoded.portnum == 8:  # TELEMETRY_APP
                await handle_telemetry_received(packet, device)
            elif packet.decoded.portnum == 3:  # POSITION_APP
                await handle_position_received(packet, device)
    
    # Handler for node updates
    async def on_node_updated(node):
        await handle_node_updated(node, device)
    
    # Add handlers to the device
    device.add_event_handler("onReceive", on_receive)
    device.add_event_handler("onNodeUpdated", on_node_updated)


async def shutdown():
    """Shutdown the direct Meshtastic integration"""
    logger.info("Shutting down direct Meshtastic integration")
    
    # Disconnect all devices
    for device_id in list(device_manager.devices.keys()):
        await device_manager.disconnect_device(device_id) 