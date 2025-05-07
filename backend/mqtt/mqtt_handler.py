"""
MQTT handler dependency functions
"""
from fastapi import Depends

# Global instance of MQTTHandler
_mqtt_handler = None

def initialize_mqtt_handler(
    broker_host, 
    broker_port,
    client_id,
    username=None,
    password=None,
    use_tls=False,
    **kwargs
):
    """Initialize the global MQTT handler"""
    global _mqtt_handler
    from ..mqtt_handler import MQTTHandler
    _mqtt_handler = MQTTHandler(
        broker_host=broker_host,
        broker_port=broker_port,
        client_id=client_id,
        username=username,
        password=password,
        use_tls=use_tls,
        **kwargs
    )
    return _mqtt_handler

def get_mqtt_handler():
    """Dependency function to get the MQTT handler"""
    global _mqtt_handler
    if _mqtt_handler is None:
        raise RuntimeError("MQTT Handler not initialized")
    return _mqtt_handler