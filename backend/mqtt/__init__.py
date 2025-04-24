"""
MQTT functionality package for the backend
"""
from .settings_routes import router as mqtt_settings_router
from .settings_routes import set_mqtt_handler 