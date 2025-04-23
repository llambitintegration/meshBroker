import os
from pydantic_settings import BaseSettings

class Settings(BaseSettings):
    """Application settings"""
    
    # MQTT Broker settings
    MQTT_BROKER_HOST: str = os.getenv("MQTT_BROKER_HOST", "127.0.0.1")
    MQTT_BROKER_PORT: int = int(os.getenv("MQTT_BROKER_PORT", 1883))
    MQTT_CLIENT_ID: str = os.getenv("MQTT_CLIENT_ID", "meshtastic_fastapi_bridge")
    MQTT_USERNAME: str = os.getenv("MQTT_USERNAME", "")
    MQTT_PASSWORD: str = os.getenv("MQTT_PASSWORD", "")
    
    # Meshtastic settings
    MESHTASTIC_TOPIC_PREFIX: str = os.getenv("MESHTASTIC_TOPIC_PREFIX", "msh")
    
    # API settings
    API_HOST: str = os.getenv("API_HOST", "0.0.0.0")
    API_PORT: int = int(os.getenv("API_PORT", 8000))
    
    # CORS settings
    CORS_ORIGINS: list = ["*"]  # Wide open for local development
    
    model_config = {
        "env_file": ".env"
    }

# Create settings instance
settings = Settings()
