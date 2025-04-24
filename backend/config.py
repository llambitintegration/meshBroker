import os
import secrets
from pydantic_settings import BaseSettings
import logging

class Settings(BaseSettings):
    """Application settings"""
    
    # MQTT Broker settings
    MQTT_BROKER_HOST: str = os.getenv("MQTT_BROKER_HOST", "127.0.0.1")
    MQTT_BROKER_PORT: int = int(os.getenv("MQTT_BROKER_PORT", 1883))
    MQTT_CLIENT_ID: str = os.getenv("MQTT_CLIENT_ID", "meshtastic_fastapi_bridge")
    MQTT_USERNAME: str = os.getenv("MQTT_USERNAME", "")
    MQTT_PASSWORD: str = os.getenv("MQTT_PASSWORD", "")
    MQTT_USE_TLS: bool = os.getenv("MQTT_USE_TLS", "false").lower() == "true"
    MQTT_KEEPALIVE: int = int(os.getenv("MQTT_KEEPALIVE", 60))
    MQTT_QOS: int = int(os.getenv("MQTT_QOS", 1))
    
    # Message persistence settings
    PERSISTENCE_ENABLED: bool = os.getenv("PERSISTENCE_ENABLED", "true").lower() == "true"
    PERSISTENCE_PATH: str = os.getenv("PERSISTENCE_PATH", "mqtt_data")
    
    # Message queue settings
    MAX_QUEUE_SIZE: int = int(os.getenv("MAX_QUEUE_SIZE", 10000))
    QUEUE_WORKER_COUNT: int = int(os.getenv("QUEUE_WORKER_COUNT", 2))
    HIGH_WATERMARK: float = float(os.getenv("HIGH_WATERMARK", 0.8))
    LOW_WATERMARK: float = float(os.getenv("LOW_WATERMARK", 0.6))
    
    # Retry settings
    MAX_RETRIES: int = int(os.getenv("MAX_RETRIES", 5))
    BASE_RETRY_DELAY: float = float(os.getenv("BASE_RETRY_DELAY", 1.0))
    MAX_RETRY_DELAY: float = float(os.getenv("MAX_RETRY_DELAY", 60.0))
    
    # Broker monitoring settings
    BROKER_MONITOR_ENABLED: bool = os.getenv("BROKER_MONITOR_ENABLED", "true").lower() == "true"
    BROKER_CHECK_INTERVAL: float = float(os.getenv("BROKER_CHECK_INTERVAL", 30.0))
    
    # Meshtastic settings
    MESHTASTIC_TOPIC_PREFIX: str = os.getenv("MESHTASTIC_TOPIC_PREFIX", "msh/tx/")
    MESHTASTIC_RECONNECT_INTERVAL: int = int(os.getenv("MESHTASTIC_RECONNECT_INTERVAL", 10))
    
    # API settings
    API_HOST: str = os.getenv("API_HOST", "0.0.0.0")
    API_PORT: int = int(os.getenv("API_PORT", 8000))
    
    # Logging settings
    LOG_LEVEL: str = os.getenv("LOG_LEVEL", "INFO")
    
    # CORS settings
    CORS_ORIGINS: list = ["*"]  # Wide open for local development
    
    # Authentication settings
    JWT_SECRET: str = os.getenv("JWT_SECRET", secrets.token_hex(32))
    JWT_ALGORITHM: str = os.getenv("JWT_ALGORITHM", "HS256")
    ACCESS_TOKEN_EXPIRE_MINUTES: int = int(os.getenv("ACCESS_TOKEN_EXPIRE_MINUTES", 60))
    REFRESH_TOKEN_EXPIRE_DAYS: int = int(os.getenv("REFRESH_TOKEN_EXPIRE_DAYS", 7))
    
    # Rate limiting settings
    DEFAULT_RATE_LIMIT: int = int(os.getenv("DEFAULT_RATE_LIMIT", 100))  # requests per minute
    ADMIN_RATE_LIMIT: int = int(os.getenv("ADMIN_RATE_LIMIT", 300))  # requests per minute
    API_RATE_LIMIT: int = int(os.getenv("API_RATE_LIMIT", 600))  # requests per minute
    
    # WebSocket settings
    WS_MAX_CONNECTIONS: int = int(os.getenv("WS_MAX_CONNECTIONS", 100))
    WS_PING_INTERVAL: int = int(os.getenv("WS_PING_INTERVAL", 30))  # seconds
    WS_PING_TIMEOUT: int = int(os.getenv("WS_PING_TIMEOUT", 10))  # seconds
    
    model_config = {
        "env_file": ".env"
    }

# Create settings instance
settings = Settings()

# Configure logging level based on settings
def configure_logging():
    """Configure logging based on environment settings"""
    log_level = getattr(logging, settings.LOG_LEVEL.upper(), logging.INFO)
    logging.basicConfig(
        level=log_level,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )
    return log_level
