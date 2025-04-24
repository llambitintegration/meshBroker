#!/usr/bin/env python3
import os
import argparse
import uvicorn
from dotenv import load_dotenv
import logging
import sys
from pathlib import Path

# Add the backend directory to the Python path
# This ensures that the backend module can be imported
backend_dir = Path(__file__).parent
sys.path.insert(0, str(backend_dir.parent))

# Import config after setting up the path
from backend.config import configure_logging

# Configure logging
logger = logging.getLogger(__name__)

def main():
    """
    Main function to run the Meshtastic MQTT Bridge backend
    """
    # Parse command line arguments
    parser = argparse.ArgumentParser(description='Run the Meshtastic MQTT Bridge backend')
    parser.add_argument('--host', type=str, default='0.0.0.0', help='Host to bind the server to')
    parser.add_argument('--port', type=int, default=8000, help='Port to bind the server to')
    parser.add_argument('--reload', action='store_true', help='Enable auto-reload')
    parser.add_argument('--env-file', type=str, default='.env', help='Path to .env file')
    parser.add_argument('--log-level', type=str, default=None, help='Logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL)')
    args = parser.parse_args()

    # Load environment variables from .env file
    env_path = args.env_file
    if os.path.exists(env_path):
        logger.info(f"Loading environment variables from {env_path}")
        load_dotenv(env_path)
    else:
        logger.warning(f"Environment file {env_path} not found, using default settings")

    # Override log level if specified in command line
    if args.log_level:
        os.environ["LOG_LEVEL"] = args.log_level.upper()
    
    # Configure logging with the settings
    log_level = configure_logging()
    
    # Log startup information
    logger.info(f"Starting Meshtastic MQTT Bridge backend on {args.host}:{args.port}")
    logger.info(f"Log level set to {logging.getLevelName(log_level)}")
    logger.info(f"MQTT Broker: {os.getenv('MQTT_BROKER_HOST', '127.0.0.1')}:{os.getenv('MQTT_BROKER_PORT', '1883')}")
    
    if os.getenv('MQTT_USERNAME'):
        logger.info(f"MQTT Authentication: Enabled (username: {os.getenv('MQTT_USERNAME')})")
    else:
        logger.warning("MQTT Authentication: Disabled")
        
    if os.getenv('MQTT_USE_TLS', 'false').lower() == 'true':
        logger.info("MQTT TLS: Enabled")
    else:
        logger.warning("MQTT TLS: Disabled")
    
    # Start uvicorn server
    uvicorn.run(
        "backend.app:app", 
        host=args.host, 
        port=args.port, 
        reload=args.reload,
        log_level=logging.getLevelName(log_level).lower()
    )

if __name__ == "__main__":
    main() 