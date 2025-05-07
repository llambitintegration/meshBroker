"""
Main FastAPI application for mesh broker
"""
from fastapi import FastAPI, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
import time
from .routers import mesh, status, meshtastic, mqtt_publish, mqtt_subscribe
from .ws.routes import router as websocket_router
from .config import settings
from .mqtt.mqtt_handler import initialize_mqtt_handler
from .auth.rate_limiter import get_limiter

# Initialize app and limiter
app = FastAPI(
    title="Mesh Broker API",
    description="API for managing Meshtastic mesh networks",
    version="0.1.0"
)
limiter = get_limiter()

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.CORS_ORIGINS,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Initialize MQTT Handler
mqtt_handler = initialize_mqtt_handler(
    broker_host=settings.MQTT_BROKER_HOST,
    broker_port=settings.MQTT_BROKER_PORT,
    client_id=settings.MQTT_CLIENT_ID,
    username=settings.MQTT_USERNAME if settings.MQTT_USERNAME else None,
    password=settings.MQTT_PASSWORD if settings.MQTT_PASSWORD else None,
    use_tls=settings.MQTT_USE_TLS,
    keepalive=settings.MQTT_KEEPALIVE,
    default_qos=settings.MQTT_QOS,
    persistence_enabled=settings.PERSISTENCE_ENABLED,
    persistence_path=settings.PERSISTENCE_PATH,
    max_queue_size=settings.MAX_QUEUE_SIZE,
    worker_count=settings.QUEUE_WORKER_COUNT,
    max_retries=settings.MAX_RETRIES
)

# Include routers
app.include_router(mesh.router)
app.include_router(status.router)
app.include_router(meshtastic.router)
app.include_router(mqtt_publish.router)
app.include_router(mqtt_subscribe.router)
app.include_router(websocket_router)

# Set up handler references in routers
status.set_mqtt_handler(mqtt_handler)

@app.get("/")
def read_root():
    """Root endpoint"""
    return {
        "message": "Welcome to Mesh Broker API",
        "docs": "/docs",
        "redoc": "/redoc",
        "status": "/status"
    }

@app.get("/api/health")
def health_check():
    """Health check endpoint for the frontend"""
    return {
        "status": "ok",
        "timestamp": int(time.time()),
        "service": "Meshtastic MQTT Bridge API"
    }

# Add startup and shutdown events
@app.on_event("startup")
async def startup_event():
    """Initialize the MQTT handler and Meshtastic integration on startup"""
    # Connect to MQTT broker
    mqtt_handler.connect()
    
    # Initialize Meshtastic integration
    from . import meshtastic_integration
    await meshtastic_integration.initialize(mqtt_handler)

@app.on_event("shutdown")
async def shutdown_event():
    """Disconnect from MQTT broker on shutdown"""
    # Disconnect from MQTT broker
    mqtt_handler.disconnect()