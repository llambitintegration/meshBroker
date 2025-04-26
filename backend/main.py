"""
Main FastAPI application for mesh broker
"""
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from .routers import mesh, status, meshtastic

app = FastAPI(
    title="Mesh Broker API",
    description="API for managing 3D mesh models",
    version="0.1.0"
)

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # In production, replace with specific origins
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Include routers
app.include_router(mesh.router)
app.include_router(status.router)
app.include_router(meshtastic.router)

@app.get("/")
def read_root():
    """Root endpoint"""
    return {
        "message": "Welcome to Mesh Broker API",
        "docs": "/docs",
        "redoc": "/redoc",
        "status": "/status"
    }