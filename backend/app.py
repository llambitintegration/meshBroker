"""
Legacy application entry point that re-exports the main application
"""
import logging
from .main import app as application
from .config import settings

# Configure logging
logger = logging.getLogger(__name__)
logger.info("Using consolidated application from main.py")

# Re-export the application for backward compatibility
app = application

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("app:app", host=settings.API_HOST, port=settings.API_PORT, reload=True)