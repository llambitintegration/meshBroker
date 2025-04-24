import time
import logging
from fastapi import Request, Response
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.types import ASGIApp
from typing import Callable, Dict, Any

from backend.monitoring.monitoring import get_metrics_manager

# Configure logger
logger = logging.getLogger(__name__)

class MetricsMiddleware(BaseHTTPMiddleware):
    """Middleware for collecting API metrics"""
    
    def __init__(self, app: ASGIApp):
        """Initialize the metrics middleware"""
        super().__init__(app)
        self.metrics_manager = get_metrics_manager()
    
    async def dispatch(self, request: Request, call_next: Callable) -> Response:
        """Process request and record metrics
        
        Args:
            request: The incoming request
            call_next: The next middleware or endpoint handler
            
        Returns:
            The response from the next handler
        """
        # Extract path and method
        path = request.url.path
        method = request.method
        
        # Skip metrics collection for certain paths
        if path.startswith("/monitoring/metrics") or path == "/monitoring/health":
            return await call_next(request)
        
        # Record start time
        start_time = time.time()
        
        # Call next middleware or endpoint
        try:
            response = await call_next(request)
            status_code = response.status_code
        except Exception as e:
            # Record error
            logger.error(f"Error in request {method} {path}: {e}")
            status_code = 500
            raise
        finally:
            # Calculate duration
            duration = time.time() - start_time
            
            # Record metrics
            try:
                # Increment request counter with labels
                self.metrics_manager.api_requests.inc(
                    label_values={
                        "endpoint": path,
                        "method": method,
                        "status": str(status_code)
                    }
                )
                
                # Record request duration
                self.metrics_manager.api_request_duration.observe(
                    duration,
                    label_values={
                        "endpoint": path,
                        "method": method
                    }
                )
                
                # Log long requests
                if duration > 1.0:
                    logger.warning(
                        f"Slow request: {method} {path} took {duration:.2f}s " +
                        f"with status {status_code}"
                    )
                
            except Exception as e:
                logger.error(f"Error recording metrics: {e}")
        
        return response


def add_metrics_middleware(app) -> None:
    """Add metrics middleware to FastAPI app
    
    Args:
        app: The FastAPI application instance
    """
    app.add_middleware(MetricsMiddleware)
    logger.info("Added metrics middleware") 