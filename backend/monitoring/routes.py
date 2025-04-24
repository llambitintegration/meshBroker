from fastapi import APIRouter, Request, Depends, HTTPException, status
from fastapi.responses import JSONResponse
import time
import logging
import os
import psutil
from typing import Dict, Any, List, Optional

# Import directly from monitoring.py instead of from the package
from backend.monitoring.monitoring import get_metrics_manager
from backend.auth import JWTBearer, AdminRequired
from backend.auth.rate_limiter import get_limiter

# Configure logger
logger = logging.getLogger(__name__)

# Initialize router with prefix
router = APIRouter(
    prefix="/monitoring",
    tags=["monitoring"],
    dependencies=[Depends(JWTBearer())],
    responses={404: {"description": "Not found"}},
)

# Get reference to rate limiter
limiter = get_limiter()


@router.get("/metrics")
@limiter.limit("30/minute")
async def get_all_metrics(request: Request):
    """Get all metrics from the system"""
    try:
        metrics_manager = get_metrics_manager()
        return JSONResponse(
            content=metrics_manager.get_metrics()
        )
    except Exception as e:
        logger.error(f"Error getting metrics: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to retrieve metrics: {str(e)}"
        )


@router.get("/metrics/{metric_name}")
@limiter.limit("60/minute")
async def get_specific_metric(metric_name: str, request: Request):
    """Get a specific metric by name"""
    try:
        metrics_manager = get_metrics_manager()
        metric = metrics_manager.get_metric(metric_name)
        
        if not metric:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Metric '{metric_name}' not found"
            )
            
        return JSONResponse(content=metric)
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting metric {metric_name}: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to retrieve metric: {str(e)}"
        )


@router.get("/system")
@limiter.limit("30/minute")
async def get_system_info(request: Request):
    """Get system information and statistics"""
    try:
        # Basic system information
        system_info = {
            "hostname": os.uname().nodename if hasattr(os, "uname") else "unknown",
            "platform": os.name,
            "uptime_seconds": int(time.time() - psutil.boot_time()),
            "cpu": {
                "count_physical": psutil.cpu_count(logical=False),
                "count_logical": psutil.cpu_count(logical=True),
                "usage_percent": psutil.cpu_percent(interval=None, percpu=False),
                "per_cpu_percent": psutil.cpu_percent(interval=None, percpu=True)
            },
            "memory": {
                "total_mb": round(psutil.virtual_memory().total / (1024 * 1024), 2),
                "available_mb": round(psutil.virtual_memory().available / (1024 * 1024), 2),
                "used_mb": round(psutil.virtual_memory().used / (1024 * 1024), 2),
                "percent": psutil.virtual_memory().percent
            },
            "disk": {
                "total_gb": round(psutil.disk_usage('/').total / (1024 * 1024 * 1024), 2),
                "used_gb": round(psutil.disk_usage('/').used / (1024 * 1024 * 1024), 2),
                "free_gb": round(psutil.disk_usage('/').free / (1024 * 1024 * 1024), 2),
                "percent": psutil.disk_usage('/').percent
            },
            "network": {
                "connections": len(psutil.net_connections())
            }
        }
        
        return JSONResponse(content=system_info)
    except Exception as e:
        logger.error(f"Error getting system info: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to retrieve system information: {str(e)}"
        )


@router.get("/process")
@limiter.limit("30/minute")
async def get_process_info(request: Request):
    """Get information about the current process"""
    try:
        process = psutil.Process(os.getpid())
        
        with process.oneshot():
            process_info = {
                "pid": process.pid,
                "name": process.name(),
                "username": process.username(),
                "created_time": process.create_time(),
                "cpu": {
                    "percent": process.cpu_percent(interval=None),
                    "num_threads": process.num_threads()
                },
                "memory": {
                    "rss_mb": round(process.memory_info().rss / (1024 * 1024), 2),
                    "vms_mb": round(process.memory_info().vms / (1024 * 1024), 2),
                    "percent": process.memory_percent()
                },
                "io": {
                    "read_count": process.io_counters().read_count if hasattr(process.io_counters(), "read_count") else 0,
                    "write_count": process.io_counters().write_count if hasattr(process.io_counters(), "write_count") else 0,
                    "read_bytes": process.io_counters().read_bytes if hasattr(process.io_counters(), "read_bytes") else 0,
                    "write_bytes": process.io_counters().write_bytes if hasattr(process.io_counters(), "write_bytes") else 0
                },
                "connections": len(process.connections())
            }
            
        return JSONResponse(content=process_info)
    except Exception as e:
        logger.error(f"Error getting process info: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to retrieve process information: {str(e)}"
        )


@router.get("/health")
@limiter.limit("60/minute")
async def health_check(request: Request):
    """Basic health check endpoint that doesn't require authentication"""
    return {"status": "healthy", "timestamp": time.time()}


@router.get("/broker")
@limiter.limit("30/minute")
async def get_broker_status(request: Request):
    """Get detailed MQTT broker status"""
    # This endpoint needs to be implemented with integration to broker_monitor.py
    # Placeholder implementation for now
    try:
        # You'll need to reference the broker monitor from your app
        # This is just a placeholder
        broker_status = {
            "status": "placeholder",
            "details": "This endpoint needs to be implemented with integration to broker_monitor.py"
        }
        
        return JSONResponse(content=broker_status)
    except Exception as e:
        logger.error(f"Error getting broker status: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to retrieve broker status: {str(e)}"
        )


@router.get("/benchmark/mqtt")
@limiter.limit("5/minute")
async def run_mqtt_benchmark(
    request: Request,
    message_count: int = 100,
    message_size: int = 256,
    qos: int = 1,
    _=Depends(AdminRequired())
):
    """Run a quick MQTT performance benchmark
    
    Only administrators can run this endpoint.
    """
    try:
        # Placeholder response
        result = {
            "status": "not_implemented",
            "description": "MQTT benchmark needs to be implemented"
        }
        
        return JSONResponse(content=result)
    except Exception as e:
        logger.error(f"Error running MQTT benchmark: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, 
            detail=f"Failed to run MQTT benchmark: {str(e)}"
        )


@router.get("/log/level")
@limiter.limit("10/minute")
async def get_log_level(request: Request, _=Depends(AdminRequired())):
    """Get the current log level"""
    try:
        return {"level": logging.getLevelName(logger.level)}
    except Exception as e:
        logger.error(f"Error getting log level: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to get log level: {str(e)}"
        )


@router.post("/log/level/{level}")
@limiter.limit("5/minute")
async def set_log_level(
    level: str,
    request: Request,
    _=Depends(AdminRequired())
):
    """Set the log level (admin only)"""
    try:
        # Convert string to logging level
        numeric_level = getattr(logging, level.upper(), None)
        if not isinstance(numeric_level, int):
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Invalid log level: {level}"
            )
        
        # Set the root logger level
        logging.getLogger().setLevel(numeric_level)
        
        # Also set our package loggers
        for logger_name in logging.root.manager.loggerDict:
            if logger_name.startswith("backend"):
                logging.getLogger(logger_name).setLevel(numeric_level)
        
        return {"status": "success", "level": level.upper()}
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error setting log level: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to set log level: {str(e)}"
        ) 