from pydantic import BaseModel
from typing import Any, Dict, List, Optional, Union, Generic, TypeVar

T = TypeVar('T')

class StatusResponse(BaseModel):
    """Standard status response"""
    success: bool
    message: str
    data: Optional[Dict[str, Any]] = None

class DataResponse(Generic[T], BaseModel):
    """Standard data response"""
    data: T

class ErrorResponse(BaseModel):
    """Standard error response"""
    success: bool = False
    error: str
    details: Optional[Dict[str, Any]] = None

class PaginatedResponse(Generic[T], BaseModel):
    """Standard paginated response"""
    items: List[T]
    total: int
    page: int
    size: int
    pages: int