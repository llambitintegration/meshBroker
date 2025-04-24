"""
Rate limiter implementation for API endpoints
"""
import time
from typing import Dict, Optional, Callable, Any
from fastapi import Request, Response, HTTPException, status, Depends
import logging
import threading
from slowapi import Limiter
from slowapi.util import get_remote_address

from backend.models.user import User
from .auth_handler import get_current_user

# Configure logger
logger = logging.getLogger(__name__)

# Global rate limiter instance
_limiter = None
_limiter_lock = threading.Lock()

def get_limiter() -> Limiter:
    """Get or create the rate limiter instance"""
    global _limiter
    
    if _limiter is None:
        with _limiter_lock:
            if _limiter is None:
                _limiter = Limiter(key_func=get_remote_address)
    
    return _limiter

class RateLimiter:
    """Rate limiter based on token bucket algorithm"""
    
    def __init__(self, rate: int = 10, per: int = 60):
        """
        Initialize rate limiter
        
        Args:
            rate: Number of requests allowed
            per: Time period in seconds
        """
        self.rate = rate
        self.per = per
        self.token_bucket: Dict[str, Dict[str, float]] = {}
        self.lock = threading.Lock()
    
    def _get_tokens(self, key: str) -> Dict[str, float]:
        """Get token bucket for key"""
        with self.lock:
            if key not in self.token_bucket:
                # Initialize token bucket
                self.token_bucket[key] = {
                    "tokens": self.rate,
                    "last_refill": time.time()
                }
            return self.token_bucket[key]
    
    def _refill_tokens(self, bucket: Dict[str, float]) -> None:
        """Refill tokens based on elapsed time"""
        now = time.time()
        time_passed = now - bucket["last_refill"]
        
        # Calculate tokens to add based on time passed
        new_tokens = time_passed * (self.rate / self.per)
        
        # Update bucket
        bucket["tokens"] = min(bucket["tokens"] + new_tokens, self.rate)
        bucket["last_refill"] = now
    
    def check_rate_limit(self, key: str) -> bool:
        """
        Check if request is within rate limit
        
        Args:
            key: Unique identifier for client (IP, user ID, etc.)
            
        Returns:
            bool: True if request is allowed, False otherwise
        """
        with self.lock:
            bucket = self._get_tokens(key)
            self._refill_tokens(bucket)
            
            if bucket["tokens"] >= 1:
                bucket["tokens"] -= 1
                return True
            else:
                return False
    
    def __call__(
        self, 
        request: Request, 
        user: Optional[User] = Depends(get_current_user)
    ) -> None:
        """
        FastAPI dependency for rate limiting
        
        Args:
            request: FastAPI request object
            user: Current user, if authenticated
        
        Raises:
            HTTPException: If rate limit exceeded
        """
        # Get identifier - use user ID if available, otherwise IP
        key = f"user:{user.username}" if user else f"ip:{request.client.host}"
        
        # Get rate limit - use user's rate limit if available
        rate = user.rate_limit if user else self.rate
        
        # Use actual user rate limit if available
        if user and hasattr(user, 'rate_limit'):
            self.rate = user.rate_limit
        
        # Check rate limit
        if not self.check_rate_limit(key):
            logger.warning(f"Rate limit exceeded for {key}")
            raise HTTPException(
                status_code=status.HTTP_429_TOO_MANY_REQUESTS,
                detail="Rate limit exceeded",
                headers={"Retry-After": str(self.per)}
            ) 