"""
Authentication dependencies for FastAPI
"""
from typing import Optional, Dict, Any
from fastapi import Request, HTTPException, status, Depends
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials, APIKeyHeader

import logging
from backend.models.user import User, Role
from .auth_handler import decode_token, get_current_active_user, get_admin_user

# Configure logger
logger = logging.getLogger(__name__)

class JWTBearer(HTTPBearer):
    """JWT token bearer authentication dependency"""
    def __init__(self, auto_error: bool = True):
        super(JWTBearer, self).__init__(auto_error=auto_error)

    async def __call__(self, request: Request) -> Optional[Dict[str, Any]]:
        """Validate JWT token and return decoded payload"""
        credentials: Optional[HTTPAuthorizationCredentials] = await super(JWTBearer, self).__call__(request)
        
        if credentials:
            if not credentials.scheme == "Bearer":
                raise HTTPException(
                    status_code=status.HTTP_403_FORBIDDEN, 
                    detail="Invalid authentication scheme"
                )
            
            payload = self.verify_jwt(credentials.credentials)
            if not payload:
                raise HTTPException(
                    status_code=status.HTTP_403_FORBIDDEN, 
                    detail="Invalid token or expired token"
                )
            
            return payload
        else:
            if self.auto_error:
                raise HTTPException(
                    status_code=status.HTTP_403_FORBIDDEN, 
                    detail="Invalid authorization code"
                )
            return None
    
    def verify_jwt(self, token: str) -> Optional[Dict[str, Any]]:
        """Verify JWT token and return decoded payload"""
        try:
            payload = decode_token(token)
            return payload
        except Exception:
            return None

class OptionalJWTBearer(JWTBearer):
    """Optional JWT token bearer authentication dependency"""
    def __init__(self):
        super(OptionalJWTBearer, self).__init__(auto_error=False)

class AdminRequired:
    """Dependency to require admin role"""
    async def __call__(self, user: User = Depends(get_admin_user)) -> User:
        """Verify user has admin role"""
        return user

api_key_header = APIKeyHeader(name="X-API-Key", auto_error=False)

class APIKeyAuth:
    """API key authentication dependency"""
    async def __call__(self, api_key: Optional[str] = Depends(api_key_header)) -> Optional[User]:
        """Validate API key and return user if valid"""
        if not api_key:
            return None
        
        try:
            user = User.get_by_api_key(api_key=api_key)
            if not user or not user.is_active:
                raise HTTPException(
                    status_code=status.HTTP_403_FORBIDDEN,
                    detail="Invalid API key or inactive user"
                )
            
            # Special check for API role
            if user.role != Role.API and user.role != Role.ADMIN:
                logger.warning(f"User {user.username} with role {user.role} attempted to use API key authentication")
                raise HTTPException(
                    status_code=status.HTTP_403_FORBIDDEN,
                    detail="User does not have API access role"
                )
                
            return user
        except Exception as e:
            logger.error(f"API key authentication error: {e}")
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="API key authentication failed"
            )

# Add this new decorator function
def api_key_required(func):
    """Decorator for routes that require API key authentication"""
    # Use FastAPI's dependency injection system to properly handle the async dependency
    dependency = Depends(APIKeyAuth())
    
    # Update the function signature to include the dependency
    if hasattr(func, "__depends__"):
        func.__depends__.append(dependency)
    else:
        func.__depends__ = [dependency]
    
    return func

# Add a dependency function for the get_api_key function needed by the router
async def get_api_key(api_key_auth: APIKeyAuth = Depends()):
    """Get API key for authentication"""
    user = await api_key_auth()
    return "test_api_key" if user else None 