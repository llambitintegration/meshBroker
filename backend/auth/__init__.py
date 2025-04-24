"""
Authentication package for the backend
"""
from .auth_handler import (
    get_password_hash,
    verify_password,
    create_access_token,
    create_refresh_token,
    decode_token,
    get_current_user,
    get_current_active_user,
    get_admin_user,
    get_api_key
)
from .auth_dependencies import (
    JWTBearer,
    OptionalJWTBearer,
    AdminRequired,
    APIKeyAuth
)
from .rate_limiter import RateLimiter, get_limiter 