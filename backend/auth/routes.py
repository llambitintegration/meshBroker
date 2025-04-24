"""
Authentication routes for FastAPI
"""
from fastapi import APIRouter, Depends, HTTPException, status, Response
from fastapi.security import OAuth2PasswordRequestForm
from datetime import datetime, timedelta
from typing import Optional, List, Dict, Any

import logging
from pydantic import BaseModel, EmailStr, Field

from backend.models.user import User, Role
from backend.auth.auth_handler import (
    get_password_hash, 
    verify_password, 
    create_access_token, 
    create_refresh_token,
    decode_token,
    get_current_active_user,
    get_admin_user,
    get_api_key
)
from backend.auth.auth_dependencies import JWTBearer
from backend.auth.rate_limiter import RateLimiter
from backend.config import settings

# Configure logger
logger = logging.getLogger(__name__)

# Rate limiter
rate_limiter = RateLimiter(rate=10, per=60)  # 10 requests per minute

# Create router
router = APIRouter(prefix="/auth", tags=["Authentication"])

# Models
class UserCreate(BaseModel):
    """User creation model"""
    username: str
    email: EmailStr
    password: str = Field(..., min_length=8)
    first_name: Optional[str] = ""
    last_name: Optional[str] = ""


class UserResponse(BaseModel):
    """User response model"""
    username: str
    email: str
    first_name: str
    last_name: str
    role: str
    is_active: bool
    is_verified: bool
    created_at: float
    last_login: Optional[float] = None
    rate_limit: int


class UserUpdate(BaseModel):
    """User update model"""
    email: Optional[EmailStr] = None
    first_name: Optional[str] = None
    last_name: Optional[str] = None
    role: Optional[str] = None
    is_active: Optional[bool] = None
    rate_limit: Optional[int] = None


class UserPasswordUpdate(BaseModel):
    """User password update model"""
    current_password: str
    new_password: str = Field(..., min_length=8)


class TokenResponse(BaseModel):
    """Token response model"""
    access_token: str
    refresh_token: str
    token_type: str = "bearer"
    expires_in: int


class RefreshRequest(BaseModel):
    """Refresh token request model"""
    refresh_token: str


class APIKeyResponse(BaseModel):
    """API key response model"""
    api_key: str
    created_at: float


# Routes
@router.post("/register", response_model=UserResponse, status_code=status.HTTP_201_CREATED)
async def register_user(user_data: UserCreate, admin_user: User = Depends(get_admin_user)):
    """Register a new user (admin only)"""
    # Check if username or email already exists
    existing_user = User.get(username=user_data.username)
    if existing_user:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Username already registered"
        )
    
    existing_email = User.get_by_email(email=user_data.email)
    if existing_email:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Email already registered"
        )
    
    # Create new user
    hashed_password = get_password_hash(user_data.password)
    new_user = User(
        username=user_data.username,
        email=user_data.email,
        hashed_password=hashed_password,
        first_name=user_data.first_name,
        last_name=user_data.last_name,
        role=Role.USER,
        is_active=True,
        is_verified=False,
        created_at=datetime.now().timestamp(),
        rate_limit=settings.DEFAULT_RATE_LIMIT
    )
    
    if not new_user.save():
        logger.error(f"Failed to save new user {user_data.username}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to create user"
        )
    
    logger.info(f"User {user_data.username} created by admin {admin_user.username}")
    
    return UserResponse(
        username=new_user.username,
        email=new_user.email,
        first_name=new_user.first_name,
        last_name=new_user.last_name,
        role=new_user.role,
        is_active=new_user.is_active,
        is_verified=new_user.is_verified,
        created_at=new_user.created_at,
        last_login=new_user.last_login,
        rate_limit=new_user.rate_limit
    )


@router.post("/token", response_model=TokenResponse)
async def login_for_access_token(
    form_data: OAuth2PasswordRequestForm = Depends(),
    _: None = Depends(rate_limiter)
):
    """Get access token from username and password"""
    user = User.get(username=form_data.username)
    
    if not user or not verify_password(form_data.password, user.hashed_password):
        logger.warning(f"Failed login attempt for user {form_data.username}")
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Incorrect username or password",
            headers={"WWW-Authenticate": "Bearer"},
        )
    
    if not user.is_active:
        logger.warning(f"Login attempt for inactive user {user.username}")
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="User is inactive",
            headers={"WWW-Authenticate": "Bearer"},
        )
    
    # Update last login time
    user.update_last_login()
    
    # Create tokens
    access_token_expires = timedelta(minutes=settings.ACCESS_TOKEN_EXPIRE_MINUTES)
    refresh_token_expires = timedelta(days=settings.REFRESH_TOKEN_EXPIRE_DAYS)
    
    access_token = create_access_token(
        data={"sub": user.username, "role": user.role},
        expires_delta=access_token_expires
    )
    
    refresh_token = create_refresh_token(
        data={"sub": user.username},
        expires_delta=refresh_token_expires
    )
    
    logger.info(f"User {user.username} logged in successfully")
    
    return TokenResponse(
        access_token=access_token,
        refresh_token=refresh_token,
        token_type="bearer",
        expires_in=settings.ACCESS_TOKEN_EXPIRE_MINUTES * 60
    )


@router.post("/refresh", response_model=TokenResponse)
async def refresh_access_token(
    refresh_request: RefreshRequest,
    _: None = Depends(rate_limiter)
):
    """Refresh access token using refresh token"""
    # Decode refresh token
    payload = decode_token(refresh_request.refresh_token)
    if not payload:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid refresh token",
            headers={"WWW-Authenticate": "Bearer"},
        )
    
    # Check token type
    if payload.get("type") != "refresh":
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid token type",
            headers={"WWW-Authenticate": "Bearer"},
        )
    
    # Get username from token
    username = payload.get("sub")
    if not username:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid token",
            headers={"WWW-Authenticate": "Bearer"},
        )
    
    # Get user from database
    user = User.get(username=username)
    if not user or not user.is_active:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="User not found or inactive",
            headers={"WWW-Authenticate": "Bearer"},
        )
    
    # Create new tokens
    access_token_expires = timedelta(minutes=settings.ACCESS_TOKEN_EXPIRE_MINUTES)
    refresh_token_expires = timedelta(days=settings.REFRESH_TOKEN_EXPIRE_DAYS)
    
    access_token = create_access_token(
        data={"sub": user.username, "role": user.role},
        expires_delta=access_token_expires
    )
    
    refresh_token = create_refresh_token(
        data={"sub": user.username},
        expires_delta=refresh_token_expires
    )
    
    logger.info(f"Refreshed tokens for user {user.username}")
    
    return TokenResponse(
        access_token=access_token,
        refresh_token=refresh_token,
        token_type="bearer",
        expires_in=settings.ACCESS_TOKEN_EXPIRE_MINUTES * 60
    )


@router.get("/me", response_model=UserResponse)
async def get_user_me(current_user: User = Depends(get_current_active_user)):
    """Get current user information"""
    return UserResponse(
        username=current_user.username,
        email=current_user.email,
        first_name=current_user.first_name,
        last_name=current_user.last_name,
        role=current_user.role,
        is_active=current_user.is_active,
        is_verified=current_user.is_verified,
        created_at=current_user.created_at,
        last_login=current_user.last_login,
        rate_limit=current_user.rate_limit
    )


@router.put("/me", response_model=UserResponse)
async def update_user_me(
    user_update: UserUpdate,
    current_user: User = Depends(get_current_active_user)
):
    """Update current user information"""
    # Only allow updating certain fields
    if user_update.email is not None:
        # Check if email is already in use
        existing = User.get_by_email(user_update.email)
        if existing and existing.username != current_user.username:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Email already in use"
            )
        current_user.email = user_update.email
    
    if user_update.first_name is not None:
        current_user.first_name = user_update.first_name
    
    if user_update.last_name is not None:
        current_user.last_name = user_update.last_name
    
    # Role and active status can only be updated by admins through admin routes
    
    # Save changes
    if not current_user.save():
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to update user"
        )
    
    logger.info(f"User {current_user.username} updated their profile")
    
    return UserResponse(
        username=current_user.username,
        email=current_user.email,
        first_name=current_user.first_name,
        last_name=current_user.last_name,
        role=current_user.role,
        is_active=current_user.is_active,
        is_verified=current_user.is_verified,
        created_at=current_user.created_at,
        last_login=current_user.last_login,
        rate_limit=current_user.rate_limit
    )


@router.put("/me/password", status_code=status.HTTP_200_OK)
async def update_password(
    password_update: UserPasswordUpdate,
    current_user: User = Depends(get_current_active_user)
):
    """Update current user password"""
    # Verify current password
    if not verify_password(password_update.current_password, current_user.hashed_password):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Incorrect current password"
        )
    
    # Update password
    current_user.hashed_password = get_password_hash(password_update.new_password)
    
    # Save changes
    if not current_user.save():
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to update password"
        )
    
    logger.info(f"User {current_user.username} updated their password")
    
    return {"message": "Password updated successfully"}


@router.post("/api-key", response_model=APIKeyResponse)
async def generate_api_key(current_user: User = Depends(get_current_active_user)):
    """Generate a new API key for the current user"""
    # Verify user is allowed to have an API key (ADMIN or API role)
    if current_user.role != Role.ADMIN and current_user.role != Role.API:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Only users with API or ADMIN role can generate API keys"
        )
    
    # Generate new API key
    current_user.api_key = get_api_key()
    
    # Save changes
    if not current_user.save():
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to generate API key"
        )
    
    logger.info(f"User {current_user.username} generated a new API key")
    
    return APIKeyResponse(
        api_key=current_user.api_key,
        created_at=datetime.now().timestamp()
    )


@router.delete("/api-key", status_code=status.HTTP_200_OK)
async def revoke_api_key(current_user: User = Depends(get_current_active_user)):
    """Revoke the current API key"""
    if not current_user.api_key:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="No API key found for this user"
        )
    
    # Remove API key
    current_user.api_key = None
    
    # Save changes
    if not current_user.save():
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to revoke API key"
        )
    
    logger.info(f"User {current_user.username} revoked their API key")
    
    return {"message": "API key revoked successfully"}


# Admin Routes
@router.get("/users", response_model=List[UserResponse])
async def get_all_users(admin_user: User = Depends(get_admin_user)):
    """Get all users (admin only)"""
    users = User.get_all()
    
    return [
        UserResponse(
            username=user.username,
            email=user.email,
            first_name=user.first_name,
            last_name=user.last_name,
            role=user.role,
            is_active=user.is_active,
            is_verified=user.is_verified,
            created_at=user.created_at,
            last_login=user.last_login,
            rate_limit=user.rate_limit
        )
        for user in users
    ]


@router.get("/users/{username}", response_model=UserResponse)
async def get_user(username: str, admin_user: User = Depends(get_admin_user)):
    """Get user by username (admin only)"""
    user = User.get(username=username)
    if not user:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="User not found"
        )
    
    return UserResponse(
        username=user.username,
        email=user.email,
        first_name=user.first_name,
        last_name=user.last_name,
        role=user.role,
        is_active=user.is_active,
        is_verified=user.is_verified,
        created_at=user.created_at,
        last_login=user.last_login,
        rate_limit=user.rate_limit
    )


@router.put("/users/{username}", response_model=UserResponse)
async def update_user(
    username: str,
    user_update: UserUpdate,
    admin_user: User = Depends(get_admin_user)
):
    """Update user by username (admin only)"""
    user = User.get(username=username)
    if not user:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="User not found"
        )
    
    # Don't allow changing your own role if you're an admin
    if username == admin_user.username and user_update.role is not None:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Cannot change your own role"
        )
    
    # Update fields
    if user_update.email is not None:
        # Check if email is already in use
        existing = User.get_by_email(user_update.email)
        if existing and existing.username != username:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Email already in use"
            )
        user.email = user_update.email
    
    if user_update.first_name is not None:
        user.first_name = user_update.first_name
    
    if user_update.last_name is not None:
        user.last_name = user_update.last_name
    
    if user_update.role is not None:
        # Validate role
        try:
            user.role = Role(user_update.role)
        except ValueError:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Invalid role '{user_update.role}'"
            )
    
    if user_update.is_active is not None:
        user.is_active = user_update.is_active
    
    if user_update.rate_limit is not None:
        user.rate_limit = user_update.rate_limit
    
    # Save changes
    if not user.save():
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to update user"
        )
    
    logger.info(f"Admin {admin_user.username} updated user {username}")
    
    return UserResponse(
        username=user.username,
        email=user.email,
        first_name=user.first_name,
        last_name=user.last_name,
        role=user.role,
        is_active=user.is_active,
        is_verified=user.is_verified,
        created_at=user.created_at,
        last_login=user.last_login,
        rate_limit=user.rate_limit
    )


@router.delete("/users/{username}", status_code=status.HTTP_200_OK)
async def delete_user(username: str, admin_user: User = Depends(get_admin_user)):
    """Delete user by username (admin only)"""
    # Don't allow deleting yourself
    if username == admin_user.username:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Cannot delete your own account"
        )
    
    user = User.get(username=username)
    if not user:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="User not found"
        )
    
    # Delete user
    if not User.delete(username):
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to delete user"
        )
    
    logger.info(f"Admin {admin_user.username} deleted user {username}")
    
    return {"message": f"User {username} deleted successfully"} 