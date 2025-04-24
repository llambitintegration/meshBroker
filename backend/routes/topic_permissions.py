"""
API routes for topic permission management
"""
from fastapi import APIRouter, Depends, HTTPException, status
from typing import List, Dict, Any, Optional

import logging
from pydantic import BaseModel, Field

from backend.models.user import User, Role
from backend.models.topic_permission import TopicPermission, PermissionType
from backend.auth.auth_handler import get_current_active_user, get_admin_user
from backend.auth.topic_permission_service import (
    has_permission,
    match_topic_pattern,
    clear_permission_cache
)

# Configure logger
logger = logging.getLogger(__name__)

# Create router
router = APIRouter(prefix="/topic-permissions", tags=["Topic Permissions"])

# Models
class TopicPermissionCreate(BaseModel):
    """Topic permission creation model"""
    topic_pattern: str = Field(..., description="Topic pattern (can include MQTT wildcards + and #)")
    permission_type: str = Field(..., description="Permission type: read, write, or readwrite")
    username: Optional[str] = Field(None, description="Username for user-specific permission")
    role: Optional[str] = Field(None, description="Role for role-based permission")
    description: Optional[str] = Field("", description="Description of the permission")

class TopicPermissionUpdate(BaseModel):
    """Topic permission update model"""
    topic_pattern: Optional[str] = Field(None, description="Topic pattern (can include MQTT wildcards + and #)")
    permission_type: Optional[str] = Field(None, description="Permission type: read, write, or readwrite")
    username: Optional[str] = Field(None, description="Username for user-specific permission")
    role: Optional[str] = Field(None, description="Role for role-based permission")
    description: Optional[str] = Field(None, description="Description of the permission")

class TopicPermissionResponse(BaseModel):
    """Topic permission response model"""
    id: int
    topic_pattern: str
    permission_type: str
    username: Optional[str]
    role: Optional[str]
    description: str
    created_by: str
    created_at: float

class PermissionCheckRequest(BaseModel):
    """Permission check request model"""
    topic: str
    permission_type: str

class PermissionCheckResponse(BaseModel):
    """Permission check response model"""
    topic: str
    permission_type: str
    has_permission: bool

# Routes
@router.get("", response_model=List[TopicPermissionResponse])
async def get_all_permissions(
    admin_user: User = Depends(get_admin_user)
):
    """Get all topic permissions (admin only)"""
    try:
        permissions = TopicPermission.get_all()
        return [
            TopicPermissionResponse(
                id=p.id,
                topic_pattern=p.topic_pattern,
                permission_type=p.permission_type.value if isinstance(p.permission_type, PermissionType) else p.permission_type,
                username=p.username,
                role=p.role,
                description=p.description,
                created_by=p.created_by,
                created_at=p.created_at
            )
            for p in permissions
        ]
    except Exception as e:
        logger.error(f"Error getting permissions: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error getting permissions: {str(e)}"
        )

@router.post("", response_model=TopicPermissionResponse, status_code=status.HTTP_201_CREATED)
async def create_permission(
    permission: TopicPermissionCreate,
    admin_user: User = Depends(get_admin_user)
):
    """Create a new topic permission (admin only)"""
    try:
        # Validate permission type
        try:
            permission_type = PermissionType(permission.permission_type)
        except ValueError:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Invalid permission type: {permission.permission_type}"
            )
        
        # Validate MQTT pattern syntax
        try:
            if '#' in permission.topic_pattern and not permission.topic_pattern.endswith('#'):
                raise ValueError("# wildcard must be at the end of the pattern")
            
            # Check if pattern is valid by trying to match it
            match_topic_pattern(permission.topic_pattern, "test/topic")
        except Exception as e:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Invalid topic pattern: {str(e)}"
            )
        
        # Validate that either username or role is provided
        if not permission.username and not permission.role:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Either username or role must be provided"
            )
        
        # Create permission
        new_permission = TopicPermission(
            topic_pattern=permission.topic_pattern,
            permission_type=permission_type,
            username=permission.username,
            role=permission.role,
            description=permission.description,
            created_by=admin_user.username
        )
        
        # Save to database
        new_permission.save()
        
        # Clear permission cache
        clear_permission_cache()
        
        return TopicPermissionResponse(
            id=new_permission.id,
            topic_pattern=new_permission.topic_pattern,
            permission_type=new_permission.permission_type.value,
            username=new_permission.username,
            role=new_permission.role,
            description=new_permission.description,
            created_by=new_permission.created_by,
            created_at=new_permission.created_at
        )
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error creating permission: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error creating permission: {str(e)}"
        )

@router.get("/{permission_id}", response_model=TopicPermissionResponse)
async def get_permission(
    permission_id: int,
    admin_user: User = Depends(get_admin_user)
):
    """Get a specific topic permission by ID (admin only)"""
    try:
        permission = TopicPermission.get(permission_id)
        if not permission:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Permission not found"
            )
        
        return TopicPermissionResponse(
            id=permission.id,
            topic_pattern=permission.topic_pattern,
            permission_type=permission.permission_type.value if isinstance(permission.permission_type, PermissionType) else permission.permission_type,
            username=permission.username,
            role=permission.role,
            description=permission.description,
            created_by=permission.created_by,
            created_at=permission.created_at
        )
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting permission: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error getting permission: {str(e)}"
        )

@router.put("/{permission_id}", response_model=TopicPermissionResponse)
async def update_permission(
    permission_id: int,
    permission_update: TopicPermissionUpdate,
    admin_user: User = Depends(get_admin_user)
):
    """Update a topic permission (admin only)"""
    try:
        # Get existing permission
        existing = TopicPermission.get(permission_id)
        if not existing:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Permission not found"
            )
        
        # Update fields
        if permission_update.topic_pattern is not None:
            # Validate MQTT pattern syntax
            try:
                if '#' in permission_update.topic_pattern and not permission_update.topic_pattern.endswith('#'):
                    raise ValueError("# wildcard must be at the end of the pattern")
                
                # Check if pattern is valid by trying to match it
                match_topic_pattern(permission_update.topic_pattern, "test/topic")
            except Exception as e:
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail=f"Invalid topic pattern: {str(e)}"
                )
            
            existing.topic_pattern = permission_update.topic_pattern
        
        if permission_update.permission_type is not None:
            # Validate permission type
            try:
                existing.permission_type = PermissionType(permission_update.permission_type)
            except ValueError:
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail=f"Invalid permission type: {permission_update.permission_type}"
                )
        
        if permission_update.username is not None:
            existing.username = permission_update.username
        
        if permission_update.role is not None:
            existing.role = permission_update.role
        
        if permission_update.description is not None:
            existing.description = permission_update.description
        
        # Validate that either username or role is provided
        if not existing.username and not existing.role:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Either username or role must be provided"
            )
        
        # Save changes
        existing.save()
        
        # Clear permission cache
        clear_permission_cache()
        
        return TopicPermissionResponse(
            id=existing.id,
            topic_pattern=existing.topic_pattern,
            permission_type=existing.permission_type.value if isinstance(existing.permission_type, PermissionType) else existing.permission_type,
            username=existing.username,
            role=existing.role,
            description=existing.description,
            created_by=existing.created_by,
            created_at=existing.created_at
        )
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error updating permission: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error updating permission: {str(e)}"
        )

@router.delete("/{permission_id}")
async def delete_permission(
    permission_id: int,
    admin_user: User = Depends(get_admin_user)
):
    """Delete a topic permission (admin only)"""
    try:
        result = TopicPermission.delete(permission_id)
        if not result:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Permission not found"
            )
        
        # Clear permission cache
        clear_permission_cache()
        
        return {"message": "Permission deleted successfully"}
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error deleting permission: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error deleting permission: {str(e)}"
        )

@router.post("/check", response_model=PermissionCheckResponse)
async def check_permission(
    request: PermissionCheckRequest,
    current_user: User = Depends(get_current_active_user)
):
    """Check if current user has permission for a topic"""
    try:
        # Validate permission type
        try:
            permission_type = PermissionType(request.permission_type)
        except ValueError:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Invalid permission type: {request.permission_type}"
            )
        
        # Check permission
        result = has_permission(current_user, request.topic, permission_type)
        
        return PermissionCheckResponse(
            topic=request.topic,
            permission_type=request.permission_type,
            has_permission=result
        )
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error checking permission: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error checking permission: {str(e)}"
        )