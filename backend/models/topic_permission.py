"""
Topic permission model
"""
import time
from enum import Enum
from typing import Optional, List, Dict, Any

# In-memory database for development. In production, replace with a real database.
permission_db = {}
id_counter = 1

class PermissionType(str, Enum):
    """Topic permission types"""
    READ = "read"
    WRITE = "write"
    READWRITE = "readwrite"

class TopicPermission:
    """Topic permission model"""
    def __init__(
        self,
        topic_pattern: str,
        permission_type: PermissionType,
        username: Optional[str] = None,
        role: Optional[str] = None,
        description: str = "",
        created_by: str = "system",
        id: Optional[int] = None,
        created_at: Optional[float] = None
    ):
        """Initialize a topic permission"""
        self.id = id
        self.topic_pattern = topic_pattern
        self.permission_type = permission_type
        self.username = username
        self.role = role
        self.description = description
        self.created_by = created_by
        self.created_at = created_at or time.time()
    
    def save(self) -> "TopicPermission":
        """Save permission to database"""
        global id_counter
        if not self.id:
            self.id = id_counter
            id_counter += 1
        
        permission_db[self.id] = self
        return self
    
    @classmethod
    def get(cls, permission_id: int) -> Optional["TopicPermission"]:
        """Get permission by ID"""
        return permission_db.get(permission_id)
    
    @classmethod
    def get_all(cls) -> List["TopicPermission"]:
        """Get all permissions"""
        return list(permission_db.values())
    
    @classmethod
    def delete(cls, permission_id: int) -> bool:
        """Delete permission by ID"""
        if permission_id in permission_db:
            del permission_db[permission_id]
            return True
        return False
    
    @classmethod
    def find_by_username(cls, username: str) -> List["TopicPermission"]:
        """Find permissions by username"""
        return [p for p in permission_db.values() if p.username == username]
    
    @classmethod
    def find_by_role(cls, role: str) -> List["TopicPermission"]:
        """Find permissions by role"""
        return [p for p in permission_db.values() if p.role == role]