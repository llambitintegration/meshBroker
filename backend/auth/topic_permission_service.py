"""
Topic permission service for authorization
"""
import re
import time
import logging
from typing import Dict, Optional, List, Set, Pattern, Tuple, Any, Union, Callable
from functools import lru_cache
from backend.models.user import User, Role
from backend.models.topic_permission import TopicPermission, PermissionType

# Configure logger
logger = logging.getLogger(__name__)

# Cache for permission checks (topic -> user -> permission_type -> result)
# This reduces database lookups for frequent permission checks
permission_cache: Dict[str, Dict[str, Dict[str, Tuple[bool, float]]]] = {}
CACHE_TTL = 300  # 5 minutes

# Cache for regex patterns to avoid recompiling them
pattern_cache: Dict[str, Pattern] = {}

def clear_permission_cache():
    """Clear the permission cache"""
    global permission_cache
    permission_cache = {}
    logger.debug("Permission cache cleared")

def _mqtt_pattern_to_regex(pattern: str) -> Pattern:
    """Convert MQTT pattern with wildcards to regex pattern
    
    Args:
        pattern: MQTT topic pattern
        
    Returns:
        Compiled regex pattern
    """
    # Check if pattern is already in cache
    if pattern in pattern_cache:
        return pattern_cache[pattern]
    
    # Convert MQTT wildcards to regex
    # + matches a single level (e.g., foo/+/bar matches foo/anything/bar)
    # # matches multiple levels (e.g., foo/# matches foo, foo/bar, foo/bar/baz)
    
    # Escape special regex characters except + and #
    escaped = re.escape(pattern).replace('\\+', '+').replace('\\#', '#')
    
    # Replace MQTT wildcards with regex equivalents
    # + matches a single level (no slashes)
    regex_str = escaped.replace('+', '[^/]+')
    
    # # must be at the end and matches any number of levels
    if '#' in regex_str:
        if not regex_str.endswith('#'):
            raise ValueError("# wildcard must be at the end of the pattern")
        regex_str = regex_str[:-1] + '.*'
    
    # Compile and cache the pattern
    compiled = re.compile(f'^{regex_str}$')
    pattern_cache[pattern] = compiled
    
    return compiled

def match_topic_pattern(pattern: str, topic: str) -> bool:
    """Check if a topic matches an MQTT pattern
    
    Args:
        pattern: MQTT topic pattern (may include wildcards)
        topic: Actual topic to match
        
    Returns:
        True if the topic matches the pattern
    """
    try:
        regex = _mqtt_pattern_to_regex(pattern)
        return bool(regex.match(topic))
    except Exception as e:
        logger.error(f"Error matching topic pattern: {e}")
        return False

def get_applicable_permissions(
    user: Optional[User], 
    topic: str, 
    permission_type: PermissionType
) -> List[TopicPermission]:
    """Get all permissions that apply to a user and topic
    
    Args:
        user: User object or None for anonymous
        topic: Topic to check
        permission_type: Permission type (READ, WRITE, READWRITE)
        
    Returns:
        List of applicable permissions
    """
    # Collect all potential permissions
    permissions: List[TopicPermission] = []
    
    # Get user-specific permissions
    if user and user.username:
        user_permissions = TopicPermission.get_by_username(user.username)
        permissions.extend(user_permissions)
    
    # Get role-based permissions
    if user and user.role:
        role_permissions = TopicPermission.get_by_role(user.role)
        permissions.extend(role_permissions)
    
    # Filter permissions by type
    type_filtered = []
    for p in permissions:
        # For READWRITE permission type, it includes both READ and WRITE
        if p.permission_type == PermissionType.READWRITE:
            type_filtered.append(p)
        # For READ permission type, it matches if requested type is READ
        elif p.permission_type == PermissionType.READ and permission_type == PermissionType.READ:
            type_filtered.append(p)
        # For WRITE permission type, it matches if requested type is WRITE
        elif p.permission_type == PermissionType.WRITE and permission_type == PermissionType.WRITE:
            type_filtered.append(p)
    
    # Filter permissions by topic pattern
    topic_filtered = []
    for p in type_filtered:
        if match_topic_pattern(p.topic_pattern, topic):
            topic_filtered.append(p)
    
    # Sort by specificity (more specific patterns come first)
    # Specificity is determined by the number of wildcards and their position
    def get_specificity(pattern: str) -> int:
        # Count wildcards (+ and #)
        wildcards = pattern.count('+') + pattern.count('#') * 2
        # Count levels (more levels is more specific)
        levels = pattern.count('/') + 1
        # Return a score (lower is more specific)
        return wildcards - levels * 2
    
    topic_filtered.sort(key=lambda p: get_specificity(p.topic_pattern))
    
    return topic_filtered

def has_permission(
    user: Optional[User], 
    topic: str, 
    permission_type: Union[PermissionType, str]
) -> bool:
    """Check if a user has permission for a topic
    
    Args:
        user: User object or None for anonymous
        topic: Topic to check
        permission_type: Permission type (READ, WRITE, READWRITE)
        
    Returns:
        True if the user has permission
    """
    # Convert string to enum if needed
    if isinstance(permission_type, str):
        permission_type = PermissionType(permission_type)
    
    # Admin role has all permissions
    if user and user.role == Role.ADMIN:
        return True
    
    # Check cache first
    username = user.username if user else 'anonymous'
    cache_key = f"{username}:{topic}:{permission_type.value}"
    
    # Check cache
    topic_cache = permission_cache.get(topic, {})
    user_cache = topic_cache.get(username, {})
    permission_cache_entry = user_cache.get(permission_type.value)
    
    if permission_cache_entry:
        result, timestamp = permission_cache_entry
        if timestamp + CACHE_TTL > time.time():
            logger.debug(f"Permission cache hit for {cache_key}")
            return result
    
    # Get applicable permissions
    permissions = get_applicable_permissions(user, topic, permission_type)
    
    # If no permissions found, check default permission
    if not permissions:
        # Default to deny access if no permissions found
        result = False
    else:
        # Permissions are already sorted by specificity
        # Use the most specific permission
        result = True  # Assume allowed if any permission matches
    
    # Cache the result
    if topic not in permission_cache:
        permission_cache[topic] = {}
    if username not in permission_cache[topic]:
        permission_cache[topic][username] = {}
    
    permission_cache[topic][username][permission_type.value] = (result, time.time())
    
    return result