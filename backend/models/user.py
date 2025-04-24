"""
User models and database interactions
"""
from typing import Optional, List, Dict
from pydantic import BaseModel, EmailStr
from enum import Enum
import logging

# Get logger
logger = logging.getLogger(__name__)

# Role enum for user roles
class Role(str, Enum):
    """User role enum"""
    USER = "user"
    ADMIN = "admin"
    READONLY = "readonly"

# Mock database - replace with actual database in production
fake_users_db = {
    "johndoe": {
        "username": "johndoe",
        "full_name": "John Doe",
        "email": "johndoe@example.com",
        "hashed_password": "$2b$12$EixZaYVK1fsbw1ZfbX3OXePaWxn96p36WQoeG6Lruj3vjPGga31lW",
        "disabled": False,
        "role": Role.USER,
    }
}

class UserBase(BaseModel):
    """Base user model with common attributes"""
    username: str
    email: Optional[EmailStr] = None
    full_name: Optional[str] = None
    disabled: Optional[bool] = None
    role: Optional[Role] = Role.USER

class UserCreate(UserBase):
    """User creation model with password"""
    password: str

class User(UserBase):
    """User model for database"""
    hashed_password: str
    
    class Config:
        orm_mode = True
        
    @classmethod
    def init_db(cls):
        """Initialize the user database with default admin user if not exists"""
        from .auth import get_password_hash
        
        # Check if admin user exists
        if "admin" not in fake_users_db:
            # Create default admin user
            admin_user = {
                "username": "admin",
                "full_name": "Administrator",
                "email": "admin@example.com",
                "hashed_password": get_password_hash("admin"),  # Default password, should be changed
                "disabled": False,
                "role": Role.ADMIN
            }
            
            fake_users_db["admin"] = admin_user
            logger.info("Created default admin user")
        
        logger.info(f"User database initialized with {len(fake_users_db)} users")

class UserInDB(User):
    """User model with additional database specific fields"""
    pass

def get_user_by_username(username: str):
    """Get a user by username from the database"""
    if username in fake_users_db:
        user_dict = fake_users_db[username]
        return UserInDB(**user_dict)
    return None

def create_user(user: UserCreate) -> User:
    """Create a new user in the database"""
    # In a real application, you would hash the password here
    # and store the user in your database
    from .auth import get_password_hash
    
    user_dict = user.dict()
    hashed_password = get_password_hash(user.password)
    
    # Remove plain password and add hashed password
    user_dict.pop("password")
    user_dict["hashed_password"] = hashed_password
    
    # Store in mock database (replace with real DB operation)
    fake_users_db[user.username] = user_dict
    
    return User(**user_dict)

def get_users() -> List[User]:
    """Get all users from the database"""
    return [User(**user_data) for user_data in fake_users_db.values()]

def update_user(username: str, user_data: Dict) -> Optional[User]:
    """Update a user in the database"""
    if username not in fake_users_db:
        return None
        
    # Update user data
    current_user = fake_users_db[username]
    for key, value in user_data.items():
        if key != "username":  # Don't allow changing the username
            current_user[key] = value
            
    return User(**current_user)

def delete_user(username: str) -> bool:
    """Delete a user from the database"""
    if username in fake_users_db:
        del fake_users_db[username]
        return True
    return False