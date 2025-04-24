"""
Database model for user authentication
"""
import time
import json
import logging
import sqlite3
from enum import Enum
from typing import Optional, Dict, List, Any, ClassVar
from dataclasses import dataclass, field, asdict

# Configure logger
logger = logging.getLogger(__name__)

class Role(str, Enum):
    """User role types"""
    ADMIN = "admin"
    USER = "user"
    READONLY = "readonly"
    API = "api"

@dataclass
class User:
    """User model for authentication"""
    # Core fields
    username: str
    email: str
    hashed_password: str
    
    # User information
    first_name: str = ""
    last_name: str = ""
    
    # Access control
    role: Role = Role.USER
    is_active: bool = True
    is_verified: bool = False
    
    # API access
    api_key: Optional[str] = None
    
    # Timestamps
    created_at: float = field(default_factory=time.time)  # Unix timestamp
    last_login: Optional[float] = None
    
    # Rate limiting
    rate_limit: int = 100  # Default requests per minute
    
    # Database configuration
    _db_path: ClassVar[str] = "mqtt_data/users.db"
    
    @classmethod
    def set_db_path(cls, path: str):
        """Set the database path"""
        cls._db_path = path
        
    @classmethod
    def init_db(cls):
        """Initialize the database for storing users"""
        try:
            conn = sqlite3.connect(cls._db_path)
            cursor = conn.cursor()
            
            # Create users table
            cursor.execute('''
            CREATE TABLE IF NOT EXISTS users (
                username TEXT PRIMARY KEY,
                email TEXT NOT NULL UNIQUE,
                hashed_password TEXT NOT NULL,
                first_name TEXT NOT NULL,
                last_name TEXT NOT NULL,
                role TEXT NOT NULL,
                is_active INTEGER NOT NULL,
                is_verified INTEGER NOT NULL,
                api_key TEXT UNIQUE,
                created_at REAL NOT NULL,
                last_login REAL,
                rate_limit INTEGER NOT NULL
            )
            ''')
            
            # Create a default admin user if no users exist
            cursor.execute("SELECT COUNT(*) FROM users")
            user_count = cursor.fetchone()[0]
            
            if user_count == 0:
                # Note: The actual password will be set during the first run
                # This is just a placeholder that will be updated
                cursor.execute(
                    """
                    INSERT INTO users
                    (username, email, hashed_password, first_name, last_name, 
                    role, is_active, is_verified, created_at, rate_limit)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        "admin",
                        "admin@example.com",
                        "CHANGE_THIS_PASSWORD",
                        "Admin",
                        "User",
                        Role.ADMIN.value,
                        1,
                        1,
                        time.time(),
                        200
                    )
                )
                logger.info("Created default admin user")
            
            conn.commit()
            conn.close()
            logger.info("Users database initialized")
            return True
        except Exception as e:
            logger.error(f"Error initializing users database: {e}")
            return False
    
    def save(self) -> bool:
        """Save the user to the database"""
        try:
            conn = sqlite3.connect(self._db_path)
            cursor = conn.cursor()
            
            cursor.execute(
                """
                INSERT OR REPLACE INTO users
                (username, email, hashed_password, first_name, last_name, 
                role, is_active, is_verified, api_key, created_at, last_login, rate_limit)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    self.username,
                    self.email,
                    self.hashed_password,
                    self.first_name,
                    self.last_name,
                    self.role.value if isinstance(self.role, Role) else self.role,
                    1 if self.is_active else 0,
                    1 if self.is_verified else 0,
                    self.api_key,
                    self.created_at,
                    self.last_login,
                    self.rate_limit
                )
            )
            
            conn.commit()
            conn.close()
            logger.debug(f"Saved user {self.username} to database")
            return True
        except Exception as e:
            logger.error(f"Error saving user to database: {e}")
            return False
    
    @classmethod
    def get(cls, username: str) -> Optional['User']:
        """Get a user from the database by username"""
        try:
            conn = sqlite3.connect(cls._db_path)
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()
            
            cursor.execute("SELECT * FROM users WHERE username = ?", (username,))
            row = cursor.fetchone()
            
            if not row:
                conn.close()
                return None
            
            # Create the user
            user = cls(
                username=row['username'],
                email=row['email'],
                hashed_password=row['hashed_password'],
                first_name=row['first_name'],
                last_name=row['last_name'],
                role=row['role'],
                is_active=bool(row['is_active']),
                is_verified=bool(row['is_verified']),
                api_key=row['api_key'],
                created_at=row['created_at'],
                last_login=row['last_login'],
                rate_limit=row['rate_limit']
            )
            
            conn.close()
            return user
        except Exception as e:
            logger.error(f"Error getting user from database: {e}")
            return None
    
    @classmethod
    def get_by_email(cls, email: str) -> Optional['User']:
        """Get a user from the database by email"""
        try:
            conn = sqlite3.connect(cls._db_path)
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()
            
            cursor.execute("SELECT * FROM users WHERE email = ?", (email,))
            row = cursor.fetchone()
            
            if not row:
                conn.close()
                return None
            
            # Create the user
            user = cls(
                username=row['username'],
                email=row['email'],
                hashed_password=row['hashed_password'],
                first_name=row['first_name'],
                last_name=row['last_name'],
                role=row['role'],
                is_active=bool(row['is_active']),
                is_verified=bool(row['is_verified']),
                api_key=row['api_key'],
                created_at=row['created_at'],
                last_login=row['last_login'],
                rate_limit=row['rate_limit']
            )
            
            conn.close()
            return user
        except Exception as e:
            logger.error(f"Error getting user by email from database: {e}")
            return None
    
    @classmethod
    def get_by_api_key(cls, api_key: str) -> Optional['User']:
        """Get a user from the database by API key"""
        try:
            conn = sqlite3.connect(cls._db_path)
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()
            
            cursor.execute("SELECT * FROM users WHERE api_key = ?", (api_key,))
            row = cursor.fetchone()
            
            if not row:
                conn.close()
                return None
            
            # Create the user
            user = cls(
                username=row['username'],
                email=row['email'],
                hashed_password=row['hashed_password'],
                first_name=row['first_name'],
                last_name=row['last_name'],
                role=row['role'],
                is_active=bool(row['is_active']),
                is_verified=bool(row['is_verified']),
                api_key=row['api_key'],
                created_at=row['created_at'],
                last_login=row['last_login'],
                rate_limit=row['rate_limit']
            )
            
            conn.close()
            return user
        except Exception as e:
            logger.error(f"Error getting user by API key from database: {e}")
            return None
    
    @classmethod
    def get_all(cls) -> List['User']:
        """Get all users from the database"""
        try:
            conn = sqlite3.connect(cls._db_path)
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()
            
            cursor.execute("SELECT * FROM users ORDER BY username")
            rows = cursor.fetchall()
            
            users = []
            for row in rows:
                user = cls(
                    username=row['username'],
                    email=row['email'],
                    hashed_password=row['hashed_password'],
                    first_name=row['first_name'],
                    last_name=row['last_name'],
                    role=row['role'],
                    is_active=bool(row['is_active']),
                    is_verified=bool(row['is_verified']),
                    api_key=row['api_key'],
                    created_at=row['created_at'],
                    last_login=row['last_login'],
                    rate_limit=row['rate_limit']
                )
                users.append(user)
            
            conn.close()
            return users
        except Exception as e:
            logger.error(f"Error getting all users from database: {e}")
            return []
    
    @classmethod
    def delete(cls, username: str) -> bool:
        """Delete a user from the database"""
        try:
            conn = sqlite3.connect(cls._db_path)
            cursor = conn.cursor()
            
            cursor.execute("DELETE FROM users WHERE username = ?", (username,))
            
            conn.commit()
            conn.close()
            logger.debug(f"Deleted user {username} from database")
            return True
        except Exception as e:
            logger.error(f"Error deleting user from database: {e}")
            return False
    
    def update_last_login(self) -> bool:
        """Update the last login timestamp"""
        self.last_login = time.time()
        return self.save() 