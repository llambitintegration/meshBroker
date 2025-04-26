from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, scoped_session
import os
from dotenv import load_dotenv
import logging

# Configure logger
logger = logging.getLogger(__name__)

# Load environment variables from .env file
load_dotenv()

# Get database URL from environment variables or use a default SQLite database
DATABASE_URL = os.getenv("DATABASE_URL", "sqlite:///./meshbroker.db")

# Database connection settings
if DATABASE_URL.startswith("sqlite"):
    # SQLite specific settings
    connect_args = {"check_same_thread": False, "timeout": 30}
    
    # Create SQLAlchemy engine with a larger pool size and set timeout on busy
    engine = create_engine(
        DATABASE_URL, 
        connect_args=connect_args,
        pool_size=10,  # Support more concurrent connections
        pool_recycle=3600,  # Recycle connections hourly
        pool_pre_ping=True  # Verify connections before using them
    )
else:
    # PostgreSQL or other database settings
    engine = create_engine(
        DATABASE_URL,
        pool_size=20,  # Larger pool for PostgreSQL
        max_overflow=10,  # Allow additional connections when pool is full
        pool_recycle=3600,  # Recycle connections hourly
        pool_pre_ping=True  # Verify connections before using them
    )

# Create sessionmaker
session_factory = sessionmaker(autocommit=False, autoflush=False, bind=engine)

# Create a scoped session to ensure thread safety
SessionLocal = scoped_session(session_factory)

# Create base class for declarative models
Base = declarative_base()

# Dependency to get DB session
def get_db():
    db = SessionLocal()
    try:
        yield db
    except Exception as e:
        logger.error(f"Database session error: {e}")
        db.rollback()  # Rollback any failed transactions
    finally:
        db.close()  # Ensure connection is returned to pool