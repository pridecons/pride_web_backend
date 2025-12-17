# db/connection.py - FIXED for SQLAlchemy 2.0

from sqlalchemy import create_engine, event, text
from sqlalchemy.orm import sessionmaker, declarative_base
from sqlalchemy.engine import Engine
from urllib.parse import quote_plus
from dotenv import load_dotenv
import os
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()

# Database configuration with defaults
DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_NAME = os.getenv("DB_NAME", "crm_db")
DB_USERNAME = os.getenv("DB_USERNAME", "postgres")
DB_PASSWORD = os.getenv("DB_PASSWORD", "password")

# Ensure password is string and quote it properly
if isinstance(DB_PASSWORD, (bytes, bytearray)):
    DB_PASSWORD = DB_PASSWORD.decode("utf-8")

# Quote password for URL safety
pw_quoted = quote_plus(str(DB_PASSWORD))

# Build DATABASE_URL with proper error handling
try:
    DATABASE_URL = (
        f"postgresql://{DB_USERNAME}:{pw_quoted}"
        f"@{DB_HOST}:{DB_PORT}/{DB_NAME}"
    )
    logger.info(f"Database URL constructed for: {DB_USERNAME}@{DB_HOST}:{DB_PORT}/{DB_NAME}")
except Exception as e:
    logger.error(f"Error constructing database URL: {e}")
    raise

# Create engine with proper configuration
engine = create_engine(
    DATABASE_URL,
    echo=False,  # Set to True for SQL debugging
    pool_size=10,
    max_overflow=20,
    pool_pre_ping=True,  # Validate connections before use
    pool_recycle=3600,   # Recycle connections every hour
    connect_args={
        "application_name": "CRM_Backend",
        "connect_timeout": 10,
    }
)

# Add connection event listeners for better error handling
@event.listens_for(Engine, "connect")
def set_connection_settings(dbapi_connection, connection_record):
    """Configure connection settings"""
    try:
        # Set timezone to UTC
        with dbapi_connection.cursor() as cursor:
            cursor.execute("SET timezone TO 'UTC'")
            dbapi_connection.commit()
    except Exception as e:
        logger.warning(f"Could not set timezone: {e}")

@event.listens_for(Engine, "checkout")
def receive_checkout(dbapi_connection, connection_record, connection_proxy):
    """Log when connection is checked out"""
    logger.debug("Connection checked out from pool")

# Session factory
SessionLocal = sessionmaker(
    autocommit=False, 
    autoflush=False, 
    bind=engine
)

# Base class for models
Base = declarative_base()

# Improved dependency for FastAPI routes
def get_db():
    """
    Database dependency with proper error handling
    """
    db = None
    try:
        db = SessionLocal()
        yield db
    except Exception as e:
        logger.error(f"Database session error: {e}")
        if db:
            db.rollback()
        raise
    finally:
        if db:
            db.close()

# Health check function - FIXED for SQLAlchemy 2.0
def check_database_connection():
    """
    Check if database connection is working
    """
    try:
        db = SessionLocal()
        # Use text() for raw SQL in SQLAlchemy 2.0
        result = db.execute(text("SELECT 1 as test"))
        test_value = result.fetchone()
        db.close()
        
        if test_value and test_value[0] == 1:
            logger.info("✅ Database connection successful")
            return True
        else:
            logger.error("❌ Database query returned unexpected result")
            return False
            
    except Exception as e:
        logger.error(f"Database health check failed: {e}")
        return False

# Alternative health check using engine directly
def check_database_connection_engine():
    """
    Alternative database health check using engine directly
    """
    try:
        with engine.connect() as conn:
            result = conn.execute(text("SELECT 1 as test"))
            test_value = result.fetchone()
            
        if test_value and test_value[0] == 1:
            logger.info("✅ Database engine connection successful")
            return True
        else:
            logger.error("❌ Database engine query returned unexpected result")
            return False
            
    except Exception as e:
        logger.error(f"Database engine health check failed: {e}")
        return False

# Test database with detailed error info
def test_database_connection():
    """
    Detailed database connection test
    """
    logger.info("🔍 Testing database connection...")
    
    try:
        # Test 1: Basic connection
        logger.info("Test 1: Basic engine connection...")
        with engine.connect() as conn:
            logger.info("✅ Engine connection successful")
        
        # Test 2: Simple query
        logger.info("Test 2: Simple query test...")
        with engine.connect() as conn:
            result = conn.execute(text("SELECT version()"))
            version = result.fetchone()
            logger.info(f"✅ Database version: {version[0][:50]}...")
        
        # Test 3: Session test
        logger.info("Test 3: Session test...")
        db = SessionLocal()
        result = db.execute(text("SELECT current_database(), current_user"))
        db_info = result.fetchone()
        logger.info(f"✅ Connected to database: {db_info[0]} as user: {db_info[1]}")
        db.close()
        
        logger.info("🎉 All database tests passed!")
        return True
        
    except Exception as e:
        logger.error(f"❌ Database test failed: {e}")
        logger.error(f"Database URL: postgresql://{DB_USERNAME}:***@{DB_HOST}:{DB_PORT}/{DB_NAME}")
        return False
    
    