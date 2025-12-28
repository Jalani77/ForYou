"""
Snowflake Connection Manager
Handles enterprise-grade connection pooling and session management with comprehensive error handling.
"""
import os
import logging
from typing import Optional, Dict, Any
import streamlit as st

try:
    import snowflake.connector
    from snowflake.snowpark import Session
    SNOWFLAKE_AVAILABLE = True
except ImportError:
    SNOWFLAKE_AVAILABLE = False

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class SnowflakeConnectionManager:
    """
    Enterprise-grade Snowflake connection manager with error handling and connection pooling.
    Implements singleton pattern for efficient resource management.
    """
    
    _instance: Optional['SnowflakeConnectionManager'] = None
    
    def __new__(cls):
        """Singleton pattern implementation."""
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance
    
    def __init__(self):
        """Initialize connection manager."""
        if not hasattr(self, 'initialized'):
            self.conn = None
            self.session = None
            self.demo_mode = False
            self.connection_params = {}
            self.initialized = True
            logger.info("SnowflakeConnectionManager initialized")
    
    def configure_connection(self) -> Dict[str, str]:
        """
        Configure connection parameters from environment variables.
        
        Returns:
            Dict containing connection parameters
        
        Raises:
            EnvironmentError: If required credentials are missing
        """
        self.connection_params = {
            'account': os.getenv('SNOWFLAKE_ACCOUNT'),
            'user': os.getenv('SNOWFLAKE_USER'),
            'password': os.getenv('SNOWFLAKE_PASSWORD'),
            'warehouse': os.getenv('SNOWFLAKE_WAREHOUSE', 'COMPUTE_WH'),
            'database': os.getenv('SNOWFLAKE_DATABASE', 'SAAS_ANALYTICS'),
            'schema': os.getenv('SNOWFLAKE_SCHEMA', 'PUBLIC'),
            'role': os.getenv('SNOWFLAKE_ROLE', 'SYSADMIN')
        }
        
        # Validate required parameters
        required_params = ['account', 'user', 'password']
        missing_params = [p for p in required_params if not self.connection_params.get(p)]
        
        if missing_params:
            error_msg = f"Missing required Snowflake credentials: {', '.join(missing_params)}"
            logger.warning(error_msg)
            raise EnvironmentError(error_msg)
        
        return self.connection_params
    
    def establish_connection(self) -> bool:
        """
        Establish connection to Snowflake with comprehensive error handling.
        
        Returns:
            bool: True if connection successful, False otherwise
        """
        if not SNOWFLAKE_AVAILABLE:
            logger.warning("Snowflake connector not available. Switching to demo mode.")
            self.demo_mode = True
            return False
        
        try:
            # Configure connection parameters
            try:
                params = self.configure_connection()
            except EnvironmentError as e:
                logger.warning(f"Configuration error: {e}. Switching to demo mode.")
                self.demo_mode = True
                return False
            
            # Establish connection
            logger.info(f"Connecting to Snowflake account: {params['account']}")
            self.conn = snowflake.connector.connect(
                user=params['user'],
                password=params['password'],
                account=params['account'],
                warehouse=params['warehouse'],
                database=params['database'],
                schema=params['schema'],
                role=params['role'],
                client_session_keep_alive=True  # Keep connection alive
            )
            
            # Test connection
            cursor = self.conn.cursor()
            cursor.execute("SELECT CURRENT_VERSION()")
            version = cursor.fetchone()[0]
            logger.info(f"Successfully connected to Snowflake version: {version}")
            cursor.close()
            
            return True
            
        except snowflake.connector.errors.DatabaseError as e:
            logger.error(f"Database connection error: {e}")
            st.error(f"❌ Failed to connect to Snowflake: {str(e)}")
            self.demo_mode = True
            return False
            
        except snowflake.connector.errors.ProgrammingError as e:
            logger.error(f"Programming error: {e}")
            st.error(f"❌ Snowflake configuration error: {str(e)}")
            self.demo_mode = True
            return False
            
        except Exception as e:
            logger.error(f"Unexpected connection error: {e}")
            st.error(f"❌ Unexpected error connecting to Snowflake: {str(e)}")
            self.demo_mode = True
            return False
    
    def get_snowpark_session(self) -> Optional[Session]:
        """
        Create and return a Snowpark session for advanced operations.
        
        Returns:
            Optional[Session]: Snowpark session if available, None otherwise
        """
        if self.demo_mode or not SNOWFLAKE_AVAILABLE:
            return None
        
        try:
            if self.session is None:
                params = self.connection_params
                connection_params = {
                    "account": params['account'],
                    "user": params['user'],
                    "password": params['password'],
                    "warehouse": params['warehouse'],
                    "database": params['database'],
                    "schema": params['schema'],
                    "role": params['role']
                }
                self.session = Session.builder.configs(connection_params).create()
                logger.info("Snowpark session created successfully")
            
            return self.session
            
        except Exception as e:
            logger.error(f"Error creating Snowpark session: {e}")
            st.error(f"❌ Failed to create Snowpark session: {str(e)}")
            return None
    
    def test_connection(self) -> bool:
        """
        Test the current connection status.
        
        Returns:
            bool: True if connection is active, False otherwise
        """
        if self.demo_mode or not self.conn:
            return False
        
        try:
            cursor = self.conn.cursor()
            cursor.execute("SELECT 1")
            cursor.fetchone()
            cursor.close()
            return True
        except Exception as e:
            logger.warning(f"Connection test failed: {e}")
            return False
    
    def close_connection(self):
        """Close all connections and clean up resources."""
        try:
            if self.session:
                self.session.close()
                self.session = None
                logger.info("Snowpark session closed")
            
            if self.conn:
                self.conn.close()
                self.conn = None
                logger.info("Snowflake connection closed")
                
        except Exception as e:
            logger.error(f"Error closing connections: {e}")
    
    def is_demo_mode(self) -> bool:
        """Check if running in demo mode."""
        return self.demo_mode
    
    def get_connection(self):
        """Get the active Snowflake connection."""
        return self.conn


@st.cache_resource
def get_connection_manager() -> SnowflakeConnectionManager:
    """
    Get or create a cached connection manager instance.
    Uses Streamlit's caching to maintain connection across reruns.
    
    Returns:
        SnowflakeConnectionManager: Singleton connection manager instance
    """
    manager = SnowflakeConnectionManager()
    manager.establish_connection()
    return manager
