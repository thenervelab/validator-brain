import logging
import os
from typing import Optional

import asyncpg
import httpx

# Configure logging
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

_http_client = None


def get_http_client():
    """
    Get or create a shared HTTP client for connection pooling.
    
    Returns:
        An httpx.AsyncClient instance with configured timeouts and connection limits
    """
    global _http_client
    if _http_client is None:
        # Create a persistent client with connection pooling
        _http_client = httpx.AsyncClient(timeout=30.0, limits=httpx.Limits(max_connections=200),
            follow_redirects=True)
    return _http_client


async def close_http_client():
    """Close the global HTTP client if it exists."""
    global _http_client
    if _http_client is not None:
        await _http_client.aclose()
        _http_client = None
        logger.info("HTTP client closed")


db_pool = None


async def init_db_pool(dsn: Optional[str] = None, init_schema: bool = True):
    """
    Initialize the global database connection pool and optionally create schema.
    
    Args:
        dsn: Database connection string. If not provided, uses environment variables.
        init_schema: Whether to initialize the database schema if tables don't exist.
    
    Returns:
        The initialized database pool
    """
    global db_pool

    if db_pool is not None:
        logger.info("Database pool already initialized")
        return db_pool

    # If no DSN provided, construct from environment variables
    if dsn is None:
        postgres_user = os.getenv("POSTGRES_USER", "user")
        postgres_password = os.getenv("POSTGRES_PASSWORD", "password")
        postgres_db = os.getenv("POSTGRES_DB", "hippius")
        postgres_host = os.getenv("POSTGRES_HOST", "localhost")
        postgres_port = os.getenv("POSTGRES_PORT", "5432")

        dsn = f"postgresql://{postgres_user}:{postgres_password}@{postgres_host}:{postgres_port}/{postgres_db}"

    try:
        # Create the connection pool
        logger.info(f"Initializing database pool with DSN: {dsn}")
        db_pool = await asyncpg.create_pool(dsn=dsn, min_size=5, max_size=20, command_timeout=60)
        logger.info("Database pool initialized successfully")

        # Initialize schema if requested
        if init_schema:
            await init_database_schema()

        return db_pool
    except Exception as e:
        logger.error(f"Failed to initialize database pool: {e}")
        raise


async def init_database_schema():
    """Initialize the database schema from schema.sql if it doesn't exist."""
    try:
        schema_path = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
                                   'schema.sql')

        # Check if schema file exists
        if not os.path.exists(schema_path):
            schema_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'schema.sql')
            if not os.path.exists(schema_path):
                logger.error(f"Schema file not found: {schema_path}")
                return

        # Read schema file
        with open(schema_path, 'r') as f:
            schema_sql = f.read()

        # Execute schema SQL
        global db_pool
        async with db_pool.acquire() as conn:
            # Check if tables already exist
            table_exists = await conn.fetchval("""
                SELECT EXISTS(
                    SELECT FROM information_schema.tables 
                    WHERE table_name = 'latest_block'
                )
                """)

            if not table_exists:
                logger.info("Initializing database schema")
                await conn.execute(schema_sql)
                logger.info("Database schema initialized successfully")
            else:
                logger.info("Database schema already exists")

    except Exception as e:
        logger.error(f"Error initializing database schema: {e}")
        raise


async def close_db_pool():
    """Close the global database pool if it exists."""
    global db_pool
    if db_pool is not None:
        await db_pool.close()
        db_pool = None
        logger.info("Database pool closed")


def get_db_pool():
    """
    Get the global database pool. Raises an exception if not initialized.
    
    Returns:
        The global asyncpg connection pool
    """
    global db_pool
    if db_pool is None:
        raise RuntimeError("Database pool not initialized. Call init_db_pool first.")
    return db_pool


def get_ipfs_node_url():
    """
    Get the IPFS node URL from environment variables or use default.
    
    Returns:
        IPFS node URL as string
    """
    return os.environ.get("IPFS_NODE_URL", "http://localhost:5001").rstrip('/')


def get_ipfs_timeout(operation_type="default"):
    """
    Get timeout duration for IPFS operations based on operation type.
    
    Args:
        operation_type: Type of operation (default, ping, dht, refs)
    
    Returns:
        Timeout in seconds
    """
    timeout_map = {"default": int(os.environ.get("IPFS_TIMEOUT_SECONDS", 10)),
        "ping": int(os.environ.get("IPFS_TIMEOUT_SECONDS", 10)),
        "dht": int(os.environ.get("IPFS_DHT_TIMEOUT_SECONDS", 60)),
        "refs": int(os.environ.get("IPFS_REFS_TIMEOUT_SECONDS", 30)),
        "fetch": int(os.environ.get("IPFS_FETCH_TIMEOUT", 60))}

    return timeout_map.get(operation_type, timeout_map["default"])


def parse_env_bool(env_var, default=False):
    """
    Parse a boolean environment variable.
    
    Args:
        env_var: Name of the environment variable
        default: Default value if variable is not set
    
    Returns:
        Boolean value of the environment variable
    """
    value = os.environ.get(env_var)
    if value is None:
        return default

    return value.lower() in ('true', 'yes', '1', 't', 'y')


def get_epoch_block_interval():
    """
    Get the epoch block interval from environment variables or use default.
    
    Returns:
        Epoch block interval as integer
    """
    return int(os.environ.get("EPOCH_BLOCK_INTERVAL", 100))
