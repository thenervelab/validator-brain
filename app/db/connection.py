"""Database connection management."""

import logging
import os

import asyncpg

logger = logging.getLogger(__name__)

db_pool = None


async def init_db_pool():
    """
    Initialize the global database connection pool and optionally create schema.

    Args:
        dsn: Database connection string. If not provided, uses environment variables.
        init_schema: Whether to initialize the database schema if tables don't exist.

    Returns:
        The initialized database pool
    """
    global db_pool

    dsn = os.getenv("DATABASE_URL")
    
    # Get connection pool settings from environment variables with sensible defaults
    min_size = int(os.getenv("DB_POOL_MIN_SIZE", "2"))  # Reduced from 5 to 2
    max_size = int(os.getenv("DB_POOL_MAX_SIZE", "8"))  # Reduced from 20 to 8
    command_timeout = int(os.getenv("DB_COMMAND_TIMEOUT", "60"))
    
    logger.info(f"Initializing database pool with DSN: {dsn}")
    logger.info(f"Pool settings: min_size={min_size}, max_size={max_size}, command_timeout={command_timeout}")
    
    db_pool = await asyncpg.create_pool(
        dsn=dsn,
        min_size=min_size,
        max_size=max_size,
        command_timeout=command_timeout,
        # Add connection pool optimization settings
        max_inactive_connection_lifetime=300,  # 5 minutes
        max_queries=50000,  # Recycle connections after 50k queries
        setup=None,  # No per-connection setup
    )
    logger.info("Database pool initialized successfully")

    return db_pool


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
