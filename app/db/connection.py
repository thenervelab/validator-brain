"""Database connection management."""
import logging
import os
from typing import Optional

import asyncpg

from app.db.sql import load_query

# Configure logging
logger = logging.getLogger(__name__)

# Global database pool
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

        # Initialize schema if requested - this is now managed by dbmate
        # but we check if tables exist as a fallback
        if init_schema:
            await check_tables_exist()

        return db_pool
    except Exception as e:
        logger.error(f"Failed to initialize database pool: {e}")
        raise


async def check_tables_exist():
    """Check if essential tables exist and warn if they don't."""
    try:
        global db_pool
        async with db_pool.acquire() as conn:
            # Check if latest_block table exists
            table_exists = await conn.fetchval("""
                SELECT EXISTS(
                    SELECT FROM information_schema.tables 
                    WHERE table_name = 'latest_block'
                )
                """)

            if not table_exists:
                logger.warning("Database schema not initialized! Tables missing.")
                logger.warning("Please run migrations using dbmate before starting the application.")
            else:
                logger.info("Database schema exists")

    except Exception as e:
        logger.error(f"Error checking database schema: {e}")
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