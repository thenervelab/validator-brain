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
    logger.info(f"Initializing database pool with DSN: {dsn}")
    db_pool = await asyncpg.create_pool(
        dsn=dsn,
        min_size=5,
        max_size=20,
        command_timeout=60,
    )
    logger.info("Database pool initialized successfully")

    return db_pool


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
                logger.warning(
                    "Please run migrations using dbmate before starting the application."
                )
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
