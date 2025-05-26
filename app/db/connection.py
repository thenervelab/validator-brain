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
