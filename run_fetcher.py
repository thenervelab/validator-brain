#!/usr/bin/env python3
# substrate_project/run_fetcher.py
import sys
import os
import asyncio
from substrate_fetcher import config, utils,storage_fetcher
import asyncpg

# Ensure the substrate_fetcher package can be found
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

async def initialize_database():
    """Initializes the database connection pool and creates tables."""
    try:
        config.db_pool = await asyncpg.create_pool(config.DATABASE_URL)
        print("Successfully created database connection pool.")
        await utils.init_db(config.db_pool)
        print("Database tables initialized.")
    except Exception as e:
        print(f"Failed to initialize database: {e}")
        raise

async def run_application():
    """Runs the Substrate fetcher application with database initialization."""
    try:
        # Initialize the database
        await initialize_database()

        # Start the fetching loop
        await storage_fetcher.start_fetching_loop_async()

    except Exception as e:
        print(f"Application error: {e}")
        await storage_fetcher.stop_fetching_async()
        raise
    finally:
        if config.db_pool:
            print("Closing database connection pool...")
            await config.db_pool.close()
            print("Database connection pool closed.")
            config.db_pool = None  # Reset to None after closing

if __name__ == "__main__":
    try:
        asyncio.run(run_application())
    except KeyboardInterrupt:
        print("\nShutting down due to keyboard interrupt...")
        # Ensure stop_fetching_async runs in the same event loop context
        loop = asyncio.get_event_loop()
        if loop.is_running():
            loop.run_until_complete(storage_fetcher.stop_fetching_async())
        else:
            asyncio.run(storage_fetcher.stop_fetching_async())
    except Exception as e:
        print(f"Unexpected error: {e}")
        sys.exit(1)