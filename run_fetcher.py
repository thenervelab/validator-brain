#!/usr/bin/env python3
import sys
import os
import asyncio
import signal
from substrate_fetcher import config, utils, storage_fetcher

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

async def initialize_database():
    """Initializes the database connection pool and creates tables."""
    try:
        config.db_pool = await utils.create_db_pool()
        print("Successfully created database connection pool.")
        await utils.init_db(config.db_pool)
        print("Database tables initialized.")
    except Exception as e:
        print(f"Failed to initialize database: {e}")
        raise

async def run_application():
    """Runs the Substrate fetcher application with database initialization."""
    # Create shutdown event
    shutdown_event = asyncio.Event()

    # Set up signal handlers
    def handle_signal():
        print("\nSignal received, initiating shutdown...")
        shutdown_event.set()

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, handle_signal)

    try:
        # Initialize the database
        await initialize_database()

        # Start the fetching loop
        fetcher_task = asyncio.create_task(storage_fetcher.start_fetching_loop_async())

        # Wait for shutdown signal
        await shutdown_event.wait()
        print("\nShutdown signal received, cleaning up...")

    except Exception as e:
        print(f"Application error: {e}")
        raise
    finally:
        # Stop the fetcher
        await storage_fetcher.stop_fetching_async()

        # Wait for fetcher task to complete if it exists
        if 'fetcher_task' in locals() and not fetcher_task.done():
            print("Waiting for fetcher task to complete...")
            try:
                await asyncio.wait_for(fetcher_task, timeout=10)
            except asyncio.TimeoutError:
                print("Fetcher task did not stop in time, cancelling...")
                fetcher_task.cancel()
                try:
                    await fetcher_task
                except asyncio.CancelledError:
                    print("Fetcher task cancelled successfully.")
                except Exception as e:
                    print(f"Error during fetcher task cancellation: {e}")

        # Close database pool
        if config.db_pool:
            print("Closing database connection pool...")
            await config.db_pool.close()
            print("Database connection pool closed.")
            config.db_pool = None

        print("Shutdown complete.")

if __name__ == "__main__":
    try:
        asyncio.run(run_application())
    except KeyboardInterrupt:
        print("\nApplication terminated by user.")
    except Exception as e:
        print(f"Unexpected error: {e}")
        sys.exit(1)