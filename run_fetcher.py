#!/usr/bin/env python3
import sys
import os
import asyncio
import signal
from urllib.parse import urlparse
from substrate_fetcher import config, utils, storage_fetcher, ipfs_health_service
from substrate_fetcher.current_epoch_check import monitor_validator_epochs
from loguru import logger

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

async def initialize_database():
    """Initializes the database connection pool and creates tables."""
    try:
        config.db_pool = await utils.create_db_pool()
        print("Successfully created database connection pool.")
        await utils.init_db(config.db_pool)
    except Exception as e:
        print(f"Failed to initialize database: {e}")
        raise

async def initialize_ipfs_node():
    """Checks if the IPFS node is up and running by performing a TCP ping."""
    try:
        # Parse the IPFS node URL to extract host and port
        parsed_url = urlparse(config.IPFS_NODE_URL)
        host = parsed_url.hostname or "localhost"
        port = parsed_url.port or 5001  # Default IPFS API port

        # Attempt to establish a TCP connection
        reader, writer = await asyncio.wait_for(
            asyncio.open_connection(host, port),
            timeout=5
        )
        # If connection is successful, close it immediately
        writer.close()
        await writer.wait_closed()
        print(f"Successfully verified IPFS node is running at {host}:{port}.")
        return True
    except Exception as e:
        print(f"Failed to connect to IPFS node at {config.IPFS_NODE_URL}: {e}")
        raise

async def run_application():
    """Runs the Substrate fetcher application with database, IPFS node, and health service initialization."""
    # Create shutdown event
    shutdown_event = asyncio.Event()

    # Set up signal handlers
    def handle_signal():
        print("\nSignal received, initiating shutdown...")
        shutdown_event.set()

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, handle_signal)

    fetcher_task = None
    health_service_task = None

    try:
        # Initialize the database
        await initialize_database()

        # Initialize the IPFS node
        await initialize_ipfs_node()

        # Start the substrate fetcher loop
        fetcher_task = asyncio.create_task(storage_fetcher.start_fetching_loop_async())
        print("Started substrate fetcher loop.")

        # Start the IPFS health service
        health_service_task = await ipfs_health_service.start_ping_service()
        if health_service_task:
            print("Started IPFS health service.")
        else:
            print("IPFS health service was already running or failed to start.")

        # Start the validator epoch monitor task
        validator_task = asyncio.create_task(monitor_validator_epochs(config.db_pool))
        print("Started validator epoch monitor.")

        # Wait for shutdown signal
        await shutdown_event.wait()
        print("\nShutdown signal received, cleaning up...")

    except Exception as e:
        print(f"Application error: {e}")
        raise
    finally:
        # Stop the IPFS health service
        if health_service_task:
            await ipfs_health_service.stop_ping_service()
            print("Stopped IPFS health service.")

        # Cancel the validator task
        if validator_task and not validator_task.done():
            print("Cancelling validator monitor task...")
            validator_task.cancel()
            try:
                await validator_task
            except asyncio.CancelledError:
                print("Validator monitor task cancelled successfully.")
            except Exception as e:
                print(f"Error during validator task cancellation: {e}")

        # Stop the fetcher
        await storage_fetcher.stop_fetching_async()
        print("Stopped substrate fetcher.")

        # Wait for fetcher task to complete if it exists
        if fetcher_task and not fetcher_task.done():
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