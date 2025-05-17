import asyncio
import os
import sys
import signal
from urllib.parse import urlparse
from loguru import logger

# Ensure parent directory is in path so imports work from anywhere
script_path = os.path.abspath(os.path.dirname(__file__))
parent_dir = os.path.dirname(script_path)
if parent_dir not in sys.path:
    sys.path.insert(0, parent_dir)

# Corrected imports for modules within the same package
from . import storage_fetcher
from . import config
from . import utils
from . import current_epoch_check
# from . import ipfs_health_service # Assuming this might also be needed

# Remove old logging configuration if any remnants are left
# For example, if logging.basicConfig or logging.getLogger was here

# Example global loguru configuration (can be expanded):
logger.remove() # Optional: remove default stderr handler if you want to customize it fully
logger.add(sys.stderr, level="INFO") # Add stderr handler with INFO level
# You can add file logging here if needed, e.g.:
# logger.add("substrate_fetcher_{time}.log", rotation="10 MB", level="DEBUG")

async def initialize_database():
    """Initializes the database connection pool and creates tables."""
    try:
        config.db_pool = await utils.create_db_pool()
        if config.db_pool:
            await utils.init_db(config.db_pool)
            logger.info("Database initialized.")
        else:
            logger.error("Failed to initialize database pool. Certain features might not work.")
    except Exception as e:
        logger.error(f"Database initialization failed: {e}. Exiting.")
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
        logger.info(f"Successfully verified IPFS node is running at {host}:{port}.")
        return True
    except Exception as e:
        logger.error(f"Failed to connect to IPFS node at {config.IPFS_NODE_URL}: {e}")
        raise

async def application_main_loop():
    """The main application logic running in asyncio."""
    logger.info("Starting Substrate Storage Fetcher Application (Async)...")

    # Initialize database pool and IPFS node
    try:
        await initialize_database()
        await initialize_ipfs_node()
    except Exception as e:
        logger.error(f"Initialization failed: {e}. Exiting.")
        return

    logger.info(f"Monitoring node: {config.NODE_URL}")
    logger.info("Press Ctrl+C to exit.")

    # Create an event for shutdown coordination
    shutdown_event = asyncio.Event()

    # Set up signal handlers
    def handle_signal():
        logger.warning("\nSignal received, shutting down...")
        shutdown_event.set()

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, handle_signal)

    # Start the fetching task
    fetcher_task = asyncio.create_task(storage_fetcher.start_fetching_loop_async())
    logger.info("Started substrate fetcher loop.")

    # Start the IPFS health service
    health_service_task = await ipfs_health_service.start_ping_service()
    if health_service_task:
        logger.info("Started IPFS health service.")
    else:
        logger.info("IPFS health service was already running or failed to start.")

    # Start the validator epoch monitor task
    validator_task = asyncio.create_task(current_epoch_check.monitor_validator_epochs(config.db_pool))
    logger.info("Started validator epoch monitor.")

    last_printed_block = -1
    try:
        while not shutdown_event.is_set() and not storage_fetcher.is_stopping():
            current_data = storage_fetcher.get_latest_data()
            current_status = storage_fetcher.get_status()

            if current_data and current_data.get("block_number", -1) > last_printed_block:
                logger.info(f"\n--- Main App: Processed Block #{current_data['block_number']} ---")
                logger.info(f"--- Fetcher Status: {current_status} ---")
                last_printed_block = current_data["block_number"]

            try:
                await asyncio.wait_for(shutdown_event.wait(), timeout=config.MAIN_LOOP_SLEEP_INTERVAL)
            except asyncio.TimeoutError:
                continue

    except Exception as e:
        logger.error(f"Error in main loop: {e}")
    finally:
        logger.info("Initiating shutdown sequence...")

        # Stop the IPFS health service
        if health_service_task:
            await ipfs_health_service.stop_ping_service()
            logger.info("Stopped IPFS health service.")

        # Cancel the validator task
        if validator_task and not validator_task.done():
            logger.info("Cancelling validator monitor task...")
            validator_task.cancel()
            try:
                await validator_task
            except asyncio.CancelledError:
                logger.info("Validator monitor task cancelled successfully.")
            except Exception as e:
                logger.error(f"Error during validator task cancellation: {e}")

        # Signal the fetcher to stop
        await storage_fetcher.stop_fetching_async()
        logger.info("Stopped substrate fetcher.")

        # Wait for fetcher task to complete
        if fetcher_task and not fetcher_task.done():
            logger.info("Waiting for fetcher task to complete...")
            try:
                await asyncio.wait_for(fetcher_task, timeout=10)
            except asyncio.TimeoutError:
                logger.warning("Fetcher task did not stop in time, cancelling...")
                fetcher_task.cancel()
                try:
                    await fetcher_task
                except asyncio.CancelledError:
                    logger.info("Fetcher task cancelled successfully.")
                except Exception as e:
                    logger.error(f"Error during fetcher task cancellation: {e}")

        # Close the database pool
        if config.db_pool:
            logger.info("Closing database connection pool...")
            await config.db_pool.close()
            logger.info("Database connection pool closed.")

        logger.info("Application shutdown complete.")

def run_application():
    """Sets up and runs the asyncio event loop for the application."""
    try:
        asyncio.run(application_main_loop())
    except KeyboardInterrupt:
        logger.info("\nApplication terminated by user.")
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        sys.exit(1)

if __name__ == "__main__":
    run_application()