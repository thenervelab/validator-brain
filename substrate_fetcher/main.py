# substrate_fetcher/main.py
import asyncio
import os
import sys
import signal

# Add parent directory to path
script_path = os.path.abspath(os.path.dirname(__file__))
parent_dir = os.path.dirname(script_path)
if parent_dir not in sys.path:
    sys.path.insert(0, parent_dir)

import storage_fetcher
import config
from substrate_fetcher import utils

async def application_main_loop():
    """The main application logic running in asyncio."""
    print("Starting Substrate Storage Fetcher Application (Async)...")

    # Initialize database pool
    try:
        config.db_pool = await utils.create_db_pool()
        if config.db_pool:
            await utils.init_db(config.db_pool)
        else:
            print("Failed to initialize database pool. Certain features might not work.")
    except Exception as e:
        print(f"Database initialization failed: {e}. Exiting.")
        return

    print(f"Monitoring node: {config.NODE_URL}")
    print("Press Ctrl+C to exit.")

    # Create an event for shutdown coordination
    shutdown_event = asyncio.Event()

    # Set up signal handlers
    def handle_signal():
        print("\nSignal received, shutting down...")
        shutdown_event.set()

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, handle_signal)

    # Start the fetching task
    fetcher_task = asyncio.create_task(storage_fetcher.start_fetching_loop_async())

    last_printed_block = -1
    try:
        while not shutdown_event.is_set() and not storage_fetcher.is_stopping():
            current_data = storage_fetcher.get_latest_data()
            current_status = storage_fetcher.get_status()

            if current_data and current_data.get("block_number", -1) > last_printed_block:
                print(f"\n--- Main App: Processed Block #{current_data['block_number']} ---")
                print(f"--- Status: {current_status} ---")
                last_printed_block = current_data["block_number"]

            try:
                await asyncio.wait_for(shutdown_event.wait(), timeout=config.MAIN_LOOP_SLEEP_INTERVAL)
            except asyncio.TimeoutError:
                continue

    except Exception as e:
        print(f"Error in main loop: {e}")
    finally:
        print("Initiating shutdown sequence...")
        
        # Signal the fetcher to stop
        await storage_fetcher.stop_fetching_async()

        # Wait for fetcher task to complete
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

        # Close the database pool
        if config.db_pool:
            print("Closing database connection pool...")
            await config.db_pool.close()
            print("Database connection pool closed.")
            
        print("Application shutdown complete.")

def run_application():
    """Sets up and runs the asyncio event loop for the application."""
    try:
        asyncio.run(application_main_loop())
    except KeyboardInterrupt:
        print("\nApplication terminated by user.")
    except Exception as e:
        print(f"Unexpected error: {e}")
        sys.exit(1)

if __name__ == "__main__":
    run_application()