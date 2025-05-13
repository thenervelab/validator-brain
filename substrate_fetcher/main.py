# substrate_fetcher/main.py
import asyncio
import time
import os
import sys

# Add parent directory to path so imports work regardless of where script is run from
script_path = os.path.abspath(os.path.dirname(__file__))
parent_dir = os.path.dirname(script_path)
if parent_dir not in sys.path:
    sys.path.insert(0, parent_dir)

# Use regular imports with sys.path manipulation
import storage_fetcher
import config
from substrate_fetcher import utils # Import utils

# _fetcher_task = None # No longer a thread, but an asyncio task
async def application_main_loop():
    """The main application logic running in asyncio."""
    print("Starting Substrate Storage Fetcher Application (Async)...")

    # Initialize database pool
    try:
        config.db_pool = await utils.create_db_pool()
        if config.db_pool:
            await utils.init_db(config.db_pool) # Initialize tables
        else:
            print("Failed to initialize database pool. Certain features might not work.")
            # Decide if you want to exit or continue without DB
            # For now, we'll let it continue and potentially fail later if DB is strictly needed
    except Exception as e:
        print(f"Database initialization failed: {e}. Exiting.")
        return # Exit if DB init fails critically

    print(f"Monitoring node: {config.NODE_URL}")
    print(f"Fetching items on new blocks (see config for specifics).")
    print("Press Ctrl+C to exit.")

    # Start the fetching logic as an asyncio task
    # This task will run start_fetching_loop_async()
    fetcher_task = asyncio.create_task(storage_fetcher.start_fetching_loop_async())

    last_printed_block = -1
    try:
        while not storage_fetcher.is_stopping(): # Check our async exit event
            current_data = storage_fetcher.get_latest_data()
            current_status = storage_fetcher.get_status()

            if current_data and current_data.get("block_number", -1) > last_printed_block:
                print(f"\n--- Main App: Processed Block #{current_data['block_number']} ---")
                # Assuming OUTPUT_JSON_FILE is still relevant or handled if DB is primary
                if hasattr(config, 'OUTPUT_JSON_FILE'):
                    print(f"--- Data saved to {config.OUTPUT_JSON_FILE} (Status: {current_status}) ---")
                else:
                    print(f"--- Status: {current_status} ---")
                last_printed_block = current_data["block_number"]
            elif not current_data and last_printed_block == -1:
                pass # print(f"Status: {current_status}", end='\r') # Optional status print

            await asyncio.sleep(config.MAIN_LOOP_SLEEP_INTERVAL) # Async sleep

    except asyncio.CancelledError:
        print("\nMain application loop cancelled.")
    finally:
        print("Requesting storage fetcher to stop...")
        await storage_fetcher.stop_fetching_async() # Signal the fetcher task

        if fetcher_task and not fetcher_task.done():
            print("Waiting for fetcher task to complete...")
            try:
                await asyncio.wait_for(fetcher_task, timeout=10)
            except asyncio.TimeoutError:
                print("Fetcher task did not stop in time. Attempting to cancel.")
                fetcher_task.cancel()
                try:
                    await fetcher_task # Await cancellation
                except asyncio.CancelledError:
                    print("Fetcher task cancelled successfully.")
            except Exception as e:
                print(f"Error during fetcher task shutdown: {e}")
        
        # Close the database pool
        if config.db_pool:
            print("Closing database connection pool...")
            await config.db_pool.close()
            print("Database connection pool closed.")
            
        print("Application shut down.")

def run_application():
    """Sets up and runs the asyncio event loop for the application."""
    try:
        asyncio.run(application_main_loop())
    except KeyboardInterrupt:
        print("\nCtrl+C received by run_application. asyncio loop should handle shutdown.")
    # The asyncio.run() should handle KeyboardInterrupt by cancelling tasks.
    # If tasks don't handle CancelledError properly, they might not clean up.

if __name__ == "__main__":
    run_application()