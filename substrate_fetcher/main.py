# substrate_fetcher/main.py
import asyncio
import time
# Removed threading import as we're using asyncio now

from . import storage_fetcher
from . import config

# _fetcher_task = None # No longer a thread, but an asyncio task

async def application_main_loop():
    """The main application logic running in asyncio."""
    print("Starting Substrate Storage Fetcher Application (Async)...")
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
                print(f"--- Data saved to {config.OUTPUT_JSON_FILE} (Status: {current_status}) ---")
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
        print("Application shut down.")

def run_application():
    """Sets up and runs the asyncio event loop for the application."""
    try:
        asyncio.run(application_main_loop())
    except KeyboardInterrupt:
        print("\nCtrl+C received by run_application. asyncio loop should handle shutdown.")
    # The asyncio.run() should handle KeyboardInterrupt by cancelling tasks.
    # If tasks don't handle CancelledError properly, they might not clean up.