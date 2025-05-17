# substrate_fetcher/ipfs_health_service.py
import asyncio
import logging
import signal
import sys
import time
import math
from . import config
from .ipfs_health_utils import perform_ipfs_ping, EPOCH_BLOCK_INTERVAL

logger = logging.getLogger(__name__)

# Number of miners to process in parallel
PING_BATCH_SIZE = 50

# Maximum time to wait for a batch to complete (seconds)
BATCH_TIMEOUT_SECONDS = 10

# Seconds to wait between epoch checks
EPOCH_CHECK_INTERVAL = 15

# Track service state
_running = False
_active_tasks = set()  # Keep track of all active tasks for clean shutdown
_last_processed_epoch = None  # Track the last epoch we processed

async def get_current_block_and_epoch():
    """Get the current block number and epoch from the database."""
    if not config.db_pool:
        logger.error("Database pool not initialized")
        return None, None
    
    try:
        async with config.db_pool.acquire() as conn:
            # Try to get latest block number from our tracked state
            result = await conn.fetchrow(
                "SELECT MAX(block_number) as block, MAX(block_number) / $1 as epoch FROM current_epoch_validator",
                EPOCH_BLOCK_INTERVAL
            )
            
            if result and result['block']:
                return result['block'], int(result['epoch'])
            
            # Fallback - no blocks in our DB yet
            return 0, 0
    except Exception as e:
        logger.error(f"Error getting current block/epoch: {e}")
        return None, None

async def get_all_miners():
    """Get all miners with IPFS node IDs from registration."""
    if not config.db_pool:
        logger.error("Database pool not initialized")
        return []
    
    try:
        async with config.db_pool.acquire() as conn:
            miners = await conn.fetch(
                """
                SELECT r.node_id, r.ipfs_node_id 
                FROM registration r
                WHERE r.ipfs_node_id IS NOT NULL
                ORDER BY node_id
                """
            )
            logger.info(f"Found {len(miners)} miners with IPFS node IDs in registration")
            return miners
    except Exception as e:
        logger.error(f"Error getting miners: {e}")
        return []

async def get_already_processed_miners_for_epoch(epoch):
    """Get all miner IDs that have already been processed for this epoch."""
    if not config.db_pool:
        logger.error("Database pool not initialized")
        return set()
    
    try:
        async with config.db_pool.acquire() as conn:
            results = await conn.fetch(
                """
                SELECT node_id 
                FROM miner_epoch_health
                WHERE epoch = $1
                """,
                epoch
            )
            processed_ids = {r['node_id'] for r in results}
            logger.info(f"Found {len(processed_ids)} miners already processed for epoch {epoch}")
            return processed_ids
    except Exception as e:
        logger.error(f"Error getting processed miners: {e}")
        return set()

async def process_miners_for_epoch(current_epoch, current_block, stop_event):
    """Process all miners for the current epoch."""
    global _last_processed_epoch
    
    # Skip if we've already processed this epoch
    if _last_processed_epoch == current_epoch:
        logger.debug(f"Already processed epoch {current_epoch}, waiting for next epoch")
        return
    
    logger.info(f"Starting to process all miners for epoch {current_epoch}")
    
    # Get all miners with IPFS node IDs
    all_miners = await get_all_miners()
    if not all_miners:
        logger.warning("No miners found with IPFS node IDs")
        return
    
    # Get miners already processed for this epoch (to avoid duplicates)
    already_processed = await get_already_processed_miners_for_epoch(current_epoch)
    
    # Filter out miners that have already been processed
    miners_to_process = [m for m in all_miners if m['node_id'] not in already_processed]
    
    if not miners_to_process:
        logger.info(f"All {len(all_miners)} miners have already been processed for epoch {current_epoch}")
        _last_processed_epoch = current_epoch
        return
    
    # Number of batches we'll need to process all miners
    num_batches = math.ceil(len(miners_to_process) / PING_BATCH_SIZE)
    logger.info(f"Processing {len(miners_to_process)} miners in {num_batches} batches for epoch {current_epoch}")
    
    # Process miners in batches
    total_success = 0
    total_failed = 0
    
    for batch_index in range(num_batches):
        # Check if we should stop
        if stop_event.is_set():
            logger.info("Stop event detected, stopping epoch processing")
            return
        
        # Get the current batch of miners
        start_idx = batch_index * PING_BATCH_SIZE
        end_idx = min(start_idx + PING_BATCH_SIZE, len(miners_to_process))
        current_batch = miners_to_process[start_idx:end_idx]
        
        logger.info(f"Processing batch {batch_index + 1}/{num_batches} with {len(current_batch)} miners")
        
        # Create and start ping tasks
        ping_tasks = []
        for miner in current_batch:
            task = asyncio.create_task(
                perform_ipfs_ping(
                    config.db_pool,
                    miner['node_id'],
                    miner['ipfs_node_id'],
                    current_epoch,
                    current_block
                )
            )
            ping_tasks.append(task)
            _active_tasks.add(task)
        
        # Wait for all pings to complete with timeout
        try:
            start_time = time.time()
            results = await asyncio.wait_for(
                asyncio.gather(*ping_tasks, return_exceptions=True),
                timeout=BATCH_TIMEOUT_SECONDS
            )
            
            # Count successes and failures
            batch_success = sum(1 for r in results if not isinstance(r, Exception))
            batch_failed = len(results) - batch_success
            
            total_success += batch_success
            total_failed += batch_failed
            
            # Log batch results
            elapsed = time.time() - start_time
            logger.info(f"Completed batch {batch_index + 1}/{num_batches} in {elapsed:.2f}s: "
                       f"{batch_success} successful, {batch_failed} failed")
            
            # Log specific errors if any
            for i, result in enumerate(results):
                if isinstance(result, Exception) and not isinstance(result, asyncio.CancelledError):
                    selected_miner = current_batch[i]
                    logger.error(f"Error during IPFS ping for miner {selected_miner['node_id']} "
                               f"(IPFS: {selected_miner['ipfs_node_id']}): {result}")
        
        except asyncio.TimeoutError:
            # Timeout occurred, some pings are taking too long
            elapsed = time.time() - start_time
            logger.warning(f"Batch {batch_index + 1}/{num_batches} timeout after {elapsed:.2f}s - "
                         f"proceeding to next batch. Some pings may still be running in background.")
            
            # Cancel any remaining tasks to avoid zombie tasks
            pending_tasks = [t for t in ping_tasks if not t.done()]
            for task in pending_tasks:
                task.cancel()
            
            if pending_tasks:
                logger.warning(f"Cancelled {len(pending_tasks)} pending ping tasks that exceeded the timeout")
    
    # Update last processed epoch
    _last_processed_epoch = current_epoch
    
    # Log completion
    logger.info(f"Completed processing for epoch {current_epoch}: "
              f"{total_success} successful pings, {total_failed} failed")

async def ping_service_loop(stop_event):
    """
    Main service loop that checks for epoch transitions and pings all miners at each new epoch.
    """
    global _running
    
    if _running:
        logger.warning("IPFS ping service already running")
        return
    
    _running = True
    
    logger.info("Starting IPFS ping service (epoch-based)")
    
    try:
        while not stop_event.is_set():
            # Get current block and epoch
            current_block, current_epoch = await get_current_block_and_epoch()
            
            if current_block is None or current_epoch is None:
                logger.warning("Could not determine current block/epoch. Waiting before retry...")
                await asyncio.sleep(EPOCH_CHECK_INTERVAL)
                continue
            
            # Check if we need to process this epoch
            if _last_processed_epoch != current_epoch:
                logger.info(f"New epoch detected: {current_epoch} (previous: {_last_processed_epoch})")
                await process_miners_for_epoch(current_epoch, current_block, stop_event)
            else:
                logger.debug(f"Epoch {current_epoch} already processed, waiting for next epoch")
            
            # Wait before checking again
            try:
                # Use wait_for to allow interrupt during sleep
                await asyncio.wait_for(
                    stop_event.wait(),
                    timeout=EPOCH_CHECK_INTERVAL
                )
                logger.info("Stop event received during epoch check interval")
                break
            except asyncio.TimeoutError:
                # This is normal - just continue to the next iteration
                pass
            
    except asyncio.CancelledError:
        logger.info("IPFS ping service task cancelled")
    except Exception as e:
        logger.error(f"Error in IPFS ping service: {e}")
    finally:
        _running = False
        logger.info("IPFS ping service loop exited")

async def start_ping_service(shutdown_event=None):
    """
    Start the IPFS ping service as a background task.
    Returns the task object.
    """
    if _running:
        logger.warning("IPFS ping service is already running")
        return None
    
    # Use the provided shutdown event or create a new one
    stop_event = shutdown_event if shutdown_event is not None else asyncio.Event()
    
    # Create and return the task
    service_task = asyncio.create_task(ping_service_loop(stop_event))
    _active_tasks.add(service_task)  # Track the main service task
    return service_task

async def stop_ping_service():
    """
    Signal the ping service to stop and wait for it to finish.
    Cancels any pending tasks and enforces a timeout for clean shutdown.
    """
    global _running
    
    if not _running:
        logger.info("IPFS ping service is not running")
        return
    
    logger.info("Stopping IPFS ping service...")
    
    # Cancel all active tasks to ensure they don't hang
    if _active_tasks:
        logger.info(f"Cancelling {len(_active_tasks)} active tasks")
        for task in _active_tasks:
            if not task.done():
                task.cancel()
    
    # Wait for running flag to clear with timeout
    try:
        shutdown_start = time.time()
        # Wait for service to mark itself as stopped or timeout
        shutdown_timeout = 5
        while _running and (time.time() - shutdown_start) < shutdown_timeout:
            await asyncio.sleep(0.1)
        
        if _running:
            logger.warning(f"Service took too long to stop (>{shutdown_timeout}s), forcing exit")
            _running = False
    except Exception as e:
        logger.error(f"Error during shutdown: {e}")
        _running = False
    
    _active_tasks.clear()  # Clear the task tracking set
    logger.info("IPFS ping service successfully stopped")

# Main entry point for running as a standalone service
if __name__ == "__main__":
    # Configure logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )

    logger.info("Starting IPFS health service")

    # Import here to avoid circular imports
    from . import utils
    
    # Create a class to hold state and avoid nonlocal variables
    class ServiceManager:
        def __init__(self):
            self.loop = asyncio.new_event_loop()
            asyncio.set_event_loop(self.loop)
            self.service_task = None
            self.is_shutting_down = False
            self.shutdown_event = asyncio.Event()
        
        def shutdown_handler(self):
            if self.is_shutting_down:
                logger.warning("Force shutdown requested - exiting immediately")
                sys.exit(1)  # Force exit if called twice
                
            self.is_shutting_down = True
            print("\nShutdown signal received. Press Ctrl+C again to force exit.")
            self.shutdown_event.set()
            
            # Schedule the shutdown coroutine
            asyncio.create_task(self.shutdown())
        
        async def shutdown(self):
            logger.info("Shutting down IPFS health service...")
            
            # Stop the ping service
            await stop_ping_service()
            
            # Cancel the main service task if it exists
            if self.service_task and not self.service_task.done():
                logger.info("Cancelling main service task")
                self.service_task.cancel()
                try:
                    await asyncio.wait_for(self.service_task, timeout=1.0)
                except (asyncio.CancelledError, asyncio.TimeoutError):
                    pass
            
            # Stop the event loop
            logger.info("Stopping event loop")
            self.loop.stop()
        
        async def main(self):
            # Initialize database
            logger.info("Initializing database connection...")
            config.db_pool = await utils.create_db_pool()
            await utils.init_db(config.db_pool)
            
            # Start the service
            logger.info("Starting ping service...")
            self.service_task = await start_ping_service(self.shutdown_event)
            
            try:
                # Keep the main task running until the service completes
                await self.service_task
            except asyncio.CancelledError:
                logger.info("Main task cancelled")
            finally:
                logger.info("Main task complete")
        
        def run(self):
            try:
                # Register signal handlers
                for sig in (signal.SIGINT, signal.SIGTERM):
                    self.loop.add_signal_handler(sig, self.shutdown_handler)
                
                # Run the main coroutine
                self.loop.run_until_complete(self.main())
                self.loop.run_forever()
                logger.info("Event loop stopped")
            
            except KeyboardInterrupt:
                logger.info("Keyboard interrupt received")
            except Exception as e:
                logger.exception(f"Unexpected error: {e}")
            finally:
                # Final cleanup
                logger.info("Cleaning up resources...")
                
                # Close the event loop
                try:
                    tasks = asyncio.all_tasks(self.loop)
                    for task in tasks:
                        task.cancel()
                    
                    # Allow tasks to respond to cancellation
                    if tasks:
                        self.loop.run_until_complete(asyncio.gather(*tasks, return_exceptions=True))
                except Exception as e:
                    logger.error(f"Error during final cleanup: {e}")
                finally:
                    logger.info("Closing event loop")
                    self.loop.close()
                
                print("Service has exited completely")
    
    # Create and run the service manager
    manager = ServiceManager()
    try:
        manager.run()
    finally:
        sys.exit(0)