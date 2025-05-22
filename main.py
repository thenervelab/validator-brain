import asyncio
import logging
import os
import signal
import sys

from db_manager import (get_latest_block_number, is_current_validator, get_storage_miners)
from health_checker import (check_miner_health, get_offline_miners, update_miner_health_metrics)
from profile_manager import (process_completed_storage_requests, prepare_miner_profile_updates)
from storage_processor import (process_storage_requests, sync_storage_requests)
from utils import (init_db_pool, close_db_pool, close_http_client, get_db_pool,
                   get_epoch_block_interval, logger)

# Global state
shutdown_event = asyncio.Event()


def handle_signals():
    """Set up signal handlers for graceful shutdown."""

    def handle_signal(sig, _):
        logger.info(f"Received signal {sig}, initiating shutdown...")
        shutdown_event.set()

    signal.signal(signal.SIGINT, handle_signal)
    signal.signal(signal.SIGTERM, handle_signal)


async def initialize():
    """Initialize required resources."""
    logger.info("Initializing IPFS Service Validator...")

    # Initialize database pool
    await init_db_pool()
    logger.info("Database pool initialized")

    # Test IPFS node connectivity
    ipfs_node_url = os.environ.get("IPFS_NODE_URL")
    logger.info(f"Using IPFS node at {ipfs_node_url}")

    # Other initialization if needed

    logger.info("Initialization complete")


async def cleanup():
    """Clean up resources during shutdown."""
    logger.info("Cleaning up resources...")

    # Close database pool
    await close_db_pool()

    # Close HTTP client
    await close_http_client()

    logger.info("Cleanup complete")


async def perform_validator_actions(block_number: int, epoch_end_block: int):
    """
    Perform validator actions based on position in epoch.
    
    Args:
        block_number: Current block number
        epoch_end_block: Block number when epoch ends
    """
    # Calculate position within epoch (0-99 for 100 block epochs)
    epoch_length = get_epoch_block_interval()
    position_in_epoch = (epoch_end_block - block_number) % epoch_length

    logger.info(
        f"Position in epoch: {position_in_epoch}/{epoch_length} (block {block_number}, epoch ends at {epoch_end_block})")

    # Early epoch tasks (0-30) - process storage requests
    if position_in_epoch > 70:
        logger.info("Early epoch: Processing storage requests")
        await sync_storage_requests()
        user_profiles, miner_profiles = await process_storage_requests()

    # Mid-epoch tasks (30-70) - check miner health
    elif 30 <= position_in_epoch <= 70:
        logger.info("Mid-epoch: Checking miner health")
        miners = await get_storage_miners()
        health_results = await check_miner_health(miners)
        await update_miner_health_metrics(health_results)

    # Late epoch tasks (70-100) - update blockchain with results
    elif position_in_epoch < 30:
        logger.info("Late epoch: Finalizing validation")
        # Fetch profiles
        user_profiles, miner_profiles = await process_storage_requests()

        # Process completed storage requests and update blockchain
        db_pool = get_db_pool()
        async with db_pool.acquire() as conn:
            # Prepare blockchain submissions
            pin_requests = await process_completed_storage_requests(conn, user_profiles)
            miner_updates = await prepare_miner_profile_updates(miner_profiles)

            # Submit to blockchain (placeholder for actual submission)
            logger.info(
                f"Would submit {len(pin_requests)} pin requests and {len(miner_updates)} miner updates to blockchain")
            # TODO: Implement actual blockchain submission

            # Check for offline miners
            offline_miners = await get_offline_miners()
            if offline_miners:
                logger.info(
                    f"Detected {len(offline_miners)} offline miners")  # TODO: Implement rebalancing from offline miners


async def validator_main_loop():
    """Main loop for validator operations."""
    keypair_path = os.environ.get("KEYSTORE_PATH")
    validator_account_id = os.environ.get("VALIDATOR_ACCOUNT_ID")

    logger.info(f"Starting main loop with validator account {validator_account_id}")

    while not shutdown_event.is_set():
        try:
            # Get current block number
            current_block = await get_latest_block_number()

            if current_block is None:
                logger.warning("Cannot determine current block number, waiting...")
                await asyncio.sleep(5)
                continue

            # Check if we are the current validator
            is_validator, epoch_end_block = await is_current_validator(validator_account_id)

            if is_validator:
                logger.info(f"We are the current validator at block {current_block}")
                await perform_validator_actions(current_block, epoch_end_block)
            else:
                logger.info(f"Not the current validator at block {current_block}")

            # Sleep for a bit before next check
            await asyncio.sleep(5)

        except Exception as e:
            logger.error(f"Error in main loop: {e}")
            await asyncio.sleep(5)


async def main():
    """Application entry point."""
    # Set up signal handlers
    handle_signals()

    try:
        # Initialize resources
        await initialize()

        # Run main validator loop
        await validator_main_loop()

    except Exception as e:
        logger.error(f"Fatal error: {e}")
        return 1
    finally:
        # Clean up resources
        await cleanup()

    return 0


if __name__ == "__main__":
    # Configure logging
    logging.basicConfig(level=logging.INFO,
                        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
                        handlers=[logging.StreamHandler(), logging.FileHandler("validator.log")])

    # Run the main function
    exit_code = asyncio.run(main())
    sys.exit(exit_code)
