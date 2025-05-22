"""Core validator functionality."""
import asyncio
import os
import signal
from typing import Optional

from app.db.connection import get_db_pool
from app.services.db_manager import get_latest_block_number, is_current_validator, get_storage_miners
from app.services.health_checker import check_miner_health, get_offline_miners, update_miner_health_metrics
from app.services.profile_manager import process_completed_storage_requests, prepare_miner_profile_updates
from app.services.storage_processor import process_storage_requests, sync_storage_requests
from app.utils.config import get_epoch_block_interval
from app.utils.logging import logger

# Global state
shutdown_event = asyncio.Event()


def handle_signals():
    """Set up signal handlers for graceful shutdown."""

    def handle_signal(sig, _):
        logger.info(f"Received signal {sig}, initiating shutdown...")
        shutdown_event.set()

    signal.signal(signal.SIGINT, handle_signal)
    signal.signal(signal.SIGTERM, handle_signal)


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

    logger.info(f"Position in epoch: {position_in_epoch}/{epoch_length} (block {block_number}, epoch ends at {epoch_end_block})")

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

            # Submit to blockchain
            logger.info(f"Submitting {len(pin_requests)} pin requests and {len(miner_updates)} miner updates to blockchain")
            
            # Check for offline miners
            offline_miners = await get_offline_miners()
            if offline_miners:
                logger.info(f"Detected {len(offline_miners)} offline miners")


async def validator_main_loop():
    """Main loop for validator operations."""
    validator_account_id = os.environ.get("VALIDATOR_ACCOUNT_ID")
    logger.info(f"Starting main loop with validator account {validator_account_id}")

    while not shutdown_event.is_set():
        # Get current block number
        current_block = await get_latest_block_number()

        if current_block is None:
            continue

        # Check if we are the current validator
        is_validator, epoch_end_block = await is_current_validator(validator_account_id)

        if is_validator:
            logger.info(f"We are the current validator at block {current_block}")
            await perform_validator_actions(current_block, epoch_end_block)
        else:
            logger.info(f"Not the current validator at block {current_block}")

        # Wait before next iteration
        await asyncio.sleep(5)