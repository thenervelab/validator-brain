import asyncio
from substrate_fetcher.substrate_utils import load_hips_keypair
import logging
import time
from . import config

logger = logging.getLogger(__name__)

async def get_latest_block_number(pool):
    """Fetches the latest block number from the latest_block table."""
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            """
            SELECT block_number
            FROM latest_block
            ORDER BY updated_at DESC
            LIMIT 1
            """
        )
        if row:
            return row['block_number']
        else:
            logger.error("No entries found in latest_block table")
            return None

async def perform_action(block_number):
    """Logs a message during the active epoch. Replace with your desired action."""
    logger.info(f"Performing action at block {block_number} (HIPS is validator)")

async def monitor_validator_epochs(pool):
    """Monitors the current_epoch_validator table and logs for 100 blocks when HIPS key matches."""
    keypair = load_hips_keypair(config.KEYSTORE_PATH)
    hips_account_id = keypair.ss58_address
    logger.info(f"Starting validator epoch monitor with HIPS account: {hips_account_id} at {time.strftime('%I:%M %p PKT, %B %d, %Y')}")

    in_action_period = False
    target_block_number = None
    last_checked_block = None

    while True:
        current_block_number = await get_latest_block_number(pool)
        logger.info(f"Current block number: {current_block_number}")
        if current_block_number is None:
            logger.info("Cannot proceed without current block number. Retrying in 5 seconds...")
            await asyncio.sleep(5)
            continue

        # If we're in an action period, continue logging until the epoch ends
        if in_action_period:
            if current_block_number >= target_block_number:
                logger.info(f"Finished action period at block {current_block_number} (target: {target_block_number})")
                in_action_period = False
                target_block_number = None
                last_checked_block = None
            else:
                await perform_action(current_block_number)
            await asyncio.sleep(5)
            continue

        # Fetch the latest validator entry (only one item in the table)
        async with pool.acquire() as conn:
            row = await conn.fetchrow(
                """
                SELECT account_id, block_number, updated_at
                FROM current_epoch_validator
                ORDER BY updated_at DESC
                LIMIT 1
                """
            )

            if row:
                account_id = row['account_id']
                block_number = row['block_number']
                updated_at = row['updated_at']
                logger.info(f"Checking validator: account_id={account_id}, block_number={block_number}, updated_at={updated_at}")

                # Skip if we've already checked this block number (same epoch)
                if last_checked_block == block_number:
                    logger.info(f"Already checked block {block_number}, skipping until epoch changes")
                    await asyncio.sleep(5)
                    continue

                last_checked_block = block_number

                if account_id == hips_account_id:
                    # New match found, start a 100-block action period
                    in_action_period = True
                    target_block_number = block_number + 100
                    logger.info(f"Match found: HIPS account {hips_account_id} is the current validator at block {block_number}")
                    logger.info(f"Will perform action until block {target_block_number} (current block: {current_block_number})")
                    await perform_action(current_block_number)
                else:
                    logger.info(f"No match: HIPS account {hips_account_id} is not the current validator at block {block_number}")
            else:
                logger.info("No entries found in current_epoch_validator table")

        await asyncio.sleep(5)

# take all storage Requests and add in pending pool if not there do that each block till epoch end
# pin all miners and then put cid of offline miners and rebelance pending pool
# store the json of all profile in a folder so we dont need to fetch each time
# take pending requests from the db and fulfill one by one and update status
# at the end on 95th block take all fulfilled requests and then submit batch 
    # 1) user storage requests
    # 2) update minerProfiles (rebalance)
    # 3) unpin requests