import asyncio
from substrate_fetcher.substrate_utils import load_hips_keypair
import logging
import time
from . import config
from ipfs_utils import ping_ipfs_node

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
    # sync strorage requests and add that to pending pool
    await sync_storage_requests(block_number)

async def sync_storage_requests(block_number):
    """Queries user_storage_requests and transfers new records to pending_pool."""
    async with config.db_pool.acquire() as conn:
        # Fetch all records from user_storage_requests
        rows = await conn.fetch(
            """
            SELECT owner_account_id, file_hash, total_replicas, file_name, last_charged_at, created_at, miner_ids, selected_validator, is_assigned, updated_at
            FROM user_storage_requests
            """
        )

        for row in rows:
            owner = row['owner_account_id']
            file_hash = row['file_hash']

            # Check if the record already exists in pending_pool
            exists = await conn.fetchval(
                """
                SELECT EXISTS (
                    SELECT 1 FROM pending_pool WHERE owner = $1 AND file_hash = $2
                )""",
                owner, file_hash
            )

            if not exists:
                # Insert new record into pending_pool
                await conn.execute(
                    """
                    INSERT INTO pending_pool (owner, file_hash, status)
                    VALUES ($1, $2, $3)
                    """,
                    owner, file_hash, "pending"
                )
                logger.info(f"Added new record to pending_pool: owner={owner}, file_hash={file_hash}")
            else:
                logger.debug(f"Record already exists in pending_pool: owner={owner}, file_hash={file_hash}")

    # Log the action call
    logger.info(f"Performing action at block {block_number} (Transferred records to pending_pool)")

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
                    await rebalance_offline_miners(pool)
                    await perform_action(current_block_number)
                else:
                    logger.info(f"No match: HIPS account {hips_account_id} is not the current validator at block {block_number}")
            else:
                logger.info("No entries found in current_epoch_validator table")
        await asyncio.sleep(5)


async def get_offline_miners(pool: asyncpg.Pool, base_ipfs_url: str = "http://") -> list:
    """Fetch all miners and identify offline ones with non-empty miner_profile_cid."""
    offline_miners = []
    
    async with pool.acquire() as conn:
        # Fetch all miners from the miners table
        miners = await conn.fetch(
            """
            SELECT node_id, ipfs_storage_max, ipfs_zfs_pool_size, last_online_block, miner_profile_cid
            FROM miners
            """
        )

        # Fetch ipfs_node_id from registration table for each miner
        for miner in miners:
            node_id = miner['node_id']
            miner_profile_cid = miner['miner_profile_cid']
            
            # Skip if miner_profile_cid is None or empty
            if not miner_profile_cid or miner_profile_cid.strip() == "":
                continue

            # Get ipfs_node_id from registration table
            ipfs_node_id = await conn.fetchval(
                """
                SELECT ipfs_node_id FROM registration WHERE node_id = $1
                """,
                node_id
            )

            if ipfs_node_id:
                # Construct API URL (assuming port 5001, adjust if different)
                api_url = f"{base_ipfs_url}{ipfs_node_id}:5001"
                is_reachable = await ping_ipfs_node(ipfs_node_id)
                
                if not is_reachable:
                    offline_miners.append({
                        "node_id": node_id,
                        "ipfs_node_id": ipfs_node_id,
                        "miner_profile_cid": miner_profile_cid
                    })
                    logger.info(f"Offline miner detected: node_id={node_id}, ipfs_node_id={ipfs_node_id}, miner_profile_cid={miner_profile_cid}")
            else:
                logger.warning(f"No ipfs_node_id found for miner: {node_id}")

    return offline_miners

async def rebalance_offline_miners(pool: asyncpg.Pool):
    # Call get_offline_miners only at the start of the epoch
    offline_miners = await get_offline_miners(pool)
    if offline_miners:
        logger.info(f"Offline miners detected at epoch start: {offline_miners}")
    else:
        logger.info("No offline miners detected at epoch start")
