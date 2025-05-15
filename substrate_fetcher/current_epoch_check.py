import asyncio
import asyncpg
import storage_fetcher
from utils import create_db_pool, logger
from substrate_utils import load_hips_keypair
import os
import config

async def check_current_epoch_validator():
    """Checks the current_epoch_validator table and logs if the account_id matches the HIPS key."""
    # Create database pool
    pool = await create_db_pool()

    try:
        # Load HIPS keypair to get the SS58 address
        keypair = load_hips_keypair(config.KEYSTORE_PATH)
        hips_account_id = keypair.ss58_address
        logger.info(f"Checking current epoch validator with HIPS account: {hips_account_id}")

        # Acquire connection and query the latest current_epoch_validator entry
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
                logger.info(f"Latest current_epoch_validator: account_id={account_id}, block_number={block_number}, updated_at={updated_at}")

                if account_id == hips_account_id:
                    logger.info(f"Match found: HIPS account {hips_account_id} is the current epoch validator")
                else:
                    logger.info(f"No match: HIPS account {hips_account_id} is not the current epoch validator")
            else:
                logger.info("No entries found in current_epoch_validator table")

    except Exception as e:
        logger.error(f"Error checking current_epoch_validator: {e}")
    finally:
        if pool:
            await pool.close()

async def main():
    await check_current_epoch_validator()

if __name__ == "__main__":
    asyncio.run(main())

