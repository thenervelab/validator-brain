"""Database manager service."""
import os
from typing import List, Dict, Optional, Tuple

from app.db.connection import get_db_pool
from app.db.sql import load_query
from app.utils.logging import logger


async def get_latest_block_number() -> Optional[int]:
    """
    Fetches the latest block number from the database.
    
    Returns:
        The latest block number or None if no entries found
    """
    db_pool = get_db_pool()
    async with db_pool.acquire() as conn:
        row = await conn.fetchrow(load_query("get_latest_block_number"))
        return row['block_number'] if row else None


async def update_user_profile(conn, owner: str, file_hash: str, file_name: str, file_size: int,
        selected_validator: str, main_req_hash: str, miner_ids: List[str]):
    """
    Update the user_profile table with file storage information.
    
    Args:
        conn: Database connection
        owner: Owner account ID
        file_hash: CID of the file
        file_name: Name of the file
        file_size: Size of the file in bytes
        selected_validator: Validator account ID
        main_req_hash: Original request hash
        miner_ids: List of selected miner IDs
    """
    try:
        # Check if entry already exists
        existing = await conn.fetchval(
            load_query("check_user_profile_exists"), 
            owner, file_hash
        )

        if existing:
            # Update existing entry
            await conn.execute(
                load_query("update_existing_user_profile"),
                owner, file_hash, file_name, file_size, selected_validator, main_req_hash,
                miner_ids
            )
            logger.info(f"Updated user profile for owner={owner}, file_hash={file_hash}")
        else:
            # Create new entry
            await conn.execute(
                load_query("create_new_user_profile"),
                owner, file_hash, file_name, file_size, main_req_hash, miner_ids,
                selected_validator, len(miner_ids)
            )
            logger.info(f"Created user profile for owner={owner}, file_hash={file_hash}")

    except Exception as e:
        logger.error(f"Error updating user profile for owner={owner}, file_hash={file_hash}: {e}")
        raise


async def update_miner_profile(conn, miner_node_id: str, file_hash: str, file_size: int,
        selected_validator: str):
    """
    Update the miner_profile table with file storage information.
    
    Args:
        conn: Database connection
        miner_node_id: Miner node ID
        file_hash: CID of the file
        file_size: Size of the file in bytes
        selected_validator: Validator account ID
    """
    try:
        # Check if entry already exists
        existing = await conn.fetchval(
            load_query("check_miner_profile_exists"),
            miner_node_id, file_hash
        )

        if existing:
            # Update existing entry
            await conn.execute(
                load_query("update_existing_miner_profile"),
                miner_node_id, file_hash, file_size, selected_validator
            )
            logger.info(f"Updated miner profile for miner={miner_node_id}, file_hash={file_hash}")
        else:
            # Create new entry
            await conn.execute(
                load_query("create_new_miner_profile"),
                miner_node_id, file_hash, file_size, selected_validator
            )
            logger.info(f"Created miner profile for miner={miner_node_id}, file_hash={file_hash}")

            # Update miner totals
            await conn.execute(
                load_query("update_miner_totals"),
                miner_node_id, file_size
            )

    except Exception as e:
        logger.error(
            f"Error updating miner profile for miner={miner_node_id}, file_hash={file_hash}: {e}")
        raise


async def load_profiles_to_memory(conn, user_profiles, miner_profiles):
    """
    Load existing profiles from database into memory.
    
    Args:
        conn: Database connection
        user_profiles: Dictionary to populate with user profiles
        miner_profiles: Dictionary to populate with miner profiles
    """
    try:
        # Load user profiles
        user_rows = await conn.fetch(load_query("get_all_user_profiles"))

        for row in user_rows:
            user_id = row['user_id']
            if user_id not in user_profiles:
                user_profiles[user_id] = []

            user_profiles[user_id].append({
                "created_at": row['created_at'].timestamp() if hasattr(row['created_at'],
                                                                   'timestamp') else row[
                    'created_at'], "file_hash": row['file_hash'], "file_name": row['file_name'],
                "file_size_in_bytes": row['file_size_in_bytes'], "is_assigned": row['is_assigned'],
                "last_charged_at": row['last_charged_at'].timestamp() if hasattr(
                    row['last_charged_at'], 'timestamp') else row['last_charged_at'],
                "main_req_hash": row['main_req_hash'], "miner_ids": row['miner_ids'],
                "owner": row['owner'], "selected_validator": row['selected_validator'],
                "total_replicas": row['total_replicas']})

        # Load miner profiles
        miner_rows = await conn.fetch(load_query("get_all_miner_profiles"))

        for row in miner_rows:
            miner_id = row['miner_node_id']
            if miner_id not in miner_profiles:
                miner_profiles[miner_id] = []

            miner_profiles[miner_id].append({
                "created_at": row['created_at'].timestamp() if hasattr(row['created_at'],
                                                                   'timestamp') else row[
                    'created_at'], "file_hash": row['file_hash'],
                "file_size_in_bytes": row['file_size_in_bytes'], "miner_node_id": miner_id,
                "selected_validator": row['selected_validator']})

        logger.info(
            f"Loaded {len(user_profiles)} user profiles and {len(miner_profiles)} miner profiles from database")

    except Exception as e:
        logger.error(f"Error loading profiles to memory: {e}")
        raise


async def get_pending_storage_requests(limit: int = 10) -> List[Dict]:
    """
    Fetch pending storage requests from the database.
    
    Args:
        limit: Maximum number of requests to fetch
        
    Returns:
        List of pending storage requests
    """
    db_pool = get_db_pool()
    async with db_pool.acquire() as conn:
        rows = await conn.fetch(
            load_query("get_pending_storage_requests"),
            limit
        )

        # Convert to list of dictionaries
        requests = [dict(row) for row in rows]
        logger.info(f"Fetched {len(requests)} pending storage requests")

        return requests


async def update_storage_request_status(conn, owner: str, file_hash: str, status: str,
        selected_miners: Optional[List[str]] = None):
    """
    Update the status of a storage request in the pending_pool table.
    
    Args:
        conn: Database connection
        owner: Owner account ID
        file_hash: CID of the file
        status: New status ('pending', 'processed', 'completed')
        selected_miners: List of selected miner IDs (for 'processed' status)
    """
    try:
        if status == 'processed' and selected_miners:
            await conn.execute(
                load_query("update_storage_request_status_with_miners"),
                status, owner, file_hash, selected_miners
            )
        else:
            await conn.execute(
                load_query("update_storage_request_status"),
                status, owner, file_hash
            )

        logger.info(
            f"Updated storage request status to {status} for owner={owner}, file_hash={file_hash}")

    except Exception as e:
        logger.error(
            f"Error updating storage request status for owner={owner}, file_hash={file_hash}: {e}")
        raise


async def get_storage_miners() -> List[Dict]:
    """
    Fetch all registered StorageMiners with their metrics from the database.
    
    Returns:
        List of storage miner dictionaries
    """
    db_pool = get_db_pool()
    async with db_pool.acquire() as conn:
        rows = await conn.fetch(load_query("get_storage_miners"))

        # Convert to list of dictionaries
        miners = [dict(row) for row in rows]
        logger.info(f"Fetched {len(miners)} storage miners")

        return miners


async def delete_completed_storage_request(conn, owner: str, file_hash: str, main_req_hash: str):
    """
    Delete a completed storage request from pending_pool and user_storage_requests.
    
    Args:
        conn: Database connection
        owner: Owner account ID
        file_hash: CID of the file
        main_req_hash: Original request hash
    """
    try:
        # Delete from pending_pool
        await conn.execute(
            load_query("delete_from_pending_pool"),
            owner, file_hash
        )
        logger.info(
            f"Deleted processed request from pending_pool: owner={owner}, file_hash={file_hash}")

        # Delete from user_storage_requests
        await conn.execute(
            load_query("delete_from_user_storage_requests"),
            owner, main_req_hash
        )
        logger.info(
            f"Deleted record from user_storage_requests: owner={owner}, file_hash={main_req_hash}")

    except Exception as e:
        logger.error(
            f"Error deleting completed storage request for owner={owner}, file_hash={file_hash}: {e}")
        raise


async def is_current_validator(account_id: str) -> Tuple[bool, Optional[int]]:
    """
    Check if the provided account ID is the current epoch validator.
    
    Args:
        account_id: Account ID to check
        
    Returns:
        Tuple of (is_validator, target_block_number)
    """
    db_pool = get_db_pool()
    async with db_pool.acquire() as conn:
        row = await conn.fetchrow(load_query("get_current_validator"))

        if row and row['validator_account'] == account_id:
            current_block = await get_latest_block_number()
            epoch_length = int(os.environ.get("EPOCH_BLOCK_INTERVAL", "100"))
            epoch_end_block = row['selected_at_block'] + epoch_length

            return True, epoch_end_block

        return False, None


async def get_storage_requests_count() -> int:
    """
    Get the count of storage requests in the database.
    
    Returns:
        The count of storage requests
    """
    db_pool = get_db_pool()
    async with db_pool.acquire() as conn:
        # Count from storage_requests table
        count = await conn.fetchval("SELECT COUNT(*) FROM storage_requests")
        return count or 0