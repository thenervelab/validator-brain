import asyncio
import random
from typing import List, Dict, Tuple

from db_manager import (update_user_profile, update_miner_profile, load_profiles_to_memory,
                        get_pending_storage_requests, update_storage_request_status,
                        get_storage_miners)
from ipfs_api import ping_ipfs_node, get_file_size
from utils import get_db_pool, logger


async def process_storage_requests() -> Tuple[Dict, Dict]:
    """
    Process pending storage requests and assign them to miners based on capacity.
    
    This function:
    1. Retrieves pending storage requests from the database
    2. Fetches available miners with their storage capacities and health metrics
    3. Allocates requests to miners using a weighted capacity planning algorithm
    4. Updates database entries for miners and users
    5. Updates the pending_pool status to "processed"
    
    Returns:
        Tuple of (user_profiles, miner_profiles) dictionaries
    """
    logger.info("Processing storage requests")

    try:
        db_pool = get_db_pool()
        async with db_pool.acquire() as conn:
            # Step 1: Fetch all registered StorageMiners with their metrics
            storage_miners = await get_storage_miners()

            if not storage_miners:
                logger.warning("No StorageMiners found in registration table.")
                return {}, {}

            # Step 2: Score miners based on capacity, current load, and reliability
            scored_miners = await score_miners(storage_miners)

            logger.info(f"Found {len(scored_miners)} active miners for assignment")

            if len(scored_miners) < 5:
                logger.warning(
                    f"Insufficient miners available (found {len(scored_miners)}, need 5). Skipping request processing.")
                return {}, {}

            # Step 3: Fetch pending storage requests
            pending_requests = await get_pending_storage_requests(limit=10)

            if not pending_requests:
                logger.info("No pending requests to process.")
                return {}, {}

            logger.info(f"Processing {len(pending_requests)} pending storage requests")

            # Initialize in-memory profile data structures
            user_profiles = {}  # owner -> list of file entries
            miner_profiles = {}  # miner_id -> list of file entries

            # First, load existing profile data from database to memory
            await load_profiles_to_memory(conn, user_profiles, miner_profiles)

            # Step 4: Process each storage request
            for request in pending_requests:
                owner = request['owner']
                file_hash = request['file_hash']
                file_name = request['file_name']
                selected_validator = request['selected_validator']
                main_req_hash = request['main_req_hash']

                # Get file size for capacity planning
                file_size = await get_file_size(file_hash)

                # Select miners for this request
                selected_miner_ids = select_miners_for_request(scored_miners, file_size)
                logger.info(f"Selected miners for request {file_hash}: {selected_miner_ids}")

                # Update pending_pool status and set selected_miners
                await update_storage_request_status(conn, owner=owner, file_hash=file_hash,
                    status="processed", selected_miners=selected_miner_ids)

                # Update user_profile table
                await update_user_profile(conn, owner=owner, file_hash=file_hash,
                    file_name=file_name, file_size=file_size, selected_validator=selected_validator,
                    main_req_hash=main_req_hash, miner_ids=selected_miner_ids)

                # Update miner_profile table for each selected miner
                for miner_id in selected_miner_ids:
                    await update_miner_profile(conn, miner_node_id=miner_id, file_hash=file_hash,
                        file_size=file_size, selected_validator=selected_validator)

                # Update in-memory profiles
                # 1. Update user profile
                if owner not in user_profiles:
                    user_profiles[owner] = []

                # Check if entry for this file_hash already exists
                entry_found = False
                for entry in user_profiles[owner]:
                    if entry.get('file_hash') == file_hash:
                        entry['file_name'] = file_name
                        entry['file_size_in_bytes'] = file_size
                        entry['is_assigned'] = True
                        entry['miner_ids'] = selected_miner_ids
                        entry['selected_validator'] = selected_validator
                        entry['main_req_hash'] = main_req_hash
                        entry_found = True
                        break

                # Add new entry if not found
                if not entry_found:
                    current_time = int(asyncio.get_event_loop().time())
                    user_profiles[owner].append(
                        {"created_at": current_time, "file_hash": file_hash, "file_name": file_name,
                            "file_size_in_bytes": file_size, "is_assigned": True,
                            "last_charged_at": current_time, "main_req_hash": main_req_hash,
                            "miner_ids": selected_miner_ids, "owner": owner,
                            "selected_validator": selected_validator,
                            "total_replicas": len(selected_miner_ids)})

                # 2. Update miner profiles
                for miner_id in selected_miner_ids:
                    if miner_id not in miner_profiles:
                        miner_profiles[miner_id] = []

                    # Check if entry exists
                    entry_found = False
                    for entry in miner_profiles[miner_id]:
                        if entry.get('file_hash') == file_hash:
                            entry['file_size_in_bytes'] = file_size
                            entry['selected_validator'] = selected_validator
                            entry_found = True
                            break

                    # Add new entry if not found
                    if not entry_found:
                        miner_profiles[miner_id].append(
                            {"created_at": int(asyncio.get_event_loop().time()),
                                "file_hash": file_hash, "file_size_in_bytes": file_size,
                                "miner_node_id": miner_id,
                                "selected_validator": selected_validator})

                # Update miner capacity in memory for next assignments in this batch
                update_miner_scores(scored_miners, selected_miner_ids, file_size)

            logger.info(f"Processed {len(pending_requests)} storage requests successfully")

            # Return the in-memory profiles for potential use by other functions
            return user_profiles, miner_profiles

    except Exception as e:
        logger.error(f"Error processing storage requests: {e}")
        return {}, {}


async def score_miners(miners: List[Dict]) -> List[Dict]:
    """
    Score miners based on capacity, current load, and reliability.
    
    Args:
        miners: List of miner dictionaries
        
    Returns:
        List of scored miners sorted by score (highest first)
    """
    scored_miners = []

    for miner in miners:
        node_id = miner['node_id']
        ipfs_node_id = miner['ipfs_node_id']

        # Check if miner is online
        is_online = miner.get('is_online', False)
        if not is_online:
            ping_result = await ping_ipfs_node(ipfs_node_id)
            is_online = ping_result['success']

        if not is_online:
            logger.warning(f"Miner {node_id} is offline, skipping from assignment")
            continue

        # Calculate miner's available storage
        max_storage = miner['ipfs_storage_max'] or miner[
            'ipfs_zfs_pool_size'] or 1000000000  # 1GB default
        used_storage = miner['miner_total_files_size'] or 0
        available_storage = max(0, max_storage - used_storage)

        # Calculate score - higher is better
        # Score = Available storage percentage * 0.6 + (1 - normalized file count) * 0.3 + success rate * 0.1
        storage_score = available_storage / max_storage if max_storage > 0 else 0
        file_count = miner['miner_total_files_pinned'] or 0
        file_count_normalized = min(1.0,
                                    file_count / 1000)  # Normalize to 0-1 range, assuming 1000 files as max
        file_score = 1.0 - file_count_normalized  # Fewer files is better
        success_rate = miner.get('miner_success_rate', 1.0)  # Default to 1.0 if not available

        total_score = (storage_score * 0.6) + (file_score * 0.3) + (success_rate * 0.1)

        scored_miners.append({'node_id': node_id, 'ipfs_node_id': ipfs_node_id,
            'available_storage': available_storage, 'max_storage': max_storage,
            'file_count': file_count, 'success_rate': success_rate, 'score': total_score})

    # Sort miners by score (highest first)
    scored_miners.sort(key=lambda m: m['score'], reverse=True)

    return scored_miners


def select_miners_for_request(scored_miners: List[Dict], file_size: int) -> List[str]:
    """
    Select miners for a storage request based on capacity and scoring.
    
    Args:
        scored_miners: List of scored miners
        file_size: Size of the file to be stored
        
    Returns:
        List of selected miner IDs
    """
    # Take top 15 miners by score
    top_miners = scored_miners[:15]

    # Pick 5 randomly from top 15 to distribute load
    selected_miners = random.sample(top_miners, min(5, len(top_miners)))

    # Extract just the node IDs
    selected_miner_ids = [m['node_id'] for m in selected_miners]

    return selected_miner_ids


def update_miner_scores(scored_miners: List[Dict], selected_miner_ids: List[str], file_size: int):
    """
    Update miner scores after a storage assignment.
    
    Args:
        scored_miners: List of scored miners
        selected_miner_ids: List of selected miner IDs
        file_size: Size of the file assigned
    """
    for miner in scored_miners:
        if miner['node_id'] in selected_miner_ids:
            # Update miner's available storage
            miner['available_storage'] = max(0, miner['available_storage'] - file_size)
            miner['file_count'] += 1

            # Recalculate score
            storage_score = miner['available_storage'] / miner['max_storage'] if miner[
                                                                                     'max_storage'] > 0 else 0
            file_count_normalized = min(1.0, miner['file_count'] / 1000)
            file_score = 1.0 - file_count_normalized
            miner['score'] = (storage_score * 0.6) + (file_score * 0.3) + (
                        miner['success_rate'] * 0.1)

    # Re-sort miners by updated scores
    scored_miners.sort(key=lambda m: m['score'], reverse=True)


async def sync_storage_requests():
    """
    Sync storage requests from user_storage_requests to pending_pool.
    """
    logger.info("Syncing storage requests to pending pool")

    try:
        db_pool = get_db_pool()
        async with db_pool.acquire() as conn:
            # Fetch all records from user_storage_requests
            rows = await conn.fetch("""
                SELECT owner_account_id, file_hash, total_replicas, file_name, last_charged_at, created_at, 
                       miner_ids, selected_validator, is_assigned, updated_at
                FROM user_storage_requests
                """)

            for row in rows:
                owner = row['owner_account_id']
                original_file_hash = row['file_hash']
                original_file_name = row['file_name']
                selected_validator = row['selected_validator']

                # Skip if already processed
                existing = await conn.fetchval("""
                    SELECT EXISTS (
                        SELECT 1 FROM pending_pool WHERE main_req_hash = $1
                    )
                    """, original_file_hash)

                if existing:
                    logger.debug(
                        f"Request already exists in pending_pool: main_req_hash={original_file_hash}")
                    continue

                # Add to pending_pool
                await conn.execute("""
                    INSERT INTO pending_pool (owner, file_hash, file_name, selected_validator, main_req_hash, status, selected_miners)
                    VALUES ($1, $2, $3, $4, $5, $6, $7)
                    """, owner, original_file_hash, original_file_name, selected_validator,
                    original_file_hash, "pending", [])
                logger.info(
                    f"Added new record to pending_pool: owner={owner}, file_hash={original_file_hash}")

            logger.info(f"Synced {len(rows)} storage requests")

    except Exception as e:
        logger.error(f"Error syncing storage requests: {e}")
        raise
