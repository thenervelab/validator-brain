import asyncio
import asyncpg
from substrate_fetcher.substrate_utils import load_hips_keypair, call_update_pin_and_storage_requests, call_update_miner_profiles, call_update_pin_check_metrics
# import logging # Removed
import time
import aiohttp
import json
import os
import shutil
import random
from typing import List, Dict
from datetime import datetime, timezone # Added for timestamp conversion
from . import config
from . import utils
from . import ipfs_utils
from . import substrate_utils
from loguru import logger # Added
import aiofiles # Added for async file operations
import aiofiles.os as aios # For async os operations if needed, though shutil.rmtree is sync

# logger = logging.getLogger(__name__) # Removed

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

async def ping_ipfs_node(ipfs_peer_id: str) -> bool:
    """Pings an IPFS node to test connectivity without storing results in the database."""
    if not ipfs_peer_id:
        logger.warning(f"No IPFS peer ID provided. Skipping ping.")
        return False

    logger.info(f"Pinging IPFS node: {ipfs_peer_id}...")
    
    ping_successful = False
    api_url = f"{config.IPFS_NODE_URL.rstrip('/')}/api/v0/ping"
    params = {'arg': ipfs_peer_id, 'count': '1'}  # count must be a string for query params
    timeout_seconds = getattr(config, 'IPFS_TIMEOUT_SECONDS', 10)
    request_timeout = aiohttp.ClientTimeout(total=timeout_seconds)

    try:
        async with aiohttp.ClientSession(timeout=request_timeout) as session:
            async with session.post(api_url, params=params) as response:
                if response.status == 200:
                    async for line in response.content:
                        try:
                            data = json.loads(line.decode('utf-8'))
                            if data.get('Success') and (data.get('Time') or data.get('AvgLatency')):
                                ping_successful = True
                                break  # Found success signal
                        except json.JSONDecodeError:
                            logger.debug(f"Non-JSON line from IPFS ping for {ipfs_peer_id}: {line}")
                        except Exception as e_parse:
                            logger.warning(f"Error parsing IPFS ping response line for {ipfs_peer_id}: {e_parse}")
                    if not ping_successful:
                        logger.warning(f"IPFS ping to {ipfs_peer_id} completed with HTTP 200 but no definitive success signal (RTT or Avg Latency) in response stream.")
                else:
                    error_text = await response.text()
                    logger.warning(f"IPFS ping to {ipfs_peer_id} failed with status {response.status}: {error_text}")
    except asyncio.TimeoutError:
        logger.warning(f"IPFS ping to {ipfs_peer_id} timed out after {timeout_seconds} seconds.")
    except aiohttp.ClientConnectorError as e_conn:
        logger.error(f"IPFS connection error for {ipfs_peer_id}: {e_conn}")
    except Exception as e_req:
        logger.error(f"Request error during IPFS ping for {ipfs_peer_id}: {e_req}")

    if ping_successful:
        logger.info(f"Successfully pinged IPFS node: {ipfs_peer_id}")
    else:
        logger.warning(f"Failed to ping IPFS node: {ipfs_peer_id}")
    
    return ping_successful

async def perform_action(block_number):
    # Sync storage requests and add them to pending pool
    await sync_storage_requests(block_number)
    await assign_to_storage_miners(block_number)

async def sync_storage_requests(block_number):
    """Queries user_storage_requests, fetches CID content, and transfers records to pending_pool."""
    async with config.db_pool.acquire() as conn:
        # Fetch all records from user_storage_requests
        rows = await conn.fetch(
            """
            SELECT owner_account_id, file_hash, total_replicas, file_name, last_charged_at, created_at, 
                   miner_ids, selected_validator, is_assigned, updated_at
            FROM user_storage_requests
            """
        )
        for row in rows:
            owner = row['owner_account_id']
            original_file_hash = row['file_hash']
            original_file_name = row['file_name']
            selected_validator = row['selected_validator']

            # Fetch content of the original CID
            content_response = await ipfs_utils.get_ipfs_content(original_file_hash, config.IPFS_NODE_URL)
            if not content_response['success']:
                logger.warning(f"Failed to fetch content for CID {original_file_hash}: {content_response['error']}")
                continue

            content = content_response['content']
            if not isinstance(content, list):
                logger.warning(f"Invalid content format for CID {original_file_hash}: Expected list, got {type(content)}")
                continue

            # Process each item in the content array
            for item in content:
                file_name = item.get('filename')
                file_hash = item.get('cid')
                if not file_name or not file_hash:
                    logger.warning(f"Invalid item in content for CID {original_file_hash}: {item}")
                    continue

                # Check if the record already exists in pending_pool
                exists = await conn.fetchval(
                    """
                    SELECT EXISTS (
                        SELECT 1 FROM pending_pool WHERE owner = $1 AND file_hash = $2
                    )""",
                    owner, file_hash
                )

                if not exists:
                    # Insert new record into pending_pool with file_name, selected_validator, main_req_hash, and empty selected_miners
                    await conn.execute(
                        """
                        INSERT INTO pending_pool (owner, file_hash, file_name, selected_validator, main_req_hash, status, selected_miners)
                        VALUES ($1, $2, $3, $4, $5, $6, $7)
                        """,
                        owner, file_hash, file_name, selected_validator, original_file_hash, "pending", []
                    )
                    logger.info(f"Added new record to pending_pool: owner={owner}, file_hash={file_hash}, file_name={file_name}, main_req_hash={original_file_hash}, selected_miners=[]")
                else:
                    logger.debug(f"Record already exists in pending_pool: owner={owner}, file_hash={file_hash}")

async def get_offline_miners(pool: asyncpg.Pool) -> list:
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
                # Use ping_ipfs_node to test connectivity
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

async def reconstruct_profiles_to_json(pool: asyncpg.Pool):
    logger.info("Reconstructing profiles from database to JSON files...")
    profiles_dir = "profiles"
    miner_profile_dir = os.path.join(profiles_dir, "miner_profile")
    user_profile_dir = os.path.join(profiles_dir, "user_profile")

    # shutil.rmtree is synchronous. If this is slow, it could be run in an executor.
    # For simplicity, keeping it synchronous for now as it's usually fast unless a huge number of files.
    if os.path.exists(profiles_dir):
        try:
            loop = asyncio.get_running_loop()
            await loop.run_in_executor(None, shutil.rmtree, profiles_dir) # Run sync rmtree in executor
            logger.info(f"Deleted existing profiles directory: {profiles_dir}")
        except Exception as e:
            logger.error(f"Error deleting profiles directory: {e}")
            return
    try:
        # os.makedirs is synchronous and generally fast.
        os.makedirs(miner_profile_dir, exist_ok=True)
        os.makedirs(user_profile_dir, exist_ok=True)
        logger.info(f"Created directories: {miner_profile_dir}, {user_profile_dir}")
    except Exception as e:
        logger.error(f"Error creating directories: {e}")
        return

    def format_timestamp(ts_val):
        if ts_val is None:
            return None
        if isinstance(ts_val, (int, float)):
            try:
                return datetime.fromtimestamp(ts_val, tz=timezone.utc).isoformat()
            except Exception as e:
                logger.warning(f"Could not convert int/float timestamp '{ts_val}' to datetime: {e}. Storing as is.")
                return ts_val 
        if isinstance(ts_val, datetime):
            return ts_val.isoformat()
        logger.warning(f"Timestamp '{ts_val}' is of unhandled type {type(ts_val)}. Storing as is.")
        return ts_val

    async with pool.acquire() as conn:
        logger.info("Processing miner profiles for JSON reconstruction...")
        miner_rows = await conn.fetch(
            """
            SELECT miner_node_id, created_at, file_hash, file_size_in_bytes, selected_validator, updated_at
            FROM miner_profile
            """
        )
        logger.info(f"Fetched {len(miner_rows)} rows from miner_profile table.")

        for row in miner_rows:
            miner_node_id = row['miner_node_id']
            miner_data_entry = {
                "created_at": format_timestamp(row['created_at']),
                "file_hash": row['file_hash'],
                "file_size_in_bytes": row['file_size_in_bytes'],
                "selected_validator": row['selected_validator'],
                "updated_at": format_timestamp(row['updated_at'])
            }
            miner_file_path = os.path.join(miner_profile_dir, f"{miner_node_id}.json")
            existing_entries = []
            if await aiofiles.os.path.exists(miner_file_path): # Use aiofiles.os.path.exists
                try:
                    async with aiofiles.open(miner_file_path, 'r') as f:
                        content = await f.read()
                        loaded_json = json.loads(content)
                        if not isinstance(loaded_json, list):
                            existing_entries = [loaded_json]
                        else:
                            existing_entries = loaded_json
                except Exception as e:
                    logger.warning(f"Error reading or parsing existing miner profile file {miner_file_path}: {e}")
                    existing_entries = []
            existing_entries.append(miner_data_entry)
            try:
                async with aiofiles.open(miner_file_path, 'w') as f:
                    await f.write(json.dumps(existing_entries, indent=4))
                logger.debug(f"Updated miner profile file: {miner_file_path}")
            except Exception as e:
                logger.error(f"Error writing miner profile file {miner_file_path}: {e}")

        logger.info("Processing user profiles for JSON reconstruction...")
        user_rows = await conn.fetch(
            """
            SELECT user_id, created_at, file_hash, file_name, file_size_in_bytes, is_assigned, last_charged_at, 
                   main_req_hash, miner_ids, owner, selected_validator, total_replicas, updated_at
            FROM user_profile
            """
        )
        logger.info(f"Fetched {len(user_rows)} rows from user_profile table.")

        for row in user_rows:
            user_id = row['user_id']
            user_data_entry = {
                "created_at": format_timestamp(row['created_at']),
                "file_hash": row['file_hash'],
                "file_name": row['file_name'],
                "file_size_in_bytes": row['file_size_in_bytes'],
                "is_assigned": row['is_assigned'],
                "last_charged_at": format_timestamp(row['last_charged_at']),
                "main_req_hash": row['main_req_hash'],
                "miner_ids": row['miner_ids'],
                "owner": row['owner'],
                "selected_validator": row['selected_validator'],
                "total_replicas": row['total_replicas'],
                "updated_at": format_timestamp(row['updated_at'])
            }
            user_file_path = os.path.join(user_profile_dir, f"{user_id}.json")
            existing_entries = []
            if await aiofiles.os.path.exists(user_file_path): # Use aiofiles.os.path.exists
                try:
                    async with aiofiles.open(user_file_path, 'r') as f:
                        content = await f.read()
                        loaded_json = json.loads(content)
                        if not isinstance(loaded_json, list):
                            existing_entries = [loaded_json]
                        else:
                            existing_entries = loaded_json
                except Exception as e:
                    logger.warning(f"Error reading or parsing existing user profile file {user_file_path}: {e}")
                    existing_entries = []
            existing_entries.append(user_data_entry)
            try:
                async with aiofiles.open(user_file_path, 'w') as f:
                    await f.write(json.dumps(existing_entries, indent=4))
                logger.debug(f"Updated user profile file: {user_file_path}")
            except Exception as e:
                logger.error(f"Error writing user profile file {user_file_path}: {e}")

    logger.info("Finished reconstructing profiles to JSON files")

async def update_pin_and_storage_requests_near_epoch_end(block_number):
    """Processes 'processed' requests 5 blocks before epoch end, updates profiles, and submits to chain."""
    logger.info(f"Checking for pin and storage update at block {block_number}...")
    
    # Fetch all processed requests from pending_pool
    async with config.db_pool.acquire() as conn:
        processed_requests = await conn.fetch(
            """
            SELECT owner, file_hash, main_req_hash, selected_miners
            FROM pending_pool
            WHERE status = $1
            """,
            "processed"
        )

    if not processed_requests:
        logger.info("No processed requests found to update pin and storage.")
        return

    logger.info(f"Found {len(processed_requests)} processed requests to update")

    # Process each request
    for request in processed_requests:
        owner = request['owner']
        file_hash = request['file_hash']
        main_req_hash = request['main_req_hash']
        
        # Parse selected_miners from database format (e.g., "{miner1,miner2}") to a Python list
        db_selected_miners = request['selected_miners']
        if isinstance(db_selected_miners, str):
            # Strip curly braces and split by comma
            parsed_miners = db_selected_miners.strip('{}').split(',')
            # Filter out empty strings that can result from an empty array string like "{}"
            selected_miners = [m for m in parsed_miners if m]
        elif isinstance(db_selected_miners, list):
            selected_miners = db_selected_miners  # Already a list
        else:
            selected_miners = [] # Default to empty list if None or other type

        # Load the user's profile JSON
        user_profile_path = os.path.join("profiles", "user_profile", f"{owner}.json")
        user_data = []
        try:
            with open(user_profile_path, 'r') as f:
                user_data = json.load(f)
                if not isinstance(user_data, list):
                    user_data = [user_data]
        except Exception as e:
            logger.error(f"Error reading user profile for owner {owner}: {e}")
            user_data = []  # Use empty array on error

        # Fetch matching user_storage_requests record
        async with config.db_pool.acquire() as conn:
            storage_request = await conn.fetchrow(
                """
                SELECT owner_account_id, file_name, total_replicas, last_charged_at, created_at,
                       miner_ids, selected_validator, is_assigned
                FROM user_storage_requests
                WHERE file_hash = $1 AND owner_account_id = $2
                """,
                main_req_hash, owner
            )
            if not storage_request:
                logger.warning(f"No user_storage_requests record found for main_req_hash {main_req_hash} and owner {owner}")

        # Calculate total file size and total files pinned
        total_file_size = 0
        total_files_pinned = 0
        updated_user_data = []
        updated_user_profile_data = []

        # Process existing user_data entries
        for entry in user_data:
            # Fetch file size for this file_hash
            file_size_response = await ipfs_utils.get_file_size(entry['file_hash'], config.IPFS_NODE_URL)
            file_size = file_size_response.get('size', 0)
            total_file_size += file_size if file_size else 0
            total_files_pinned += 1
            

            # Convert file_hash to byte array
            file_hash_hex = file_hash.encode('utf-8').hex()
            file_hash_bytes = bytes.fromhex(file_hash_hex)  # convert hex to bytes
            file_hash_vec = list(file_hash_bytes)  # convert bytes to list of integers 

            # Encode main_req_hash
            entry_main_req_hash = entry.get('main_req_hash')
            main_req_hash_encoded = entry_main_req_hash.encode('utf-8').hex() if entry_main_req_hash else None

            # Update the entry
            updated_entry = {
                "created_at": entry['created_at'],
                "file_hash": file_hash_vec,
                "file_name": entry['file_name'],
                "file_size_in_bytes": file_size if file_size else entry.get('file_size_in_bytes', 0),
                "is_assigned": entry['is_assigned'],
                "last_charged_at": entry['last_charged_at'],
                "main_req_hash": main_req_hash_encoded,
                "miner_ids": selected_miners,
                "owner": entry['owner'],
                "selected_validator": entry['selected_validator'],
                "total_replicas": entry['total_replicas']
            }
            updated_user_data.append(updated_entry)
            updated_user_profile_data.append(entry)

        # Add new entry from user_storage_requests if found
        if storage_request:
            logger.info("Adding new entry from storage request")
            # Fetch file size for the processed request's file_hash
            file_size_response = await ipfs_utils.get_file_size(file_hash, config.IPFS_NODE_URL)
            file_size = file_size_response.get('size', 0)
            total_file_size += file_size if file_size else 0
            total_files_pinned += 1

            # Convert file_hash to byte array
            file_hash_hex = file_hash.encode('utf-8').hex()
            file_hash_bytes = bytes.fromhex(file_hash_hex)  # convert hex to bytes
            file_hash_vec = list(file_hash_bytes)  # convert bytes to list of integers 

            # Encode main_req_hash
            processed_main_req_hash_encoded = main_req_hash.encode('utf-8').hex() if main_req_hash else None

            # Create new entry
            new_entry = {
                "created_at": storage_request['created_at'],
                "file_hash": file_hash_vec,
                "file_name": storage_request['file_name'],
                "file_size_in_bytes": file_size if file_size else 0,
                "is_assigned": storage_request['is_assigned'],
                "last_charged_at": storage_request['last_charged_at'],
                "main_req_hash": processed_main_req_hash_encoded,
                "miner_ids": selected_miners,
                "owner": storage_request['owner_account_id'],
                "selected_validator": storage_request['selected_validator'],
                "total_replicas": storage_request['total_replicas']
            }
            new_file_entry = {
                "created_at": storage_request['created_at'],
                "file_hash": file_hash,
                "file_name": storage_request['file_name'],
                "file_size_in_bytes": file_size if file_size else 0,
                "is_assigned": storage_request['is_assigned'],
                "last_charged_at": storage_request['last_charged_at'],
                "main_req_hash": main_req_hash,
                "miner_ids": selected_miners,
                "owner": storage_request['owner_account_id'],
                "selected_validator": storage_request['selected_validator'],
                "total_replicas": storage_request['total_replicas']
            }
            updated_user_data.append(new_entry)
            updated_user_profile_data.append(new_file_entry)

            # Update miner profile JSON files for each selected miner
            miner_profile_dir = os.path.join("profiles", "miner_profile")
            os.makedirs(miner_profile_dir, exist_ok=True)
            for miner_id in selected_miners:
                miner_profile_path = os.path.join(miner_profile_dir, f"{miner_id}.json")
                miner_data = []
                try:
                    with open(miner_profile_path, 'r') as f:
                        miner_data = json.load(f)
                        if not isinstance(miner_data, list):
                            miner_data = [miner_data]
                except FileNotFoundError:
                    logger.info(f"Miner profile not found, creating new: {miner_profile_path}")
                    miner_data = []
                except Exception as e:
                    logger.error(f"Error reading miner profile for miner {miner_id}: {e}")
                    miner_data = []

                # Create new entry for miner profile
                miner_entry = {
                    "created_at": storage_request['created_at'],
                    "file_hash": request['file_hash'],
                    "file_size_in_bytes": file_size if file_size else 0,
                    "miner_node_id": miner_id,
                    "selected_validator": storage_request['selected_validator']
                }
                miner_data.append(miner_entry)

                # Write updated miner profile so it submits minerprofile updated
                try:
                    with open(miner_profile_path, 'w') as f:
                        json.dump(miner_data, f, indent=4)
                    logger.info(f"Updated miner profile for miner {miner_id}: {miner_profile_path}")
                except Exception as e:
                    logger.error(f"Error writing miner profile for miner {miner_id}: {e}")
        else:
            logger.warning(f"No matching user_storage_requests record found for main_req_hash {main_req_hash} and owner {owner}. Cannot create new profile entry for {file_hash}.")

        logger.info(f"Preparing to upload {len(updated_user_data)} entries to IPFS for owner {owner}")
        logger.info(f"entries are {updated_user_data}")
        # Pin the updated user profile to IPFS
        pin_response = await ipfs_utils.upload_json_to_ipfs(data=updated_user_data, api_url=config.IPFS_NODE_URL)
        if not pin_response['success']:
            logger.error(f"Failed to pin updated user profile for owner {owner}: {pin_response['error']}")
            continue

        user_profile_cid = pin_response['cid']
        logger.info(f"Pinned updated user profile for owner {owner} to CID: {user_profile_cid}")

        processed_main_req_hash_encoded = main_req_hash.encode('utf-8').hex() if main_req_hash else None
        # Construct the parameter for call_update_pin_and_storage_requests
        pin_request = [
            {
                "storage_request_owner": owner,
                "storage_request_file_hash": processed_main_req_hash_encoded,
                "file_size": total_file_size,
                "user_profile_cid": user_profile_cid,
                "total_files_pinned": total_files_pinned
            }
        ]

        # Call the chain function
        logger.info(f"Submitting update_pin_and_storage_requests for : {pin_request}...")
        pin_success = await call_update_pin_and_storage_requests(pin_request)
        logger.info(f"update_pin_and_storage_requests {'succeeded' if pin_success else 'failed'} for owner {owner}")

        # If the transaction was successful, delete the processed request from pending_pool
        if pin_success:
            async with config.db_pool.acquire() as conn:
                await conn.execute(
                    """
                    DELETE FROM pending_pool
                    WHERE owner = $1 AND file_hash = $2
                    """,
                    owner,
                    file_hash
                )
                logger.info(f"Deleted processed request from pending_pool: owner={owner}, file_hash={file_hash}")

                # Delete from user_storage_requests
                await conn.execute(
                    """
                    DELETE FROM user_storage_requests
                    WHERE owner_account_id = $1 AND file_hash = $2
                    """,
                    owner,
                    main_req_hash
                )
                logger.info(f"Deleted record from user_storage_requests: owner={owner}, file_hash={main_req_hash}")
        else:
            logger.warning(f"Transaction failed, retaining processed request in pending_pool: owner={owner}, file_hash={file_hash}")
            
        # Update the local user profile file
        try:
            os.makedirs(os.path.dirname(user_profile_path), exist_ok=True)
            with open(user_profile_path, 'w') as f:
                json.dump(updated_user_profile_data, f, indent=4)
            logger.info(f"Updated user profile file with new CID: {user_profile_path}")
        except Exception as e:
            logger.error(f"Error writing updated user profile file {user_profile_path}: {e}")

    logger.info(f"Finished processing pin and storage updates at block {block_number}")

async def detect_offline_miners_at_epoch_start(pool: asyncpg.Pool):
    """Detect offline miners at the start of each epoch and log the result."""
    try:
        offline_miners = await get_offline_miners(pool)
        if offline_miners:
            logger.info(f"Offline miners detected at epoch start: {offline_miners}")
        else:
            logger.info("No offline miners detected at epoch start")
    except Exception as e:
        logger.error(f"Error detecting offline miners: {e}")

async def assign_to_storage_miners(block_number):
    """Processes pending storage requests by assigning them to 1 random miner and updating profiles."""
    profiles_dir = "profiles"
    miner_profile_dir = os.path.join(profiles_dir, "miner_profile")
    user_profile_dir = os.path.join(profiles_dir, "user_profile")

    async with config.db_pool.acquire() as conn:
        storage_miners_rows = await conn.fetch(
            """
            SELECT node_id FROM registration WHERE node_type ILIKE 'StorageMiner'
            """,
        )
        available_miners = [row['node_id'] for row in storage_miners_rows]
        logger.debug(f"Found registered StorageMiners with node_ids: {available_miners}")
        
        if not available_miners:
            logger.warning("No StorageMiners available from registration table. Skipping assignment action.")
            return

        # Fetch up to 10 pending requests, including file_name, selected_validator, and main_req_hash
        pending_requests = await conn.fetch(
            """
            SELECT owner, file_hash, file_name, selected_validator, main_req_hash
            FROM pending_pool
            WHERE status = $1
            LIMIT 10
            """,
            "pending"
        )
        if not pending_requests:
            logger.info("No pending requests to process.")
            return

        for request in pending_requests:
            owner = request['owner']
            file_hash = request['file_hash']
            file_name = request['file_name']
            selected_validator = request['selected_validator']
            main_req_hash = request['main_req_hash']

            # Select 1 random miner
            selected_miners = random.sample(available_miners, 1)
            logger.info(f"Selected miner for request {file_hash}: {selected_miners}")

            # Update miner_profile JSON
            miner_file_path = os.path.join(miner_profile_dir, f"{selected_miners[0]}.json")  # Use selected miner
            if os.path.exists(miner_file_path):
                try:
                    with open(miner_file_path, 'r') as f:
                        miner_data = json.load(f)
                        if not isinstance(miner_data, list):
                            miner_data = [miner_data]

                    # Find or create entry for this file_hash
                    entry_found = False
                    for entry in miner_data:
                        if entry.get('file_hash') == file_hash:
                            entry['miner_ids'] = selected_miners
                            entry['is_assigned'] = True
                            entry['selected_validator'] = selected_validator
                            entry_found = True
                            break
                    if not entry_found:
                        file_size_response = await ipfs_utils.get_file_size(file_hash, config.IPFS_NODE_URL, timeout=30)
                        miner_data.append({
                            "created_at": int(time.time()),
                            "file_hash": file_hash,
                            "file_size_in_bytes": file_size_response.get('size', 0),
                            "miner_node_id": selected_miners[0],
                            "selected_validator": selected_validator,
                            "miner_ids": selected_miners,
                            "is_assigned": True
                        })

                    with open(miner_file_path, 'w') as f:
                        json.dump(miner_data, f, indent=4)
                    logger.debug(f"Updated miner profile file: {miner_file_path}")
                except Exception as e:
                    logger.error(f"Error updating miner profile file {miner_file_path}: {e}")

            # Update user_profile JSON
            user_file_path = os.path.join(user_profile_dir, f"{owner}.json")
            if os.path.exists(user_file_path):
                try:
                    with open(user_file_path, 'r') as f:
                        user_data = json.load(f)
                        if not isinstance(user_data, list):
                            user_data = [user_data]

                    # Find or create entry for this file_hash
                    entry_found = False
                    for entry in user_data:
                        if entry.get('file_hash') == file_hash:
                            entry['is_assigned'] = True
                            entry['miner_ids'] = selected_miners
                            entry['selected_validator'] = selected_validator
                            entry['file_name'] = file_name
                            entry['main_req_hash'] = main_req_hash
                            entry_found = True
                            break
                    if not entry_found:
                        file_size_response = await ipfs_utils.get_file_size(file_hash, config.IPFS_NODE_URL, timeout=30)
                        user_data.append({
                            "created_at": int(time.time()),
                            "file_hash": file_hash,
                            "file_name": file_name,
                            "file_size_in_bytes": file_size_response.get('size', 0),
                            "is_assigned": True,
                            "last_charged_at": int(time.time()),
                            "main_req_hash": main_req_hash,
                            "miner_ids": selected_miners,
                            "owner": owner,
                            "selected_validator": selected_validator,
                            "total_replicas": 1
                        })

                    with open(user_file_path, 'w') as f:
                        json.dump(user_data, f, indent=4)
                    logger.debug(f"Updated user profile file: {user_file_path}")
                except Exception as e:
                    logger.error(f"Error updating user profile file {user_file_path}: {e}")

            # Update pending_pool status (mark as processed) and set selected_miners
            await conn.execute(
                """
                UPDATE pending_pool
                SET status = $1, selected_miners = $4
                WHERE owner = $2 AND file_hash = $3
                """,
                "processed",
                owner,
                file_hash,
                selected_miners
            )
            logger.info(f"Processed request for owner={owner}, file_hash={file_hash}, selected_miners={selected_miners}")

    logger.info(f"Performed action at block {block_number} (Processed {len(pending_requests)} requests)")

async def monitor_validator_epochs(pool):
    try:
        keypair = load_hips_keypair(config.KEYSTORE_PATH)
    except Exception as e:
        logger.error(f"CRITICAL: Failed to load HIPS keypair from {config.KEYSTORE_PATH}, monitor_validator_epochs cannot start. Error: {e}")
        return # Exit if keypair fails
    hips_account_id = keypair.ss58_address
    logger.info(f"Starting validator epoch monitor with HIPS account: {hips_account_id} at {time.strftime('%I:%M %p PKT, %B %d, %Y')}")

    in_action_period = False
    validator_term_start_block = None 
    validator_term_end_block = None   
    processing_cutoff_block = None # Will be set when term starts
    EPOCH_LENGTH = 100 
    SUBMISSION_GRACE_PERIOD_BLOCKS = 30 # Stop processing new requests this many blocks before term ends

    while True:
        logger.debug(f"monitor_validator_epochs loop start. in_action_period: {in_action_period}, term_end: {validator_term_end_block}, cutoff: {processing_cutoff_block}")
        current_block_number = await get_latest_block_number(pool)
        logger.info(f"Current block from DB: {current_block_number}")
        
        if current_block_number is None:
            logger.warning("Cannot proceed: current_block_number is None. Retrying in 5 seconds...")
            await asyncio.sleep(5)
            continue

        if in_action_period:
            logger.debug(f"In action period. Current: {current_block_number}, Term ends: {validator_term_end_block}, Cutoff: {processing_cutoff_block}")
            if current_block_number > validator_term_end_block:
                logger.info(f"Validator term officially ended. Current: {current_block_number}, Term End: {validator_term_end_block}")
                in_action_period = False
                validator_term_start_block = None
                validator_term_end_block = None
                processing_cutoff_block = None
            else:
                # Only perform actions if before the processing cutoff
                if processing_cutoff_block is not None and current_block_number <= processing_cutoff_block:
                    logger.info(f"Block {current_block_number}: Performing actions (before cutoff {processing_cutoff_block}).")
                    await perform_action(current_block_number)
                elif processing_cutoff_block is not None and current_block_number > processing_cutoff_block:
                    logger.info(f"Block {current_block_number}: Past processing cutoff {processing_cutoff_block}. Focusing on submissions.")
                
                # Submission logic (last 5 blocks of 100-block epoch)
                current_epoch_start_calc = ((current_block_number - 1) // EPOCH_LENGTH) * EPOCH_LENGTH + 1
                current_epoch_end_calc = current_epoch_start_calc + EPOCH_LENGTH - 1
                submission_window_start = current_epoch_end_calc - 4 
                submission_window_end = current_epoch_end_calc     
                logger.debug(f"Block: {current_block_number}, Term: [{validator_term_start_block}-{validator_term_end_block}], Epoch: [{current_epoch_start_calc}-{current_epoch_end_calc}], SubmitWin: [{submission_window_start}-{submission_window_end}]")
                # if submission_window_start <= current_block_number <= submission_window_end:
                if current_block_number % 50 == 0:
                    logger.info(f"Block {current_block_number} in submission window [{submission_window_start}-{submission_window_end}]. Triggering.")
                    await update_pin_and_storage_requests_near_epoch_end(current_block_number)
                    await update_miner_profiles_near_epoch_end(current_block_number)
            await asyncio.sleep(5) 
            continue

        # If not in an action period, check if we should become the validator
        logger.info("Not in action period. Checking DB for current validator...")
        async with pool.acquire() as conn:
            logger.debug("Querying current_epoch_validator table...")
            validator_info_row = await conn.fetchrow(
                """
                SELECT account_id, block_number 
                FROM current_epoch_validator
                ORDER BY updated_at DESC
                LIMIT 1
                """
            )
            logger.debug(f"DB query result for validator_info_row: {validator_info_row}")

            if validator_info_row: 
                db_validator_account_id = validator_info_row['account_id']
                db_validator_term_start_block = validator_info_row['block_number']
                logger.info(f"Comparing HIPS ID: '{hips_account_id}' with DB Validator ID: '{db_validator_account_id}' (term starts: {db_validator_term_start_block})")
                if db_validator_account_id == hips_account_id:
                    if not validator_term_start_block or validator_term_start_block != db_validator_term_start_block:
                        in_action_period = True
                        validator_term_start_block = db_validator_term_start_block
                        term_epoch_start_block = ((validator_term_start_block - 1) // EPOCH_LENGTH) * EPOCH_LENGTH + 1
                        validator_term_end_block = term_epoch_start_block + EPOCH_LENGTH - 1 
                        processing_cutoff_block = validator_term_end_block - SUBMISSION_GRACE_PERIOD_BLOCKS
                        logger.info(f"MATCH: HIPS account is current validator. Assigned for epoch starting ~{term_epoch_start_block}. Term active until block {validator_term_end_block}. Processing cutoff at {processing_cutoff_block}.")
                        await perform_rebalance_and_reconstruct_profiles(pool)
                        if current_block_number <= processing_cutoff_block: # Perform initial action only if not past cutoff
                             await perform_action(current_block_number)
                        else:
                            logger.info(f"Matched validator term, but already past processing cutoff ({current_block_number} > {processing_cutoff_block}). Skipping initial perform_action.")
                else:
                    logger.info(f"NO MATCH: HIPS ID '{hips_account_id}' does not match DB Validator ID '{db_validator_account_id}'. Awaiting turn.")
                    # Reset in_action_period if it was somehow true but IDs don't match
                    if in_action_period: 
                        logger.warning(f"State inconsistency: Was in_action_period=true, but HIPS ID does not match DB validator. Resetting.")
                        in_action_period = False
                        validator_term_start_block = None
                        validator_term_end_block = None
            else:
                logger.info("No entries found in current_epoch_validator table. Cannot determine current validator.")
        
        await asyncio.sleep(15) 

async def update_miner_profiles_near_epoch_end(block_number):
    """Updates miner profiles 5 blocks before epoch end and submits to chain."""
    logger.info(f"Updating miner profiles at block {block_number}...")

    miner_profile_dir = os.path.join("profiles", "miner_profile")
    if not os.path.exists(miner_profile_dir):
        logger.warning(f"Miner profile directory not found: {miner_profile_dir}")
        return
    logger.info("found miner profile dir ...")
    # List to store miner profile data for the chain function
    miner_profiles = []

    # Iterate through all JSON files in the miner profile directory
    for filename in os.listdir(miner_profile_dir):
        if not filename.endswith('.json'):
            logger.info("miner profile is not a json...")
            continue

        logger.info("miner profile is not a json...")
        miner_file_path = os.path.join(miner_profile_dir, filename)
        try:
            with open(miner_file_path, 'r') as f:
                logger.info("miner profile opened...")
                miner_data = json.load(f)
                if not isinstance(miner_data, list):
                    miner_data = [miner_data]
                    logger.info("miner profile is a list...")
        except Exception as e:
            logger.info("miner profile was empty...")
            miner_data = []

        # Skip if the miner data is empty
        if not miner_data:
            logger.info(f"Empty miner profile file: {miner_file_path}")
            continue

        # Track total file size and file count for this miner
        total_file_size = 0
        total_files_pinned = 0

        # Update each item in the miner profile
        updated_miner_data = []
        for entry in miner_data:
            file_hash = entry['file_hash']
            # Encode file_hash to byte array
            file_hash_hex = file_hash.encode('utf-8').hex()
            file_hash_bytes = bytes.fromhex(file_hash_hex)  # convert hex to bytes
            file_hash_vec = list(file_hash_bytes)  # convert bytes to list of integers 

            # Update totals
            file_size = entry.get('file_size_in_bytes', 0)
            total_file_size += file_size if file_size else 0
            total_files_pinned += 1

            # Create updated entry
            updated_entry = {
                "created_at": entry['created_at'],
                "file_hash": file_hash_vec,
                "file_size_in_bytes": file_size,
                "miner_node_id": entry['miner_node_id'],
                "selected_validator": entry['selected_validator']
            }
            updated_miner_data.append(updated_entry)
            logger.info(f"updated miner entry is {updated_entry}...")

        # Pin the updated miner profile to IPFS
        logger.info(f"trying to submit for getting json  tx now : {updated_miner_data}...")
        pin_response = await ipfs_utils.upload_json_to_ipfs(data=updated_miner_data, api_url=config.IPFS_NODE_URL)
        logger.info(f"pin_response : {pin_response}")
        if not pin_response['success']:
            logger.error(f"Failed to pin updated miner profile for {filename}: {pin_response['error']}")
            continue

        new_cid = pin_response['cid']
        logger.info(f"Pinned updated miner profile for {filename} to CID: {new_cid}")

        # Add to miner_profiles list
        miner_node_id = updated_miner_data[0]['miner_node_id']  # Assuming all entries have the same miner_node_id
        miner_profiles.append({
            "miner_node_id": miner_node_id,
            "cid": new_cid,
            "files_count": total_files_pinned,
            "files_size": total_file_size
        })

        logger.info(f"updated miner profile file  : {new_cid}") 
        # Optionally, update the local file with the new data
        try:
            with open(miner_file_path, 'w') as f:
                json.dump(updated_miner_data, f, indent=4)
            logger.debug(f"Updated miner profile file: {miner_file_path}")
        except Exception as e:
            logger.error(f"Error writing updated miner profile file {miner_file_path}: {e}")

    # Call the chain function if there are profiles to submit
    if miner_profiles:
        logger.info(f"Submitting update_miner_profiles with {len(miner_profiles)} profiles...")
        logger.info(f"trying to submit for updating profile now : {miner_profiles}...")
        success = await call_update_miner_profiles(miner_profiles)
        logger.info(f"update_miner_profiles {'succeeded' if success else 'failed'}")
    else:
        logger.info("No miner profiles to submit.")

    logger.info(f"Finished updating miner profiles at block {block_number}")

async def detect_offline_miners_at_epoch_start(pool: asyncpg.Pool):
    """Detect offline miners at the start of each epoch and log the result."""
    try:
        offline_miners = await get_offline_miners(pool)
        if offline_miners:
            logger.info(f"Offline miners detected at epoch start: {offline_miners}")
        else:
            logger.info("No offline miners detected at epoch start")
    except Exception as e:
        logger.error(f"Error detecting offline miners: {e}")

async def perform_rebalance_and_reconstruct_profiles(pool: asyncpg.Pool):
    """Orchestrates epoch tasks: detects offline miners, reconstructs profiles, and processes pending requests."""
    await detect_offline_miners_at_epoch_start(pool)
    await reconstruct_profiles_to_json(pool)

    profiles_dir = "profiles"
    miner_profile_dir = os.path.join(profiles_dir, "miner_profile")

    offline_miners = await get_offline_miners(pool)
    if not offline_miners:
        logger.info("No offline miners to process.")
        return

    async with pool.acquire() as conn:
        for miner in offline_miners:
            node_id = miner['node_id']
            miner_file_path = os.path.join(miner_profile_dir, f"{node_id}.json")

            if os.path.exists(miner_file_path):
                try:
                    with open(miner_file_path, 'r') as f:
                        profile_data = json.load(f)
                        if not isinstance(profile_data, list):
                            profile_data = [profile_data]

                    for entry in profile_data:
                        file_hash = entry.get('file_hash')
                        if file_hash:
                            # Fetch the owner, file_name, selected_validator, and main_req_hash from user_profile
                            user_info = await conn.fetchrow(
                                """
                                SELECT owner, file_name, selected_validator, main_req_hash 
                                FROM user_profile
                                WHERE file_hash = $1
                                LIMIT 1
                                """,
                                file_hash
                            )
                            if user_info:
                                owner = user_info['owner']
                                file_name = user_info['file_name']
                                selected_validator = user_info['selected_validator']
                                main_req_hash = user_info['main_req_hash']

                                # Check if the record already exists in pending_pool
                                exists = await conn.fetchval(
                                    """
                                    SELECT EXISTS (
                                        SELECT 1 FROM pending_pool WHERE owner = $1 AND file_hash = $2
                                    )""",
                                    owner, file_hash
                                )
                                if not exists:
                                    await conn.execute(
                                        """
                                        INSERT INTO pending_pool (owner, file_hash, file_name, selected_validator, main_req_hash, status, selected_miners)
                                        VALUES ($1, $2, $3, $4, $5, $6, $7)
                                        """,
                                        owner, file_hash, file_name, selected_validator, main_req_hash, "pending", []
                                    )
                                    logger.info(f"Added pending request for owner={owner}, file_hash={file_hash}, file_name={file_name}, main_req_hash={main_req_hash}, selected_miners=[] from offline miner {node_id}")
                                else:
                                    logger.debug(f"Pending request already exists for owner={owner}, file_hash={file_hash}")
                            else:
                                logger.warning(f"No user info found for file_hash={file_hash} in user_profile")
                        else:
                            logger.warning(f"No file_hash found in entry: {entry}")
                except Exception as e:
                    logger.error(f"Error processing miner profile for {node_id}: {e}")
            else:
                logger.info(f"No miner profile file found for offline miner: {node_id}")

    logger.info("Finished processing epoch tasks")

async def update_pin_check_metrics_near_block(block_number):
    """Updates pin check metrics every 1200th block and submits to chain."""
    logger.info(f"Checking pin check metrics at block {block_number}...")
    
    if block_number % 1200 != 0:
        logger.debug(f"Block {block_number} is not a 1200th block, skipping metric update")
        return

    # Fetch all miner metrics from miner_epoch_health
    async with config.db_pool.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT node_id, pin_check_successes, pin_check_failures
            FROM miner_epoch_health
            """
        )

    if not rows:
        logger.info("No miner pin check metrics found in miner_epoch_health")
        return

    # Collect metrics in an array
    metrics = []
    for row in rows:
        node_id = row['node_id']
        total_pin_checks = row['pin_check_successes'] + row['pin_check_failures']
        successful_pin_checks = row['pin_check_successes']
        metrics.append({
            "node_id": node_id,
            "total_pin_checks": total_pin_checks,
            "successful_pin_checks": successful_pin_checks
        })

    # Call the chain function if there are metrics to submit
    if metrics:
        logger.info(f"Submitting update_pin_check_metrics with {len(metrics)} miners...")
        success = await call_update_pin_check_metrics(metrics)
        logger.info(f"update_pin_check_metrics {'succeeded' if success else 'failed'}")
    else:
        logger.info("No pin check metrics to submit")

    logger.info(f"Finished processing pin check metrics at block {block_number}")