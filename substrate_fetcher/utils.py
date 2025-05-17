# substrate_fetcher/utils.py
import asyncio
import asyncpg
import json
import multiprocessing as mp
from queue import Empty as QueueEmptyException
import signal
import sys
import os
from typing import Any, Dict, List, Tuple
from . import config
import logging
import aiohttp
from multiprocessing import Queue as MPQueue
import os
import psutil
import time

# Ensure parent directory is in path so imports work from anywhere
script_path = os.path.abspath(os.path.dirname(__file__))
parent_dir = os.path.dirname(script_path)
if parent_dir not in sys.path:
    sys.path.insert(0, parent_dir)

# Configure logging
logger = logging.getLogger(__name__)

# Cache for IPFS content
_ipfs_manifest_cache: Dict[str, Any] = {}

# --- Database Configuration ---
async def create_db_pool():
    """Creates and returns a connection pool to the PostgreSQL database."""
    try:
        pool = await asyncpg.create_pool(config.DATABASE_URL)
        print("Successfully created database connection pool.")
        return pool
    except Exception as e:
        print(f"Failed to create database connection pool: {e}")
        raise

async def init_db(pool: asyncpg.Pool):
    """Initializes the database by creating necessary tables if they don't exist."""
    async with pool.acquire() as conn:
        # Create miners table
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS miners (
                node_id VARCHAR(100) PRIMARY KEY,
                ipfs_storage_max BIGINT,
                ipfs_zfs_pool_size BIGINT,
                last_online_block INTEGER,
                miner_profile_cid VARCHAR(255),
                successful_pin_checks INTEGER,
                total_pin_checks INTEGER,
                miner_total_files_size BIGINT,
                miner_total_files_pinned INTEGER,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """)

        # Create registration table
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS registration (
                node_id VARCHAR(100) PRIMARY KEY,
                ipfs_node_id VARCHAR(100) NOT NULL,
                node_type VARCHAR(100) NOT NULL,
                owner VARCHAR(100) NOT NULL,
                registered_at INTEGER NOT NULL,
                status VARCHAR(20) NOT NULL,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """)

        # Create miner_epoch_health table (for IPFS health service)
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS miner_epoch_health (
                node_id TEXT,
                ipfs_peer_id TEXT,
                epoch BIGINT,
                ping_successes INTEGER DEFAULT 0,
                ping_failures INTEGER DEFAULT 0,
                pin_check_successes INTEGER DEFAULT 0,
                pin_check_failures INTEGER DEFAULT 0,
                last_ping_attempt TIMESTAMP,
                last_ping_block BIGINT,
                last_pin_check_attempt TIMESTAMP,
                last_activity_at TIMESTAMP,
                PRIMARY KEY (node_id, epoch)
            );
        """)

        # Update the miner_profile table creation
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS miner_profile (
                entry_id SERIAL PRIMARY KEY,
                miner_node_id VARCHAR(100) NOT NULL,
                created_at INTEGER NOT NULL,
                file_hash VARCHAR(255) NOT NULL,
                file_size_in_bytes BIGINT NOT NULL,
                selected_validator VARCHAR(100) NOT NULL,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(miner_node_id, file_hash)  -- This ensures uniqueness for conflict detection
            );
        """)

        # Update the user_profile table creation (make main_req_hash nullable)
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS user_profile (
                entry_id SERIAL PRIMARY KEY,
                user_id VARCHAR(100) NOT NULL,
                created_at INTEGER NOT NULL,
                file_hash VARCHAR(255) NOT NULL,
                file_name VARCHAR(255) NOT NULL,
                file_size_in_bytes BIGINT NOT NULL,
                is_assigned BOOLEAN NOT NULL,
                last_charged_at INTEGER NOT NULL,
                main_req_hash VARCHAR(255),  -- Changed to allow NULL
                miner_ids TEXT[] NOT NULL,
                owner VARCHAR(100) NOT NULL,
                selected_validator VARCHAR(100) NOT NULL,
                total_replicas INTEGER NOT NULL,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(user_id, file_hash)  -- This ensures uniqueness for conflict detection
            );
        """)

        await conn.execute("""
            CREATE TABLE IF NOT EXISTS current_epoch_validator (
                id SERIAL PRIMARY KEY,
                account_id VARCHAR(100),
                block_number BIGINT,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """)
        # New table for UserStorageRequests
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS user_storage_requests (
                id SERIAL PRIMARY KEY,
                owner_account_id VARCHAR(100) NOT NULL,
                file_hash VARCHAR(350) NOT NULL,
                total_replicas INTEGER NOT NULL,
                file_name VARCHAR(350) NOT NULL,
                last_charged_at BIGINT NOT NULL,
                created_at BIGINT NOT NULL,
                miner_ids TEXT[],
                selected_validator VARCHAR(100) NOT NULL,
                is_assigned BOOLEAN NOT NULL,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(owner_account_id, file_hash)
            );
        """)

        # New table for storing the latest block number
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS latest_block (
                id SERIAL PRIMARY KEY,
                block_number BIGINT NOT NULL,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """)

        # Create pending_pool table if it doesn't exist
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS pending_pool (
                id SERIAL PRIMARY KEY,
                owner VARCHAR(100) NOT NULL,
                file_hash VARCHAR(350) NOT NULL,
                file_name VARCHAR(350),
                main_req_hash VARCHAR(350),
                selected_miners TEXT[],
                selected_validator VARCHAR(100),
                status VARCHAR(20) NOT NULL DEFAULT 'pending',
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(owner, file_hash)
            );
        """)
        print("Database tables initialized.")


# --- Database Operations for Fetcher ---
async def clear_table(pool: asyncpg.Pool, table_name: str):
    """Clears all data from the specified table."""
    async with pool.acquire() as conn:
        await conn.execute(f"DELETE FROM {table_name};")
        print(f"Cleared table: {table_name}")

async def update_execution_unit_metrics(pool: asyncpg.Pool, metrics_data: Dict[str, Dict[str, int]]):
    """
    Updates or inserts ExecutionUnit.NodeMetrics, MinerTotalFilesSize, and MinerTotalFilesPinned data into the miners table.
    If the node_id exists, updates the metrics; otherwise, inserts a new row.
    """
    async with pool.acquire() as conn:
        async with conn.transaction():
            for node_id, metrics in metrics_data.items():
                ipfs_storage_max = metrics.get("ipfs_storage_max", None)
                ipfs_zfs_pool_size = metrics.get("ipfs_zfs_pool_size", None)
                successful_pin_checks = metrics.get("successful_pin_checks", None)
                total_pin_checks = metrics.get("total_pin_checks", None)
                miner_total_files_size = metrics.get("miner_total_files_size", None)
                miner_total_files_pinned = metrics.get("miner_total_files_pinned", None)

                # Check if the node_id exists
                exists = await conn.fetchval(
                    "SELECT 1 FROM miners WHERE node_id = $1", node_id
                )

                if exists:
                    # Update existing row
                    await conn.execute(
                        """
                        UPDATE miners
                        SET ipfs_storage_max = $2,
                            ipfs_zfs_pool_size = $3,
                            successful_pin_checks = $4,
                            total_pin_checks = $5,
                            miner_total_files_size = $6,
                            miner_total_files_pinned = $7,
                            updated_at = CURRENT_TIMESTAMP
                        WHERE node_id = $1;
                        """,
                        node_id, ipfs_storage_max, ipfs_zfs_pool_size, successful_pin_checks, total_pin_checks,
                        miner_total_files_size, miner_total_files_pinned
                    )
                else:
                    # Insert new row (with only metrics for now)
                    await conn.execute(
                        """
                        INSERT INTO miners (node_id, ipfs_storage_max, ipfs_zfs_pool_size, successful_pin_checks, total_pin_checks,
                                          miner_total_files_size, miner_total_files_pinned)
                        VALUES ($1, $2, $3, $4, $5, $6, $7)
                        ON CONFLICT (node_id) DO UPDATE
                        SET ipfs_storage_max = EXCLUDED.ipfs_storage_max,
                            ipfs_zfs_pool_size = EXCLUDED.ipfs_zfs_pool_size,
                            successful_pin_checks = EXCLUDED.successful_pin_checks,
                            total_pin_checks = EXCLUDED.total_pin_checks,
                            miner_total_files_size = EXCLUDED.miner_total_files_size,
                            miner_total_files_pinned = EXCLUDED.miner_total_files_pinned,
                            updated_at = CURRENT_TIMESTAMP;
                        """,
                        node_id, ipfs_storage_max, ipfs_zfs_pool_size, successful_pin_checks, total_pin_checks,
                        miner_total_files_size, miner_total_files_pinned
                    )
                    print(f"Inserted new metrics for node_id: {node_id}")

async def save_miners_data(pool: asyncpg.Pool, block_numbers: Dict[str, Any], miner_profiles: Dict[str, Any]):
    """
    Saves BlockNumbers and MinerProfile data into the miners table.
    Deletes previous data for these fields (but preserves ExecutionUnit metrics).
    """
    async with pool.acquire() as conn:
        async with conn.transaction():
            # # Step 1: Clear previous block numbers and miner profiles (set to NULL)
            # await conn.execute(
            #     """
            #     UPDATE miners
            #     SET last_online_block = NULL,
            #         miner_profile_cid = NULL,
            #         updated_at = CURRENT_TIMESTAMP;
            #     """
            # )
            # print("Cleared previous BlockNumbers and MinerProfile data from miners table.")

            # Step 2: Batch insert or update BlockNumbers and MinerProfile data sequentially
            node_ids = set(list(block_numbers.keys()) + list(miner_profiles.keys()))
            for node_id in node_ids:
                last_online_block = None
                if node_id in block_numbers:
                    value = block_numbers[node_id]
                    if isinstance(value, list) and value:
                        last_online_block = value[0]  # Take the first block number if it's a list
                    elif isinstance(value, (int, str)):
                        last_online_block = int(value) if isinstance(value, str) else value
                    else:
                        print(f"Unexpected block_numbers value for {node_id}: {value}")
                miner_profile_cid = miner_profiles.get(node_id, None)

                try:
                    await conn.execute(
                        """
                        INSERT INTO miners (node_id, last_online_block, miner_profile_cid)
                        VALUES ($1, $2, $3)
                        ON CONFLICT (node_id) DO UPDATE
                        SET last_online_block = EXCLUDED.last_online_block,
                            miner_profile_cid = EXCLUDED.miner_profile_cid,
                            updated_at = CURRENT_TIMESTAMP;
                        """,
                        node_id, last_online_block, miner_profile_cid
                    )
                except Exception as e:
                    print(f"Error inserting/updating miners data for node_id {node_id}: {e}")

async def save_registration_data(pool: asyncpg.Pool, node_registration: Dict, coldkey_registration: Dict):
    """Saves NodeRegistration and ColdkeyNodeRegistration data into the registration table."""
    async with pool.acquire() as conn:
        async with conn.transaction():
            # Combine both registration datasets
            all_registrations = {**node_registration, **coldkey_registration}

            # Step 1: Process each registration entry
            for node_id, data in all_registrations.items():
                try:
                    # Validate required fields
                    required_fields = ["ipfs_node_id", "node_type", "owner", "registered_at", "status"]
                    if not all(field in data for field in required_fields):
                        missing = [f for f in required_fields if f not in data]
                        print(f"Skipping node_id {node_id}: Missing fields {missing}")
                        continue

                    ipfs_node_id = data["ipfs_node_id"]
                    node_type = data["node_type"]
                    owner = data["owner"]
                    registered_at = data["registered_at"]
                    status = data["status"]

                    await conn.execute(
                        """
                        INSERT INTO registration (node_id, ipfs_node_id, node_type, owner, registered_at, status)
                        VALUES ($1, $2, $3, $4, $5, $6)
                        ON CONFLICT (node_id) DO UPDATE
                        SET ipfs_node_id = EXCLUDED.ipfs_node_id,
                            node_type = EXCLUDED.node_type,
                            owner = EXCLUDED.owner,
                            registered_at = EXCLUDED.registered_at,
                            status = EXCLUDED.status,
                            updated_at = CURRENT_TIMESTAMP;
                        """,
                        node_id, ipfs_node_id, node_type, owner, registered_at, status
                    )
                except asyncpg.exceptions.PostgresError as e:
                    print(f"Database error saving registration data for node_id {node_id}: {e}")
                    raise
                except Exception as e:
                    print(f"Error saving registration data for node_id {node_id}: {e}")
                    raise

def _preprocess_json_content(content: Any) -> Any:
    """
    Preprocesses JSON content by decoding file_hash and main_req_hash, and cleaning CID strings.
    
    Args:
        content (Any): The JSON content to preprocess.
        
    Returns:
        Any: Preprocessed content.
    """
    # Handle lists (e.g., list of dictionaries)
    if isinstance(content, list):
        return [_preprocess_json_content(item) for item in content]
    
    # Handle dictionaries
    if isinstance(content, dict):
        processed_content = content.copy()
        # Check and decode file_name if it exists and is a list of integers
        if "file_name" in processed_content and isinstance(processed_content["file_name"], list):
            byte_list = processed_content["file_name"]
            if all(isinstance(x, int) for x in byte_list):
                try:
                    # Convert list of integers to bytes then to string
                    byte_data = bytes(byte_list)
                    decoded_str = byte_data.decode('utf-8')
                    processed_content["file_name"] = decoded_str
                except UnicodeDecodeError as e:
                    logger.warning(f"Failed to decode file_name as UTF-8: {e}. Keeping as byte array.")

        # Check and decode file_hash if it exists and is a list of integers
        if "file_hash" in processed_content and isinstance(processed_content["file_hash"], list):
            byte_list = processed_content["file_hash"]
            if all(isinstance(x, int) for x in byte_list):
                # Convert list of integers to bytes
                byte_data = bytes(byte_list)
                # Convert bytes to hex string as an intermediate step
                hex_str = byte_data.hex()
                try:
                    # Decode hex string to UTF-8
                    decoded_str = byte_data.decode('utf-8')
                    updated_decoded_str = bytes.fromhex(decoded_str).decode('utf-8')
                    processed_content["file_hash"] = updated_decoded_str
                except UnicodeDecodeError as e:
                    logger.warning(f"Failed to decode file_hash as UTF-8: {e}. Using hex string instead.")
                    processed_content["file_hash"] = hex_str  # Fallback to hex string if UTF-8 fails

        # Check and decode main_req_hash if it exists and is a hex string
        if "main_req_hash" in processed_content and isinstance(processed_content["main_req_hash"], str):
            hex_str = processed_content["main_req_hash"]
            try:
                # Decode hex string to UTF-8
                decoded_str = bytes.fromhex(hex_str).decode('utf-8')
                processed_content["main_req_hash"] = decoded_str
            except (ValueError, UnicodeDecodeError) as e:
                logger.error(f"Failed to decode main_req_hash for content: {e}")
                processed_content["main_req_hash"] = hex_str  # Fallback to hex string if decoding fails

        # Recursively process nested dictionaries
        for key, value in processed_content.items():
            processed_content[key] = _preprocess_json_content(value)
        return processed_content
    
    # Handle strings (e.g., for CID cleaning)
    if isinstance(content, str):
        # Clean CID strings by removing surrounding \" if present
        if content.startswith('"') and content.endswith('"'):
            cleaned_content = content.strip('"')
            return cleaned_content
    
    return content

async def fetch_cid_content(cid: str, session: aiohttp.ClientSession) -> Any | None:
    """
    Fetches the content of a CID from an IPFS gateway and returns it as JSON after preprocessing.
    
    Args:
        cid (str): The Content Identifier (e.g., "Qm...") to fetch.
        session (aiohttp.ClientSession): The aiohttp session to use for the request.
        
    Returns:
        Any | None: The preprocessed JSON content if successful, or None if failed.
    """
    if not cid or not config.IPFS_GATEWAY_URL:
        logger.error("CID or IPFS_GATEWAY_URL is empty")
        return None

    # Clean the CID by removing surrounding quotes if present
    cleaned_cid = cid.strip('"')
    if cleaned_cid != cid:
        logger.debug(f"Cleaned CID from {cid} to {cleaned_cid}")

    # Check cache first using the cleaned CID
    if cleaned_cid in _ipfs_manifest_cache:
        logger.debug(f"IPFS manifest CID {cleaned_cid} found in cache.")
        return _ipfs_manifest_cache[cleaned_cid]

    # Ensure no duplicate slashes if IPFS_GATEWAY_URL ends with one
    gateway_base = config.IPFS_GATEWAY_URL.rstrip('/')
    url = f"{gateway_base}/ipfs/{cleaned_cid}"
    logger.debug(f"Fetching IPFS content from: {url}")
    try:
        async with session.get(url, timeout=config.IPFS_TIMEOUT_SECONDS) as response:
            if response.status == 200:
                try:
                    # Try to parse as JSON directly
                    content = await response.json()
                    logger.debug(f"Successfully fetched and parsed content for CID {cleaned_cid}")
                except aiohttp.ContentTypeError:
                    # If not JSON, try reading text then parsing
                    logger.warning(f"IPFS content for {cleaned_cid} not directly JSON, trying text decode.")
                    text_content = await response.text()
                    try:
                        content = json.loads(text_content)
                        logger.debug(f"Successfully parsed text content as JSON for CID {cleaned_cid}")
                    except json.JSONDecodeError as je:
                        logger.error(f"Failed to parse IPFS text content as JSON for {cleaned_cid} from {url}: {je}. Content: {text_content[:200]}...")
                        return None
                except Exception as e:
                    logger.error(f"Error processing IPFS JSON response for {cleaned_cid} from {url}: {e}")
                    return None
            else:
                logger.error(f"IPFS gateway request for {cleaned_cid} failed with status {response.status}: {url}")
                return None
    except asyncio.TimeoutError:
        logger.error(f"IPFS gateway request timed out for {cleaned_cid} from {url}")
        return None
    except aiohttp.ClientError as e:
        logger.error(f"IPFS gateway client error for {cleaned_cid} from {url}: {e}")
        return None
    except Exception as e:
        logger.error(f"Unexpected error fetching IPFS content for {cleaned_cid} from {url}: {e}")
        return None

    # Preprocess the content before caching
    preprocessed_content = _preprocess_json_content(content)
    _ipfs_manifest_cache[cleaned_cid] = preprocessed_content  # Cache preprocessed content using cleaned CID
    return preprocessed_content

async def fetch_all_cid_contents_async(cid_pairs: List[Tuple[str, str]]) -> Dict[str, Any]:
    """
    Fetches content for specified CIDs asynchronously.
    
    Args:
        cid_pairs (List[Tuple[str, str]]): List of (node_id, cid) pairs to fetch.
        
    Returns:
        Dict[str, Any]: Dictionary mapping node IDs to their CID JSON content.
    """
    logger.debug(f"Fetching content for {len(cid_pairs)} CIDs")
    results = {}
    successful_cids = []
    failed_cids = []

    async with aiohttp.ClientSession() as session:
        tasks = []
        for node_id, cid in cid_pairs:
            tasks.append(fetch_cid_content(cid, session))
        
        # Wait for all tasks to complete
        responses = await asyncio.gather(*tasks, return_exceptions=True)
        
        # Map responses back to node_ids and track successes/failures
        for (node_id, cid), response in zip(cid_pairs, responses):
            results[node_id] = response if not isinstance(response, Exception) else None
            if response is None or isinstance(response, Exception):
                logger.warning(f"No content fetched for node_id {node_id} (CID: {cid})")
                failed_cids.append((node_id, cid))
            else:
                successful_cids.append((node_id, cid))
    
    if failed_cids:
        logger.info(f"Failed CIDs: {[f'{node_id}:{cid}' for node_id, cid in failed_cids]}")

    return results

async def save_ipfs_profiles(db_pool: asyncpg.Pool, ipfs_content: Dict[str, Any]):
    """Saves IPFS profiles to the database, skipping existing entries."""
    async with db_pool.acquire() as conn:
        success_count = 0
        skip_count = 0
        try:
            for node_id, profiles in ipfs_content.items():
                try:
                    # Ensure profiles is iterable
                    if not isinstance(profiles, (list, tuple)):
                        logger.warning(f"Skipping node {node_id}: profiles is not iterable")
                        skip_count += 1
                        continue

                    for profile in profiles:
                        if 'miner_node_id' in profile:
                            # MinerProfile
                            required_fields = ['miner_node_id', 'created_at', 'file_hash', 'file_size_in_bytes', 'selected_validator']
                            if not all(field in profile for field in required_fields):
                                missing = [field for field in required_fields if field not in profile]
                                logger.warning(f"Skipping MinerProfile entry for miner_node_id {node_id}: Missing fields {missing}")
                                skip_count += 1
                                continue
                            try:
                                result = await conn.execute(
                                    """
                                    INSERT INTO miner_profile (miner_node_id, created_at, file_hash, file_size_in_bytes, selected_validator)
                                    VALUES ($1, $2, $3, $4, $5)
                                    ON CONFLICT (miner_node_id, file_hash) DO NOTHING
                                    """,
                                    node_id,
                                    profile['created_at'],
                                    profile['file_hash'],
                                    profile['file_size_in_bytes'],
                                    profile['selected_validator']
                                )
                                if result == "INSERT 0 1":  # Successfully inserted
                                    logger.info(f"Inserted new MinerProfile entry for miner_node_id: {node_id}")
                                    success_count += 1
                                else:  # Conflict occurred, skipped
                                    logger.debug(f"Skipped existing MinerProfile entry for miner_node_id: {node_id}")
                                    skip_count += 1
                            except Exception as e:
                                logger.error(f"Error saving MinerProfile for {node_id}: {e}")
                                skip_count += 1
                                continue
                        elif 'owner' in profile:
                            # UserProfile
                            required_fields = ['owner', 'created_at', 'file_hash']
                            optional_fields = {
                                'file_name': None,
                                'file_size_in_bytes': 0,
                                'is_assigned': False,
                                'last_charged_at': None,
                                'main_req_hash': None,  # Allow NULL
                                'miner_ids': [],
                                'owner': None,
                                'selected_validator': None,
                                'total_replicas': 0
                            }
                            if not all(field in profile for field in required_fields):
                                missing = [field for field in required_fields if field not in profile]
                                logger.warning(f"Skipping UserProfile entry for user_id {node_id}: Missing fields {missing}")
                                skip_count += 1
                                continue
                            try:
                                # Merge required and optional fields
                                profile_data = {**optional_fields, **profile}
                                result = await conn.execute(
                                    """
                                    INSERT INTO user_profile (
                                        user_id, created_at, file_hash, file_name, file_size_in_bytes,
                                        is_assigned, last_charged_at, main_req_hash, miner_ids,
                                        owner, selected_validator, total_replicas
                                    )
                                    VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
                                    ON CONFLICT (user_id, file_hash) DO NOTHING
                                    """,
                                    node_id,
                                    profile_data['created_at'],
                                    profile_data['file_hash'],
                                    profile_data['file_name'],
                                    profile_data['file_size_in_bytes'],
                                    profile_data['is_assigned'],
                                    profile_data['last_charged_at'],
                                    profile_data['main_req_hash'],
                                    profile_data['miner_ids'],
                                    profile_data['owner'],
                                    profile_data['selected_validator'],
                                    profile_data['total_replicas']
                                )
                                if result == "INSERT 0 1":  # Successfully inserted
                                    logger.info(f"Inserted new UserProfile entry for user_id: {node_id}")
                                    success_count += 1
                                else:  # Conflict occurred, skipped
                                    logger.debug(f"Skipped existing UserProfile entry for user_id: {node_id}")
                                    skip_count += 1
                            except Exception as e:
                                logger.error(f"Error saving UserProfile for {node_id}: {e}")
                                skip_count += 1
                                continue
                except Exception as e:
                    logger.error(f"Error processing profiles for node {node_id}: {e}")
                    skip_count += 1
                    continue
            logger.info(f"Profile Save Summary: {success_count} profiles inserted, {skip_count} skipped (existing or invalid)")
        except Exception as e:
            logger.error(f"Critical error in save_ipfs_profiles: {e}")
            raise

def ipfs_fetch_worker(queue: MPQueue, ipfs_content: mp.Manager().dict, event_queue: MPQueue = None):
    logging.basicConfig(
        level=logging.DEBUG,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    logger = logging.getLogger(__name__)
    logger.info("Starting IPFS fetch worker process")
    parent_pid = os.getppid()
    while True:
        try:
            if not psutil.pid_exists(parent_pid):
                logger.error("Parent process terminated, exiting IPFS fetch worker")
                break
            data = queue.get(timeout=10)
            if data is None:
                logger.info("Received sentinel value, stopping IPFS fetch worker")
                if event_queue:
                    event_queue.put("done")
                break
            cid_pairs, current_node_ids = data
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            try:
                new_content = loop.run_until_complete(fetch_all_cid_contents_async(cid_pairs))
            except Exception as e:
                logger.error(f"Error in async fetch: {e}")
                new_content = {}
            finally:
                loop.close()
            ipfs_content.update(new_content)
            current_node_ids_set = set(current_node_ids)
            for k in list(ipfs_content.keys()):
                if k not in current_node_ids_set:
                    del ipfs_content[k]
            if event_queue:
                event_queue.put("done")
        except QueueEmptyException:
            time.sleep(1)
        except Exception as e:
            logger.error(f"Error in IPFS fetch worker: {e}")
            if event_queue:
                try:
                    event_queue.put("done")
                except Exception as e2:
                    logger.error(f"Failed to signal completion: {e2}")
            time.sleep(1)
    logger.info("IPFS fetch worker process exiting")

# --- Utility Functions ---
def get_storage_key_string(module: str, storage_item: str, params: Any = None) -> str:
    """Generates a string representation of a storage key for consistent JSON keys."""
    if params is None:
        return f"{module}.{storage_item}"
    if isinstance(params, (str, int)):
        return f"{module}.{storage_item}({params})"
    if isinstance(params, bytes):
        return f"{module}.{storage_item}(0x{params.hex()})"
    if isinstance(params, (list, tuple)):
        params_str = ",".join([f"0x{p.hex()}" if isinstance(p, bytes) else str(p) for p in params])
        return f"{module}.{storage_item}({params_str})"
    return f"{module}.{storage_item}({str(params)})"

async def save_current_epoch_validator(db_pool, account_id, block_number):
    """Saves or updates the current epoch validator in database, replacing all existing rows."""
    async with db_pool.acquire() as conn:
        try:
            # Delete all existing rows
            await conn.execute("DELETE FROM current_epoch_validator")
            # Insert new row
            await conn.execute(
                """
                INSERT INTO current_epoch_validator (account_id, block_number, updated_at)
                VALUES ($1, $2, NOW())
                """,
                account_id,
                block_number
            )
            logger.info(f"Updated CurrentEpochValidator: {account_id} at block {block_number}")
        except Exception as e:
            logger.error(f"Error saving CurrentEpochValidator: {e}")
            raise

async def save_user_storage_requests(pool: asyncpg.Pool, requests: Dict[Tuple[str, str], Any]):
    """Saves UserStorageRequests data to the database, converting BoundedVec fields to strings."""
    async with pool.acquire() as conn:
        async with conn.transaction():
            # logger.info(f"Saving {requests} UserStorageRequests")
            for (owner_account_id, file_hash), request in requests.items():
                try:
                    if request is None:
                        logger.info(f"Skipping None request for owner {owner_account_id}, file_hash {file_hash}")
                        continue

                    required_fields = [
                        "total_replicas", "owner", "file_hash", "file_name",
                        "last_charged_at", "created_at", "selected_validator", "is_assigned"
                    ]
                    if not all(field in request for field in required_fields):
                        missing = [f for f in required_fields if f not in request]
                        logger.warning(f"Skipping UserStorageRequest for {owner_account_id}, {file_hash}: Missing fields {missing}")
                        continue

                    # Convert BoundedVec fields
                    file_hash_str = bounded_vec_to_string(file_hash)
                    file_name_str = bounded_vec_to_raw_string(request["file_name"])
                    miner_ids = request.get("miner_ids", None)
                    miner_ids_str = (
                        [bounded_vec_to_raw_string(miner_id) for miner_id in miner_ids]
                        if miner_ids is not None else []
                    )

                    await conn.execute(
                        """
                        INSERT INTO user_storage_requests (
                            owner_account_id, file_hash, total_replicas, file_name,
                            last_charged_at, created_at, miner_ids, selected_validator, is_assigned
                        )
                        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
                        ON CONFLICT (owner_account_id, file_hash) DO UPDATE
                        SET total_replicas = EXCLUDED.total_replicas,
                            file_name = EXCLUDED.file_name,
                            last_charged_at = EXCLUDED.last_charged_at,
                            created_at = EXCLUDED.created_at,
                            miner_ids = EXCLUDED.miner_ids,
                            selected_validator = EXCLUDED.selected_validator,
                            is_assigned = EXCLUDED.is_assigned,
                            updated_at = CURRENT_TIMESTAMP;
                        """,
                        owner_account_id,
                        file_hash_str,
                        int(request["total_replicas"]),
                        file_name_str,
                        int(request["last_charged_at"]),
                        int(request["created_at"]),
                        miner_ids_str,
                        str(request["selected_validator"]),
                        bool(request["is_assigned"])
                    )
                    # logger.debug(f"Saved UserStorageRequest for {owner_account_id}, {file_hash_str}")
                except Exception as e:
                    logger.error(f"Error saving UserStorageRequest for {owner_account_id}, {file_hash}: {e}")
                    continue
            logger.info(f"Saved {len(requests)} UserStorageRequests to database.")

def bounded_vec_to_string(bounded_vec: Any) -> str:
    """Converts a BoundedVec (list of integers, bytes, or hex string) to a UTF-8 string, with double-decoding for hex strings."""
    try:
        # Handle list/tuple of integers (BoundedVec as list of bytes)
        if isinstance(bounded_vec, (list, tuple)) and all(isinstance(x, int) for x in bounded_vec):
            logger.debug(f"BoundedVec is list of integers: {bounded_vec}")
            byte_data = bytes(bounded_vec)
            return byte_data.decode('utf-8')

        # Handle bytes directly
        elif isinstance(bounded_vec, bytes):
            logger.debug(f"BoundedVec is bytes: {bounded_vec}")
            return bounded_vec.decode('utf-8')

        # Handle string input
        elif isinstance(bounded_vec, str):
            logger.debug(f"BoundedVec is string: {bounded_vec}")
            try:
                return bytes.fromhex(bounded_vec).decode('utf-8')
            except ValueError:
                # logger.warning(f"Invalid hex string format: {bounded_vec}, returning as string")
                return bounded_vec

        # Other types: force str conversion first
        else:
            logger.debug(f"Unhandled type for BoundedVec: {type(bounded_vec)}. Attempting to convert to string.")
            str_data = str(bounded_vec)
            try:
                return bytes.fromhex(str_data).decode('utf-8')
            except (UnicodeDecodeError, ValueError) as e:
                logger.warning(f"Failed to decode BoundedVec as UTF-8 or process hex string: {e}. Returning string.")
                return str_data

    except Exception as e:
        logger.error(f"Error converting BoundedVec to string: {e}")
        return str(bounded_vec)

def bounded_vec_to_raw_string(bounded_vec: Any) -> str:
    """Converts any bounded_vec input to a plain string representation without decoding."""
    return str(bounded_vec)

async def save_latest_block(db_pool, block_number):
    """Saves or updates the latest block number in the database, replacing all existing rows."""
    async with db_pool.acquire() as conn:
        try:
            # Delete all existing rows
            await conn.execute("DELETE FROM latest_block")
            # Insert new row
            await conn.execute(
                """
                INSERT INTO latest_block (block_number, updated_at)
                VALUES ($1, NOW())
                """,
                block_number
            )
            logger.info(f"Updated LatestBlock: {block_number}")
        except Exception as e:
            logger.error(f"Error saving LatestBlock: {e}")
            raise
