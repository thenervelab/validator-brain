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

        # Create MinerProfile table with individual columns
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS miner_profile (
                entry_id SERIAL PRIMARY KEY,
                miner_node_id VARCHAR(100) NOT NULL,
                created_at INTEGER NOT NULL,
                file_hash VARCHAR(255) NOT NULL,
                file_size_in_bytes BIGINT NOT NULL,
                selected_validator VARCHAR(100) NOT NULL,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(miner_node_id, entry_id)
            );
        """)

        # Create UserProfile table with individual columns
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
                main_req_hash VARCHAR(255) NOT NULL,
                miner_ids TEXT[] NOT NULL,
                owner VARCHAR(100) NOT NULL,
                selected_validator VARCHAR(100) NOT NULL,
                total_replicas INTEGER NOT NULL,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(user_id, entry_id)
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
    Updates or inserts ExecutionUnit.NodeMetrics data into the miners table.
    If the node_id exists, updates the metrics; otherwise, inserts a new row.
    """
    async with pool.acquire() as conn:
        async with conn.transaction():
            for node_id, metrics in metrics_data.items():
                ipfs_storage_max = metrics.get("ipfs_storage_max", None)
                ipfs_zfs_pool_size = metrics.get("ipfs_zfs_pool_size", None)

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
                            updated_at = CURRENT_TIMESTAMP
                        WHERE node_id = $1;
                        """,
                        node_id, ipfs_storage_max, ipfs_zfs_pool_size
                    )
                    print(f"Updated ExecutionUnit metrics for node_id: {node_id}")
                else:
                    # Insert new row (with only metrics for now)
                    await conn.execute(
                        """
                        INSERT INTO miners (node_id, ipfs_storage_max, ipfs_zfs_pool_size)
                        VALUES ($1, $2, $3)
                        ON CONFLICT (node_id) DO UPDATE
                        SET ipfs_storage_max = EXCLUDED.ipfs_storage_max,
                            ipfs_zfs_pool_size = EXCLUDED.ipfs_zfs_pool_size,
                            updated_at = CURRENT_TIMESTAMP;
                        """,
                        node_id, ipfs_storage_max, ipfs_zfs_pool_size
                    )
                    print(f"Inserted new ExecutionUnit metrics for node_id: {node_id}")

async def save_miners_data(pool: asyncpg.Pool, block_numbers: Dict[str, Any], miner_profiles: Dict[str, Any]):
    """
    Saves BlockNumbers and MinerProfile data into the miners table.
    Deletes previous data for these fields (but preserves ExecutionUnit metrics).
    """
    async with pool.acquire() as conn:
        async with conn.transaction():
            # Step 1: Clear previous block numbers and miner profiles (set to NULL)
            await conn.execute(
                """
                UPDATE miners
                SET last_online_block = NULL,
                    miner_profile_cid = NULL,
                    updated_at = CURRENT_TIMESTAMP;
                """
            )
            print("Cleared previous BlockNumbers and MinerProfile data from miners table.")

            # Step 2: Batch insert or update BlockNumbers and MinerProfile data sequentially
            node_ids = set(list(block_numbers.keys()) + list(miner_profiles.keys()))
            for node_id in node_ids:
                last_online_block = None
                if node_id in block_numbers:
                    value = block_numbers[node_id]
                    if isinstance(value, list) and value:
                        last_online_block = value[0]  # Take the first block number if it's a list
                    elif isinstance(value, (int, str)):  # Handle single value or unexpected type
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
                    print(f"Inserted/Updated miners data for node_id: {node_id}")
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
                    print(f"Saved registration data for node_id: {node_id}")
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

async def save_ipfs_profiles(pool: asyncpg.Pool, ipfs_content: Dict[str, Any]):
    """
    Saves MinerProfile and UserProfile contents to their respective database tables with individual columns,
    skipping failed CIDs individually.
    """
    logger.info(f"Starting save_ipfs_profiles with ipfs_content: {list(ipfs_content.keys())}")
    successful_saves = 0
    failed_saves = 0

    async with pool.acquire() as conn:
        async with conn.transaction():
            for node_id, content in ipfs_content.items():
                if content is None:
                    logger.warning(f"Skipping node_id {node_id}: Content is None (likely fetch failed).")
                    failed_saves += 1
                    continue
                if not isinstance(content, list):
                    logger.warning(f"Skipping node_id {node_id}: Content is not a list, got {type(content)}: {content}")
                    failed_saves += 1
                    continue

                # Determine if this is a MinerProfile or UserProfile based on structure
                if content and all("miner_node_id" in item for item in content):
                    # MinerProfile
                    logger.debug(f"Processing MinerProfile for node_id {node_id}")
                    # Delete existing entries for this miner_node_id
                    await conn.execute(
                        "DELETE FROM miner_profile WHERE miner_node_id = $1",
                        node_id
                    )
                    # Insert new entries
                    for entry in content:
                        try:
                            required_fields = ["created_at", "file_hash", "file_size_in_bytes", "miner_node_id", "selected_validator"]
                            if not all(field in entry for field in required_fields):
                                missing = [f for f in required_fields if f not in entry]
                                logger.warning(f"Skipping MinerProfile entry for miner_node_id {node_id}: Missing fields {missing}")
                                continue

                            await conn.execute(
                                """
                                INSERT INTO miner_profile (
                                    miner_node_id, created_at, file_hash, file_size_in_bytes, selected_validator
                                ) VALUES ($1, $2, $3, $4, $5)
                                """,
                                node_id,
                                entry["created_at"],
                                entry["file_hash"],
                                entry["file_size_in_bytes"],
                                entry["selected_validator"]
                            )
                            print(f"Saved MinerProfile entry for miner_node_id: {node_id}")
                            successful_saves += 1
                        except Exception as e:
                            logger.error(f"Error saving MinerProfile entry for miner_node_id {node_id}: {e}")
                            failed_saves += 1
                            continue  # Skip this entry and proceed to the next

                elif content and all("owner" in item for item in content):
                    # UserProfile
                    logger.debug(f"Processing UserProfile for node_id {node_id}")
                    # Delete existing entries for this user_id
                    await conn.execute(
                        "DELETE FROM user_profile WHERE user_id = $1",
                        node_id
                    )
                    # Insert new entries
                    for entry in content:
                        try:
                            required_fields = [
                                "created_at", "file_hash", "file_name", "file_size_in_bytes",
                                "is_assigned", "last_charged_at", "main_req_hash", "miner_ids",
                                "owner", "selected_validator", "total_replicas"
                            ]
                            if not all(field in entry for field in required_fields):
                                missing = [f for f in required_fields if f not in entry]
                                logger.warning(f"Skipping UserProfile entry for user_id {node_id}: Missing fields {missing}")
                                continue

                            await conn.execute(
                                """
                                INSERT INTO user_profile (
                                    user_id, created_at, file_hash, file_name, file_size_in_bytes,
                                    is_assigned, last_charged_at, main_req_hash, miner_ids,
                                    owner, selected_validator, total_replicas
                                ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
                                """,
                                node_id,
                                entry["created_at"],
                                entry["file_hash"],
                                entry["file_name"],
                                entry["file_size_in_bytes"],
                                entry["is_assigned"],
                                entry["last_charged_at"],
                                entry["main_req_hash"],
                                entry["miner_ids"],
                                entry["owner"],
                                entry["selected_validator"],
                                entry["total_replicas"]
                            )
                            print(f"Saved UserProfile entry for user_id: {node_id}")
                            successful_saves += 1
                        except Exception as e:
                            logger.error(f"Error saving UserProfile entry for user_id {node_id}: {e}")
                            failed_saves += 1
                            continue  # Skip this entry and proceed to the next
                else:
                    logger.warning(f"Skipping node_id {node_id}: Content does not match MinerProfile or UserProfile structure: {content}")
                    failed_saves += 1

    # Log summary of saves
    logger.info(f"Profile Save Summary: {successful_saves} profiles saved successfully, {failed_saves} failed or skipped")

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

import time  # Added for sleep in worker