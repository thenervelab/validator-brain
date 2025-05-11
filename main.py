import asyncio
import logging
import signal
import os
import json # For decoding user profile bytes if they are JSON
import aiohttp # Added for IPFS fetching
from typing import Any, Dict, Set, List, Optional # Added Optional

from sqlalchemy.ext.asyncio import AsyncSession

# Project imports
from config import SUBSTRATE_NODE_URL, IPFS_GATEWAY_URL, POLLING_INTERVAL_SECONDS, ECHO_SQL # Added IPFS_GATEWAY_URL, POLLING_INTERVAL_SECONDS, ECHO_SQL
from database.models import create_db_and_tables, AsyncSessionLocal, engine as db_engine, User, Miner
from database import crud
from indexing.substrate_fetcher import get_chain_head_hash, fetch_all_chain_data, _substrate_instance, get_block_hash_by_number, get_block_number_by_hash # Added new fetcher functions

# Setup logging
logging.basicConfig(level=logging.DEBUG if ECHO_SQL else logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

IPFS_TIMEOUT_SECONDS = 10 # Timeout for IPFS requests
LOOKBACK_WINDOW = int(os.getenv("LOOKBACK_WINDOW", "50")) # Number of blocks to re-process on restart
# Ensure LOOKBACK_WINDOW is not negative
if LOOKBACK_WINDOW < 0: LOOKBACK_WINDOW = 0 

# Cache for IPFS manifests within a single block processing run, to avoid re-fetching the same manifest CID multiple times.
# More advanced caching (e.g.,跨-block, persistent) could be added later if needed.
_ipfs_manifest_cache: Dict[str, List[Dict[Any, Any]] | None] = {}

def _convert_file_hash_to_cid_string(file_hash_input: Any) -> str | None:
    if not file_hash_input:
        return None
    
    cid_string = None
    if isinstance(file_hash_input, list): # Assuming list of ASCII/UTF-8 numbers
        try:
            cid_string = bytes(file_hash_input).decode('utf-8')
        except Exception as e:
            logger.error(f"Error converting file_hash_array {file_hash_input} to CID string: {e}")
            return None
    elif isinstance(file_hash_input, str):
        cid_string = file_hash_input # Already a string, possibly from str(key_obj) in fetcher
    else:
        logger.warning(f"Unexpected type for file_hash_input: {type(file_hash_input)}. Attempting str().")
        cid_string = str(file_hash_input)

    if not cid_string:
        return None
        
    cid_string = cid_string.strip('"\'') # Strip quotes just in case

    if (cid_string.startswith("Qm") and len(cid_string) == 46) or \
       (cid_string.startswith("ba") and len(cid_string) > 45) or \
       (cid_string.startswith("k5") and len(cid_string) > 45) or \
       (all(c in '0123456789abcdefABCDEF' for c in cid_string) and len(cid_string) > 40): # Hex CIDs
        return cid_string
    else:
        logger.warning(f"Decoded/validated file_hash does not appear to be a standard CID string: {cid_string}")
        return None

async def fetch_ipfs_json_content(cid: str, session: aiohttp.ClientSession) -> Any | None: # Return type Any
    """Fetches content from an IPFS gateway and parses it as JSON."""
    if not cid or not IPFS_GATEWAY_URL:
        return None
    
    if cid in _ipfs_manifest_cache: # Check cache first
        logger.debug(f"IPFS manifest CID {cid} found in cache.")
        return _ipfs_manifest_cache[cid]

    # Ensure no duplicate slashes if IPFS_GATEWAY_URL ends with one
    gateway_base = IPFS_GATEWAY_URL.rstrip('/')
    url = f"{gateway_base}/ipfs/{cid}"
    logger.debug(f"Fetching IPFS content from: {url}")
    try:
        async with session.get(url, timeout=IPFS_TIMEOUT_SECONDS) as response:
            if response.status == 200:
                try:
                    # Try to parse as JSON directly
                    content = await response.json()
                except aiohttp.ContentTypeError: # If not JSON, try reading text then parsing
                    logger.warning(f"IPFS content for {cid} not directly JSON, trying text decode.")
                    text_content = await response.text()
                    try:
                        content = json.loads(text_content)
                    except json.JSONDecodeError as je:
                        logger.error(f"Failed to parse IPFS text content as JSON for {cid} from {url}: {je}. Content: {text_content[:200]}...")
                        return None
                except Exception as e:
                    logger.error(f"Error processing IPFS JSON response for {cid} from {url}: {e}")
                    return None
            else:
                logger.error(f"IPFS gateway request for {cid} failed with status {response.status}: {url}")
                return None
    except asyncio.TimeoutError:
        logger.error(f"IPFS gateway request timed out for {cid} from {url}")
        return None
    except aiohttp.ClientError as e:
        logger.error(f"IPFS gateway client error for {cid} from {url}: {e}")
        return None
    except Exception as e:
        logger.error(f"Unexpected error fetching IPFS content for {cid} from {url}: {e}")
        return None

    _ipfs_manifest_cache[cid] = content # Cache result, even if None (to avoid re-fetching failures quickly)
    return content

async def process_block_data(db: AsyncSession, block_hash: str, block_number: int, http_session: aiohttp.ClientSession):
    logger.info(f"Processing data for block: {block_number} ({block_hash})")
    _ipfs_manifest_cache.clear()
    chain_data = await fetch_all_chain_data(block_hash=block_hash)
    if not chain_data: logger.error(f"Failed to fetch data for block {block_hash}."); return False # Indicate failure

    user_owned_cids_map: Dict[str, Set[str]] = {}
    miner_assigned_cids_map: Dict[str, Set[str]] = {}
    all_cids_to_ensure_exist: Set[str] = set()
    active_user_ids_from_chain = set()
    raw_user_profiles = chain_data.get('user_profiles', [])
    logger.info(f"Found {len(raw_user_profiles)} raw user profile entries.")
    for user_profile_entry in raw_user_profiles:
        user_id = user_profile_entry.get('user_id_bytes')
        new_profile_manifest_cid = user_profile_entry.get('profile_cid')
        if not user_id: continue
        active_user_ids_from_chain.add(user_id)
        existing_user_in_db = await crud.get_user_by_id(db, user_id)
        old_profile_manifest_cid = existing_user_in_db.profile_cid if existing_user_in_db else None
        await crud.upsert_user(db, user_id=user_id, profile_cid_str=new_profile_manifest_cid)
        user_owned_cids_map.setdefault(user_id, set())
        if new_profile_manifest_cid:
            if new_profile_manifest_cid != old_profile_manifest_cid or not existing_user_in_db or new_profile_manifest_cid not in _ipfs_manifest_cache:
                logger.info(f"User {user_id} manifest CID {new_profile_manifest_cid}: new, changed, or not in session cache. Fetching from IPFS...")
                user_manifest_files = await fetch_ipfs_json_content(new_profile_manifest_cid, http_session)
                if isinstance(user_manifest_files, list):
                    logger.info(f"User {user_id} manifest {new_profile_manifest_cid} parsed. Files: {len(user_manifest_files)}")
                    for file_item in user_manifest_files:
                        if not isinstance(file_item, dict): continue
                        actual_file_cid = _convert_file_hash_to_cid_string(file_item.get('file_hash'))
                        if actual_file_cid:
                            all_cids_to_ensure_exist.add(actual_file_cid)
                            user_owned_cids_map[user_id].add(actual_file_cid)
                            assigned_miner_ids = file_item.get('miner_ids', [])
                            if isinstance(assigned_miner_ids, list):
                                for m_id in assigned_miner_ids:
                                    if m_id and isinstance(m_id, str):
                                        miner_assigned_cids_map.setdefault(m_id, set()).add(actual_file_cid)
                elif new_profile_manifest_cid: logger.error(f"User {user_id} manifest {new_profile_manifest_cid}: failed to fetch/parse.")
            else:
                logger.debug(f"User {user_id} manifest CID {new_profile_manifest_cid}: unchanged. Re-populating maps from cache/previous state is implicitly handled if manifest content was already processed and cached this run.")
    # Process UserStorageRequests 
    # (Assuming logic as before, depends on your pallet's design for how these interact with user profile manifests)
    # ... (UserStorageRequests processing - can be added back if needed, ensuring it uses http_session for IPFS calls) ...
    for cid_str_to_create in all_cids_to_ensure_exist:
        await crud.get_or_create_content_item(db, cid_str_to_create)
    for user_id_processed, owned_cids_set in user_owned_cids_map.items():
        await crud.update_user_owned_cids(db, user_id_processed, list(owned_cids_set))
    await crud.deactivate_users_not_in_list(db, active_user_ids_from_chain)
    logger.info(f"Users processed. Active: {len(active_user_ids_from_chain)}.")

    raw_miner_profiles = chain_data.get('miner_profiles', [])
    miner_states_lookup = {state['miner_id_bytes']: state['state'] for state in chain_data.get('miner_states', [])}
    per_miner_pinned_lookup = {item['miner_id_bytes']: item['total_files_pinned'] for item in chain_data.get('per_miner_total_files_pinned', [])}
    per_miner_size_lookup = {item['miner_id_bytes']: item['total_files_size'] for item in chain_data.get('per_miner_total_files_size', [])}
    node_metrics_lookup = {item['miner_id_bytes']: item['metrics_data'] for item in chain_data.get('execution_node_metrics', [])}
    all_miner_ids_in_system = set(miner_assigned_cids_map.keys())
    for profile in raw_miner_profiles:
        if profile.get('miner_id_bytes'): all_miner_ids_in_system.add(profile.get('miner_id_bytes'))

    logger.info(f"Processing {len(all_miner_ids_in_system)} unique miners.")
    for miner_id in all_miner_ids_in_system:
        if not miner_id: continue
        profile_entry = next((p for p in raw_miner_profiles if p.get('miner_id_bytes') == miner_id), {})
        new_miner_profile_manifest_cid = profile_entry.get('profile_cid')
        existing_miner_in_db = await crud.get_miner_by_id(db, miner_id)
        old_miner_profile_manifest_cid = existing_miner_in_db.profile_cid if existing_miner_in_db else None
        miner_self_pinned_cids = set()
        if new_miner_profile_manifest_cid:
            if new_miner_profile_manifest_cid != old_miner_profile_manifest_cid or not existing_miner_in_db or new_miner_profile_manifest_cid not in _ipfs_manifest_cache:
                logger.info(f"Miner {miner_id} manifest CID {new_miner_profile_manifest_cid}: new, changed, or not in session cache. Fetching from IPFS...")
                miner_manifest_content = await fetch_ipfs_json_content(new_miner_profile_manifest_cid, http_session)
                if isinstance(miner_manifest_content, list):
                    logger.info(f"Miner {miner_id} manifest {new_miner_profile_manifest_cid} parsed. Files: {len(miner_manifest_content)}")
                    for file_item in miner_manifest_content:
                        if not isinstance(file_item, dict): continue
                        actual_file_cid = _convert_file_hash_to_cid_string(file_item.get('file_hash'))
                        if actual_file_cid:
                            all_cids_to_ensure_exist.add(actual_file_cid)
                            miner_self_pinned_cids.add(actual_file_cid)
                elif new_miner_profile_manifest_cid: logger.error(f"Miner {miner_id} manifest {new_miner_profile_manifest_cid}: failed to fetch/parse.")
            else:
                logger.debug(f"Miner {miner_id} manifest CID {new_miner_profile_manifest_cid}: unchanged. Implicitly using previous state for self-pinned CIDs.")
        metrics_data = node_metrics_lookup.get(miner_id)
        declared_capacity = None
        if metrics_data and isinstance(metrics_data, dict): # Ensure metrics_data is a dict
            declared_capacity = metrics_data.get('total_storage_bytes') 
            if declared_capacity is None:
                 logger.warning(f"Field 'total_storage_bytes' not found in metrics_data for miner {miner_id}: {metrics_data}")
            elif not isinstance(declared_capacity, int):
                 logger.warning(f"Field 'total_storage_bytes' for miner {miner_id} is not an integer: {declared_capacity}. Setting to None.")
                 declared_capacity = None # Or attempt conversion if it's a numeric string
        elif metrics_data: # metrics_data exists but is not a dict as expected
             logger.warning(f"metrics_data for miner {miner_id} is not a dictionary: {metrics_data}. Cannot extract declared capacity.")

        await crud.upsert_miner(
            db, miner_id=miner_id, profile_cid_str=new_miner_profile_manifest_cid, 
            state_data=miner_states_lookup.get(miner_id),
            total_files_pinned=per_miner_pinned_lookup.get(miner_id),
            total_files_size_bytes=per_miner_size_lookup.get(miner_id),
            declared_capacity_bytes=declared_capacity
        )
        final_miner_cids = list(miner_assigned_cids_map.get(miner_id, set()).union(miner_self_pinned_cids))
        await crud.update_miner_pinned_cids(db, miner_id, final_miner_cids)
    await crud.deactivate_miners_not_in_list(db, all_miner_ids_in_system)
    logger.info(f"Miners processed. Total unique active miners: {len(all_miner_ids_in_system)}.")
    logger.info(f"DB operations for block {block_hash} prepared.")
    return True # Indicate success

async def main_loop():
    logger.info("Starting IPFS Substrate Indexer...")
    await create_db_and_tables()
    
    start_block_number: Optional[int] = None
    async with AsyncSessionLocal() as db_session:
        indexer_state = await crud.get_indexer_state(db_session)
        if indexer_state and indexer_state.last_processed_block_number is not None:
            start_block_number = max(0, indexer_state.last_processed_block_number - LOOKBACK_WINDOW + 1)
            logger.info(f"Resuming from block number {start_block_number} (last processed: {indexer_state.last_processed_block_number}, lookback: {LOOKBACK_WINDOW})")
        else:
            logger.info("No previous indexer state found, or last processed block is null. Will start from current chain tip minus lookback.")

    last_processed_block_hash_for_run = None # Tracks last block hash processed in this run to avoid immediate re-processing if no new blocks yet
    if indexer_state and indexer_state.last_processed_block_hash:
        last_processed_block_hash_for_run = indexer_state.last_processed_block_hash

    running = True
    def shutdown_handler(signum, frame):
        nonlocal running
        logger.info(f"Shutdown signal ({signal.Signals(signum).name}) received. Stopping indexer gracefully...")
        running = False
    signal.signal(signal.SIGINT, shutdown_handler)
    signal.signal(signal.SIGTERM, shutdown_handler)

    async with aiohttp.ClientSession() as http_session:
        while running:
            try:
                current_tip_block_hash = await get_chain_head_hash()
                if not current_tip_block_hash: 
                    await asyncio.sleep(POLLING_INTERVAL_SECONDS); continue
                
                current_tip_block_number = await get_block_number_by_hash(current_tip_block_hash)
                if current_tip_block_number is None: 
                    logger.error(f"Could not get block number for tip hash {current_tip_block_hash}"); 
                    await asyncio.sleep(POLLING_INTERVAL_SECONDS); continue

                if start_block_number is None: # First run after no state, or state was null
                    start_block_number = max(0, current_tip_block_number - LOOKBACK_WINDOW)
                    logger.info(f"Initial start block number set to: {start_block_number}")
                
                block_to_process_number = start_block_number

                while block_to_process_number <= current_tip_block_number and running:
                    block_hash_to_process = await get_block_hash_by_number(block_to_process_number)
                    if not block_hash_to_process:
                        logger.error(f"Could not get block hash for number {block_to_process_number}. Skipping.")
                        block_to_process_number += 1
                        continue
                    
                    # Avoid reprocessing the same hash if it was the last one from previous run and no new blocks yet
                    if block_hash_to_process == last_processed_block_hash_for_run and block_to_process_number == start_block_number:
                        if block_to_process_number == current_tip_block_number: # Already at tip, nothing new
                             logger.debug(f"Block {block_to_process_number} ({block_hash_to_process}) was last processed and is still tip. Waiting for new blocks.")
                             break # Break from inner while, will go to outer sleep
                        else: # More blocks to catch up to
                            logger.info(f"Skipping block {block_to_process_number} as it was the last one processed in the previous session.")
                            block_to_process_number += 1
                            start_block_number = block_to_process_number # Adjust start for next iteration of outer loop
                            continue

                    async with AsyncSessionLocal() as db_session:
                        try:
                            success = await process_block_data(db_session, block_hash_to_process, block_to_process_number, http_session)
                            if success:
                                await crud.update_indexer_state(db_session, block_hash_to_process, block_to_process_number)
                                await db_session.commit()
                                logger.info(f"Successfully processed and committed data for block: {block_to_process_number} ({block_hash_to_process})")
                                last_processed_block_hash_for_run = block_hash_to_process
                                start_block_number = block_to_process_number + 1 # Advance for next iteration
                            else:
                                logger.error(f"Processing failed for block {block_to_process_number} ({block_hash_to_process}). Will retry on next cycle if applicable.")
                                # Potentially break or implement more specific retry for the block
                        except Exception as e:
                            logger.error(f"DB Error processing block {block_to_process_number} ({block_hash_to_process}): {e}", exc_info=True)
                            try: await db_session.rollback(); logger.info(f"DB session rolled back for block {block_hash_to_process}.")
                            except Exception as r_err: logger.error(f"Error during rollback for block {block_hash_to_process}: {r_err}", exc_info=True)
                    
                    if not running: break
                    block_to_process_number +=1
                
                if not running: logger.info("Shutdown signal during block processing, exiting main loop."); break
                
                # If we've caught up to the tip, set start_block_number for the next polling cycle
                if block_to_process_number > current_tip_block_number:
                    start_block_number = current_tip_block_number + 1

                logger.debug(f"Finished processing up to {current_tip_block_number}. Next poll will check from {start_block_number}.")
                await asyncio.sleep(POLLING_INTERVAL_SECONDS) 

            except ConnectionRefusedError:
                logger.error(f"Substrate node connection refused. Retrying in {POLLING_INTERVAL_SECONDS*3}s...")
                await asyncio.sleep(POLLING_INTERVAL_SECONDS * 3)
            except Exception as e:
                logger.error(f"Unexpected error in main loop: {e}", exc_info=True)
                await asyncio.sleep(POLLING_INTERVAL_SECONDS)
    logger.info("Indexer main loop has finished.")
    await shutdown_cleanup()

async def shutdown_cleanup():
    logger.info("Performing graceful shutdown cleanup...")
    if _substrate_instance and hasattr(_substrate_instance, 'close'):
        try: _substrate_instance.close(); logger.info("Substrate connection closed.")
        except Exception as e: logger.error(f"Error closing Substrate connection: {e}", exc_info=True)
    
    if db_engine:
        try: await db_engine.dispose(); logger.info("Database engine disposed.")
        except Exception as e: logger.error(f"Error disposing database engine: {e}", exc_info=True)
    logger.info("Cleanup complete. Indexer shut down.")

if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    try:
        loop.run_until_complete(main_loop())
    except KeyboardInterrupt:
        logger.info("Indexer stopped by user via KeyboardInterrupt.")
    finally:
        logger.info("Main execution scope finished. Ensuring final cleanup...")
        all_tasks = asyncio.all_tasks(loop) if not loop.is_closed() else set()
        cleanup_task_already_done = any(
            t.get_name() == shutdown_cleanup.__name__ or t.done() and t.exception() is None 
            for t in all_tasks if hasattr(t, 'get_coro') and t.get_coro().__name__ == shutdown_cleanup.__name__
        ) or not running

        if not cleanup_task_already_done:
            logger.info("Attempting to run shutdown_cleanup explicitly...")
            if loop.is_closed():
                loop = asyncio.new_event_loop()
                asyncio.set_event_loop(loop)
            try:
                loop.run_until_complete(shutdown_cleanup())
            except RuntimeError as e:
                if "Event loop is closed" in str(e) or "cannot schedule new futures after shutdown" in str(e):
                    logger.info(f"Event loop was closed or shutting down before final cleanup could run fully: {e}")
                else:
                    logger.error(f"RuntimeError during final explicit cleanup: {e}", exc_info=True)
            except Exception as e:
                logger.error(f"Unexpected error during final explicit cleanup: {e}", exc_info=True)
        else:
            logger.info("Cleanup task was already handled or loop exited normally.")
        
        if hasattr(loop, 'is_closed') and not loop.is_closed():
            loop.close()
    logger.info("Application has fully terminated.") 