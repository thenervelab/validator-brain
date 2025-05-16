import asyncio
import multiprocessing as mp
from substrateinterface import SubstrateInterface
from substrateinterface.exceptions import SubstrateRequestException
from typing import Any
import logging
import os
import sys
import time
import traceback
from queue import Empty as QueueEmptyException

# Ensure parent directory is in path so imports work from anywhere
script_path = os.path.abspath(os.path.dirname(__file__))
parent_dir = os.path.dirname(script_path)
if parent_dir not in sys.path:
    sys.path.insert(0, parent_dir)

from . import config
from . import utils

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# --- Module-level state ---
_substrate_instance: SubstrateInterface | None = None
_current_status = "Initializing"
_exit_event = asyncio.Event()
_last_connection_error = None
_connection_attempt_count = 0
_ipfs_fetch_process = None
_ipfs_fetch_queue = None
_ipfs_content = None
_previous_ipfs_profiles = {}
_latest_data = None  # Store latest block data

def get_status():
    return _current_status

def is_stopping():
    return _exit_event.is_set()

def get_latest_data():
    """Returns the latest block data."""
    return _latest_data

def _update_status(new_status: str):
    global _current_status
    _current_status = new_status
    logger.info(f"Status updated: {new_status}")

def _get_substrate_interface(force_reconnect=False) -> SubstrateInterface | None:
    """Initializes and returns a SubstrateInterface instance, with reconnection logic."""
    global _substrate_instance, _last_connection_error, _connection_attempt_count
    if _substrate_instance is None or force_reconnect:
        if _substrate_instance and force_reconnect:
            try:
                _substrate_instance.close()
            except Exception:
                pass
            _substrate_instance = None

        _update_status(f"Connecting to {config.NODE_URL}...")
        if _connection_attempt_count % 5 == 0 or str(_last_connection_error) != "Connecting":
            print(f"Attempting to connect to Substrate node: {config.NODE_URL} (Attempt {_connection_attempt_count + 1})")
        _connection_attempt_count += 1
        _last_connection_error = "Connecting"
        try:
            _substrate_instance = SubstrateInterface(url=config.NODE_URL, type_registry=config.TYPE_REGISTRY if 'TYPE_REGISTRY' in dir(config) else None)
            genesis_hash = _substrate_instance.get_block_hash(0)
            if not genesis_hash:
                raise ConnectionError("Failed to retrieve genesis hash.")
            block_number = _substrate_instance.query('System', 'Number').value
            if block_number is None:
                raise ConnectionError("Failed to retrieve current block number.")
            print(f"Successfully connected to node: Block #{block_number} (Genesis: {genesis_hash})")
            _update_status("Connected")
            _last_connection_error = None
            _connection_attempt_count = 0
        except ConnectionRefusedError as e:
            if str(e) != str(_last_connection_error):
                print(f"⚠️ Connection refused: Ensure the Substrate node is running at {config.NODE_URL}.")
                _last_connection_error = str(e)
            _update_status("Connection Refused")
            _substrate_instance = None
        except SubstrateRequestException as e:
            if str(e) != str(_last_connection_error):
                print(f"⚠️ Substrate request error during connection: {e}")
                _last_connection_error = str(e)
            _update_status(f"Connection Substrate Error: {e}")
            _substrate_instance = None
        except Exception as e:
            if str(e) != str(_last_connection_error):
                print(f"⚠️ Failed to connect to Substrate node: {e} (Type: {type(e).__name__})")
                _last_connection_error = str(e)
            _update_status(f"Connection Error: {type(e).__name__}")
            _substrate_instance = None
    return _substrate_instance

async def _execute_query_async(query_fn, *args, **kwargs):
    """Helper to run synchronous substrate queries in an executor for async context."""
    substrate = _get_substrate_interface()
    if not substrate:
        fn_name = getattr(query_fn, '__name__', 'query')
        logger.error(f"Cannot execute {fn_name}: No Substrate connection.")
        return None
    loop = asyncio.get_running_loop()
    try:
        result = await loop.run_in_executor(None, lambda: query_fn(*args, **kwargs))
        return result
    except SubstrateRequestException as e:
        fn_name = getattr(query_fn, '__name__', 'query')
        logger.error(f"Substrate request error during {fn_name}: {e}")
        _get_substrate_interface(force_reconnect=True)
        return None
    except Exception as e:
        fn_name = getattr(query_fn, '__name__', 'query')
        logger.error(f"An unexpected error occurred during {fn_name}: {e} (Type: {type(e).__name__})")
        if "ConnectionClosed" in str(e) or "Socket" in str(e):
            _get_substrate_interface(force_reconnect=True)
        return None

async def fetch_all_chain_data(substrate, block_hash=None, block_number=None, event_queue=None):
    """Fetches all configured storage items and maps for the given block, then saves to DB."""
    try:
        if not substrate:
            logger.error("Cannot fetch chain data: No Substrate connection.")
            return {}

        # 1. Fetch individual storage items
        if config.STORAGE_ITEMS_TO_FETCH:
            logger.info(f"Fetching individual items at block: {block_hash or 'latest'}")
            multi_query_params = []
            for item_config in config.STORAGE_ITEMS_TO_FETCH:
                if len(item_config) in (2, 3):
                    module, item = item_config[:2]
                    params = item_config[2] if len(item_config) == 3 else None
                    if params and isinstance(params, str):
                        if params.startswith('0x'):
                            try:
                                params = bytes.fromhex(params[2:])
                            except ValueError:
                                # logger.error(f"Invalid hex string for params in {module}.{item}: {params}")
                                params = None
                        else:
                            logger.warning(f"Params for {module}.{item} is a string ({params}), treating as literal.")
                    if params:
                        multi_query_params.append((module, item, params))
                    else:
                        multi_query_params.append((module, item))

            if multi_query_params:
                # logger.info(f"Query multi params: {multi_query_params}")
                try:
                    result = await _execute_query_async(substrate.query, module, item, block_hash=block_hash)
                    logger.debug(f"Raw query_multi results: {result}")
                    if result is not None:
                        value = result.value if hasattr(result, 'value') else result
                        logger.debug(f"CurrentEpochValidator value: {value}")
                        try:
                            if value is not None:
                                if isinstance(value, (tuple, list)) and len(value) == 2:
                                    account_id, block_num = value
                                    await utils.save_current_epoch_validator(config.db_pool, str(account_id), int(block_num))
                                elif isinstance(value, dict) and "account_id" in value and "block_number" in value:
                                    account_id = value["account_id"]
                                    block_num = value["block_number"]
                                    await utils.save_current_epoch_validator(config.db_pool, str(account_id), int(block_num))
                                else:
                                    logger.warning(f"Unexpected CurrentEpochValidator value format: {value}")
                                    await utils.save_current_epoch_validator(config.db_pool, None, None)
                            else:
                                logger.info("CurrentEpochValidator is None")
                                await utils.save_current_epoch_validator(config.db_pool, None, None)
                        except Exception as e:
                            logger.error(f"Error processing CurrentEpochValidator value {value}: {e}")
                            await utils.save_current_epoch_validator(config.db_pool, None, None)
                    else:
                        logger.error("query_multi returned None or failed.")
                except Exception as e:
                    logger.error(f"Error during query_multi: {e} (Type: {type(e).__name__})\n{traceback.format_exc()}")
                    # Fallback to individual queries
                    for item_config_tuple in multi_query_params:
                        module, item = item_config_tuple[:2]
                        params = item_config_tuple[2] if len(item_config_tuple) == 3 else None
                        storage_key_name = utils.get_storage_key_string(module, item, params)
                        try:
                            if params:
                                result = await _execute_query_async(substrate.query, module, item, params=params, block_hash=block_hash)
                            else:
                                result = await _execute_query_async(substrate.query, module, item, block_hash=block_hash)
                            logger.debug(f"Individual query result for {storage_key_name}: {result}")
                            if result is not None:
                                if module == "IpfsPallet" and item == "CurrentEpochValidator":
                                    value = result.value if hasattr(result, 'value') else result
                                    logger.debug(f"CurrentEpochValidator individual query value: {value}")
                                    try:
                                        if value is not None:
                                            if isinstance(value, (tuple, list)) and len(value) == 2:
                                                account_id, block_num = value
                                                await utils.save_current_epoch_validator(config.db_pool, str(account_id), int(block_num))
                                            elif isinstance(value, dict) and "account_id" in value and "block_number" in value:
                                                account_id = value["account_id"]
                                                block_num = value["block_number"]
                                                await utils.save_current_epoch_validator(config.db_pool, str(account_id), int(block_num))
                                            else:
                                                logger.warning(f"Unexpected CurrentEpochValidator value format: {value}")
                                                await utils.save_current_epoch_validator(config.db_pool, None, None)
                                        else:
                                            await utils.save_current_epoch_validator(config.db_pool, None, None)
                                    except Exception as e:
                                        logger.error(f"Error processing CurrentEpochValidator individual query value {value}: {e}")
                                        await utils.save_current_epoch_validator(config.db_pool, None, None)
                            else:
                                logger.error(f"Failed to fetch {storage_key_name}")
                        except Exception as e:
                            logger.error(f"Error during individual query for {storage_key_name}: {e}\n{traceback.format_exc()}")

        # 2. Fetch all entries for specified StorageMaps
        if config.STORAGE_MAPS_TO_FETCH_ALL:
            logger.info(f"Fetching storage maps at block: {block_hash or 'latest'} (Block number: {block_number})")
            user_storage_requests = {}  # Store UserStorageRequests data
            node_registration = {}  # Store NodeRegistration data
            coldkey_registration = {}  # Store ColdkeyNodeRegistration data

            for module, map_name in config.STORAGE_MAPS_TO_FETCH_ALL:
                # Skip ExecutionUnit.NodeMetrics unless block_number is a multiple of 300
                if module == "ExecutionUnit" and map_name == "NodeMetrics" and block_number is not None and block_number % 300 != 0:
                    logger.info(f"Skipping ExecutionUnit.NodeMetrics at block {block_number} (not a multiple of 300)")
                    continue

                map_key_name = utils.get_storage_key_string(module, map_name) + "_ALL"
                try:
                    map_entries_raw = await _execute_query_async(substrate.query_map, module, map_name, block_hash=block_hash)
                    if map_entries_raw is not None:
                        for key_storage_obj, value_storage_obj in map_entries_raw:
                            # Extract the node_id from the key
                            entry_key_param_str = '0x' + key_storage_obj.value.hex() if hasattr(key_storage_obj, 'value') and isinstance(key_storage_obj.value, bytes) else str(key_storage_obj)

                            if module == "IpfsPallet" and map_name == "UserStorageRequests":
                                # Handle StorageDoubleMap: key_storage_obj is a tuple (owner_account_id, file_hash)
                                if isinstance(key_storage_obj, (tuple, list)) and len(key_storage_obj) == 2:
                                    owner_account_id = str(key_storage_obj[0])  # SS58 address
                                    file_hash = utils.bounded_vec_to_string(key_storage_obj[1])  # Convert BoundedVec to string
                                    value = value_storage_obj.value if hasattr(value_storage_obj, 'value') else value_storage_obj
                                    user_storage_requests[(owner_account_id, file_hash)] = value
                                else:
                                    logger.warning(f"Unexpected key format for UserStorageRequests: {key_storage_obj}")

                            elif module == "Registration" and map_name == "NodeRegistration":
                                value = value_storage_obj.value if hasattr(value_storage_obj, 'value') else value_storage_obj
                                # Skip if the value is None (Option<NodeInfo> returned None)
                                if value is None:
                                    logger.debug(f"NodeRegistration for node {entry_key_param_str} is None, skipping.")
                                    continue
                                # Process NodeInfo struct
                                try:
                                    node_info = {
                                        "node_id": utils.bounded_vec_to_string(value.get("node_id")),
                                        "ipfs_node_id": utils.bounded_vec_to_string(value.get("ipfs_node_id")) if value.get("ipfs_node_id") else None,
                                        "node_type": value.get("node_type", ""),
                                        "owner": str(value.get("owner", "")),
                                        "registered_at": int(value.get("registered_at", 0)),
                                        "status": value.get("status", "")
                                    }
                                    node_registration[entry_key_param_str] = node_info
                                except Exception as e:
                                    logger.error(f"Error processing NodeRegistration for node {entry_key_param_str}: {e}")

                            elif module == "Registration" and map_name == "ColdkeyNodeRegistration":
                                value = value_storage_obj.value if hasattr(value_storage_obj, 'value') else value_storage_obj
                                # Skip if the value is None (Option<NodeInfo> returned None)
                                if value is None:
                                    logger.debug(f"ColdkeyNodeRegistration for node {entry_key_param_str} is None, skipping.")
                                    continue
                                # Process NodeInfo struct
                                try:
                                    node_info = {
                                        "node_id": utils.bounded_vec_to_string(value.get("node_id")),
                                        "ipfs_node_id": utils.bounded_vec_to_string(value.get("ipfs_node_id")) if value.get("ipfs_node_id") else None,
                                        "node_type": value.get("node_type", ""),
                                        "owner": str(value.get("owner", "")),
                                        "registered_at": int(value.get("registered_at", 0)),
                                        "status": value.get("status", "")
                                    }
                                    coldkey_registration[entry_key_param_str] = node_info
                                except Exception as e:
                                    logger.error(f"Error processing ColdkeyNodeRegistration for node {entry_key_param_str}: {e}")

                            # Other maps will be handled below in save logic
                    else:
                        logger.error(f"query_map for {map_key_name} returned None or failed.")
                except Exception as e:
                    logger.error(f"Error during query_map for {map_key_name}: {e}\n{traceback.format_exc()}")

        # 3. Save data to the database
        if block_number is not None:
            # Handle Registration data
            if node_registration or coldkey_registration:
                logger.info(f"NodeRegistration entries: {len(node_registration)}, ColdkeyNodeRegistration entries: {len(coldkey_registration)}")
                try:
                    await utils.save_registration_data(config.db_pool, node_registration, coldkey_registration)
                    logger.info("Successfully saved registration data to database.")
                except Exception as e:
                    logger.error(f"Error saving registration data: {e}")
                    raise

            # Handle UserStorageRequests
            if user_storage_requests:
                try:
                    await utils.save_user_storage_requests(config.db_pool, user_storage_requests)
                    logger.info("Successfully saved UserStorageRequests data to database.")
                except Exception as e:
                    logger.error(f"Error saving UserStorageRequests data: {e}")
                    raise

            # Initialize metrics_data as an empty dict by default
            metrics_data = {}
            # Handle ExecutionUnit.NodeMetrics (update, don't delete) only every 300th block
            if any(module == "ExecutionUnit" and map_name == "NodeMetrics" for module, map_name in config.STORAGE_MAPS_TO_FETCH_ALL) and block_number % 300 == 0:
                map_entries_raw = await _execute_query_async(substrate.query_map, "ExecutionUnit", "NodeMetrics", block_hash=block_hash)
                if map_entries_raw is not None:
                    for key_storage_obj, value_storage_obj in map_entries_raw:
                        entry_key_param_str = '0x' + key_storage_obj.value.hex() if hasattr(key_storage_obj, 'value') and isinstance(key_storage_obj.value, bytes) else str(key_storage_obj)
                        logger.debug(f"NodeMetrics for node {entry_key_param_str}: {value_storage_obj.value}")
                        metrics_data[entry_key_param_str] = {
                            "ipfs_storage_max": value_storage_obj.value.get('ipfs_storage_max', 0),
                            "ipfs_zfs_pool_size": value_storage_obj.value.get('ipfs_zfs_pool_size', 0),
                            "successful_pin_checks": value_storage_obj.value.get('successful_pin_checks', 0),
                            "total_pin_checks": value_storage_obj.value.get('total_pin_checks', 0)
                        }
                        logger.debug(f"Extracted metrics for node {entry_key_param_str}: {metrics_data[entry_key_param_str]}")

            # Fetch and handle MinerTotalFilesSize and MinerTotalFilesPinned
            miner_files_size_data = {}
            miner_files_pinned_data = {}
            miner_files_size_entries = await _execute_query_async(substrate.query_map, "IpfsPallet", "MinerTotalFilesSize", block_hash=block_hash)
            if miner_files_size_entries is not None:
                for key_storage_obj, value_storage_obj in miner_files_size_entries:
                    node_id = '0x' + key_storage_obj.value.hex() if hasattr(key_storage_obj, 'value') and isinstance(key_storage_obj.value, bytes) else str(key_storage_obj.value)
                    miner_files_size_data[node_id] = value_storage_obj.value if hasattr(value_storage_obj, 'value') else value_storage_obj.value

            miner_files_pinned_entries = await _execute_query_async(substrate.query_map, "IpfsPallet", "MinerTotalFilesPinned", block_hash=block_hash)
            if miner_files_pinned_entries is not None:
                for key_storage_obj, value_storage_obj in miner_files_pinned_entries:
                    node_id = '0x' + key_storage_obj.value.hex() if hasattr(key_storage_obj, 'value') and isinstance(key_storage_obj.value, bytes) else str(key_storage_obj.value)
                    miner_files_pinned_data[node_id] = value_storage_obj.value if hasattr(value_storage_obj, 'value') else value_storage_obj.value

            # Combine all metrics into a single dictionary for database update
            all_metrics_data = {}
            all_node_ids = set(list(metrics_data.keys()) + list(miner_files_size_data.keys()) + list(miner_files_pinned_data.keys()))
            for node_id in all_node_ids:
                all_metrics_data[node_id] = {
                    "ipfs_storage_max": metrics_data.get(node_id, {}).get("ipfs_storage_max", 0),
                    "ipfs_zfs_pool_size": metrics_data.get(node_id, {}).get("ipfs_zfs_pool_size", 0),
                    "successful_pin_checks": metrics_data.get(node_id, {}).get("successful_pin_checks", 0),
                    "total_pin_checks": metrics_data.get(node_id, {}).get("total_pin_checks", 0),
                    "miner_total_files_size": miner_files_size_data.get(node_id, 0),
                    "miner_total_files_pinned": miner_files_pinned_data.get(node_id, 0)
                }
            await utils.update_execution_unit_metrics(config.db_pool, all_metrics_data)

            # Fetch and handle BlockNumbers and MinerProfile for saving to miners table
            block_numbers_data = {}
            miner_profile_data = {}
            block_numbers_result = await _execute_query_async(substrate.query_map, "ExecutionUnit", "BlockNumbers", block_hash=block_hash)
            if block_numbers_result is not None:
                for key_storage_obj, value_storage_obj in block_numbers_result:
                    entry_key_param_str = '0x' + key_storage_obj.value.hex() if hasattr(key_storage_obj, 'value') and isinstance(key_storage_obj.value, bytes) else str(key_storage_obj)
                    logger.debug(f"BlockNumbers for node {entry_key_param_str}: {value_storage_obj.value}")
                    # BlockNumbers might return a list or a single integer
                    block_value = value_storage_obj.value
                    if isinstance(block_value, list) and block_value:
                        block_numbers_data[entry_key_param_str] = block_value[0]  # Take the first block number
                    else:
                        block_numbers_data[entry_key_param_str] = block_value if isinstance(block_value, (int, str)) else None
            else:
                logger.warning("BlockNumbers query returned None")

            miner_profile_result = await _execute_query_async(substrate.query_map, "IpfsPallet", "MinerProfile", block_hash=block_hash)
            if miner_profile_result is not None:
                for key_storage_obj, value_storage_obj in miner_profile_result:
                    entry_key_param_str = '0x' + key_storage_obj.value.hex() if hasattr(key_storage_obj, 'value') and isinstance(key_storage_obj.value, bytes) else str(key_storage_obj)
                    logger.debug(f"MinerProfile for node {entry_key_param_str}: {value_storage_obj.value}")
                    # MinerProfile should be a BoundedVec<u8, ConstU32<255>>, decode to string
                    profile_value = value_storage_obj.value
                    if isinstance(profile_value, (list, bytes)):
                        profile_value = utils.bounded_vec_to_string(profile_value)
                    miner_profile_data[entry_key_param_str] = profile_value if isinstance(profile_value, str) else None
            else:
                logger.warning("MinerProfile query returned None")

            # Save BlockNumbers and MinerProfile data to the miners table
            if block_numbers_data or miner_profile_data:
                try:
                    logger.info(f"Saving BlockNumbers and MinerProfile data for {len(block_numbers_data)} nodes (BlockNumbers) and {len(miner_profile_data)} nodes (MinerProfile)")
                    await utils.save_miners_data(config.db_pool, block_numbers_data, miner_profile_data)
                    logger.info("Successfully saved miners data (BlockNumbers and MinerProfile) to database.")
                except Exception as e:
                    logger.error(f"Error saving miners data: {e}")
                    raise
            else:
                logger.warning("No BlockNumbers or MinerProfile data to save.")

            # Save the latest block number to the database
            try:
                await utils.save_latest_block(config.db_pool, block_number)
                logger.info(f"Successfully saved latest block number: {block_number}")
            except Exception as e:
                logger.error(f"Error saving latest block number: {e}")
                raise

        # 4. Queue changed CIDs for IPFS content fetch
        global _previous_ipfs_profiles
        ipfs_profiles = {}
        miner_profile_result = await _execute_query_async(substrate.query_map, "IpfsPallet", "MinerProfile", block_hash=block_hash)
        if miner_profile_result is not None:
            for key_storage_obj, value_storage_obj in miner_profile_result:
                entry_key_param_str = '0x' + key_storage_obj.value.hex() if hasattr(key_storage_obj, 'value') and isinstance(key_storage_obj.value, bytes) else str(key_storage_obj)
                profile_value = value_storage_obj.value
                if isinstance(profile_value, (list, bytes)):
                    profile_value = utils.bounded_vec_to_string(profile_value)
                ipfs_profiles[entry_key_param_str] = profile_value
        user_profile_result = await _execute_query_async(substrate.query_map, "IpfsPallet", "UserProfile", block_hash=block_hash)
        if user_profile_result is not None:
            for key_storage_obj, value_storage_obj in user_profile_result:
                entry_key_param_str = '0x' + key_storage_obj.value.hex() if hasattr(key_storage_obj, 'value') and isinstance(key_storage_obj.value, bytes) else str(key_storage_obj)
                profile_value = value_storage_obj.value
                if isinstance(profile_value, (list, bytes)):
                    profile_value = utils.bounded_vec_to_string(profile_value)
                ipfs_profiles[entry_key_param_str] = profile_value

        # Identify changed CIDs
        changed_cids = []
        for node_id, cid in ipfs_profiles.items():
            if node_id not in _previous_ipfs_profiles or _previous_ipfs_profiles[node_id] != cid:
                changed_cids.append((node_id, cid))
        _previous_ipfs_profiles = ipfs_profiles.copy()

        # Send changed CIDs to the IPFS fetch process and wait for completion
        if changed_cids and _ipfs_fetch_queue:
            if event_queue is None:
                logger.error("event_queue is not provided, skipping IPFS fetch")
                return
            logger.info(f"Queueing {len(changed_cids)} changed/new CIDs for IPFS fetch")
            _ipfs_fetch_queue.put((changed_cids, set(ipfs_profiles.keys())))
            start_time = time.time()
            timeout_seconds = getattr(config, 'IPFS_FETCH_TIMEOUT', 60)
            while time.time() - start_time < timeout_seconds:
                try:
                    if not event_queue.empty():
                        logger.info("Received completion signal from IPFS fetch worker")
                        event_queue.get_nowait()
                        break
                except QueueEmptyException:
                    await asyncio.sleep(0.1)
            else:
                logger.warning(f"IPFS fetch worker timed out after {timeout_seconds} seconds")
                
            ipfs_content = dict(_ipfs_content)
            if ipfs_content:
                logger.info(f"Saving IPFS content for {len(ipfs_content)} nodes")
                await utils.save_ipfs_profiles(config.db_pool, ipfs_content)
                _ipfs_content.clear()
                logger.info("Cleared ipfs_content")
            else:
                logger.info("No IPFS content to save")
        else:
            logger.info("No changes in CIDs, skipping IPFS content fetch")

    except Exception as e:
        logger.error(f"Critical error in fetch_all_chain_data: {e} (Type: {type(e).__name__})\n{traceback.format_exc()}")
        raise

async def _handle_new_block_data(header_data, update_nr, subscription_id, event_queue):
    """Async callback for new blocks, fetches data and processes it."""
    global _latest_data
    try:
        substrate = _get_substrate_interface()
        if _exit_event.is_set() or not substrate:
            logger.warning("Skipping block processing: Exit event set or no Substrate connection")
            return

        block_hash = header_data.get('hash')
        block_number = header_data.get('number')

        if not block_hash or not block_number:
            logger.error("Could not extract block hash or number from header data")
            return

        if isinstance(block_hash, bytes):
            block_hash_hex = '0x' + block_hash.hex()
        else:
            block_hash_hex = block_hash

        _update_status(f"Processing block #{block_number}")
        logger.info(f"New finalized block: #{block_number} (Hash: {block_hash_hex})")

        await fetch_all_chain_data(substrate, block_hash=block_hash_hex, block_number=block_number, event_queue=event_queue)
        logger.info(f"Successfully processed data for block #{block_number}")
        
        _latest_data = {
            "block_number": block_number,
            "block_hash": block_hash_hex,
            "timestamp": time.time()
        }
        
        _update_status(f"Connected (Last fetch: Block #{block_number})")

    except Exception as e:
        logger.error(f"Error in _handle_new_block_data: {e} (Type: {type(e).__name__})\n{traceback.format_exc()}")
        raise

async def _get_chain_head_hash() -> str | None:
    """Asynchronously fetches the latest block hash."""
    try:
        substrate = _get_substrate_interface()
        if not substrate:
            logger.error("Cannot fetch chain head hash: No Substrate connection.")
            return None
        result = await _execute_query_async(substrate.get_chain_head)
        if isinstance(result, str):
            logger.info(f"Chain head hash retrieved as string: {result}")
            return result
        elif result and hasattr(result, 'value') and isinstance(result.value, str):
            logger.info(f"Chain head hash retrieved from .value as string: {result.value}")
            return result.value
        elif result and hasattr(result, 'value') and isinstance(result.value, bytes):
            hash_str = '0x' + result.value.hex()
            logger.info(f"Chain head hash retrieved from .value as bytes, converted to: {hash_str}")
            return hash_str
        elif isinstance(result, bytes):
            hash_str = '0x' + result.hex()
            logger.info(f"Chain head hash retrieved as bytes, converted to: {hash_str}")
            return hash_str
        logger.error("Failed to retrieve chain head hash: Result is None or unexpected type.")
        return None
    except Exception as e:
        logger.error(f"Error in _get_chain_head_hash: {e} (Type: {type(e).__name__})\n{traceback.format_exc()}")
        return None

async def _get_block_number_by_hash(block_hash: str) -> int | None:
    """Fetches the block number for a given block hash."""
    try:
        substrate = _get_substrate_interface()
        if not substrate:
            logger.error("Cannot fetch block number: No Substrate connection.")
            return None
        block_header_dict = await _execute_query_async(substrate.get_block_header, block_hash=block_hash)
        if block_header_dict and 'header' in block_header_dict and 'number' in block_header_dict['header']:
            logger.info(f"Block number for hash {block_hash}: {block_header_dict['header']['number']}")
            return block_header_dict['header']['number']
        logger.error(f"Could not get block number from header for hash {block_hash}")
        return None
    except Exception as e:
        logger.error(f"Error in _get_block_number_by_hash: {e} (Type: {type(e).__name__})\n{traceback.format_exc()}")
        return None

async def _initial_fetch_async(event_queue):
    """Async initial fetch."""
    substrate = _get_substrate_interface()
    if not substrate:
        logger.error("Cannot perform initial fetch: No Substrate connection.")
        return False
    _update_status("Performing initial fetch")
    logger.info("Performing initial fetch...")

    head_hash = await _get_chain_head_hash()
    if head_hash:
        block_number = await _get_block_number_by_hash(head_hash)
        if block_number is not None:
            header_data_for_handler = {
                'hash': bytes.fromhex(head_hash[2:]),
                'number': block_number
            }
            await _handle_new_block_data(header_data_for_handler, 0, "initial_fetch", event_queue)
            return True
        else:
            logger.error("Could not get initial block number.")
            _update_status("Error during initial fetch (no block number)")
            return False
    else:
        logger.error("Could not get initial head hash for initial fetch.")
        _update_status("Error during initial fetch (no head hash)")
        return False

async def start_fetching_loop_async():
    """Main async loop to connect, subscribe, and handle reconnections."""
    global _ipfs_fetch_process, _ipfs_fetch_queue, _ipfs_content
    _update_status("Starting async fetcher loop")
    logger.info("Starting fetcher loop...")

    try:
        # Start IPFS fetch worker process
        manager = mp.Manager()
        _ipfs_fetch_queue = manager.Queue()
        _ipfs_content = manager.dict()
        event_queue = manager.Queue()  # Queue for worker to signal completion
        _ipfs_fetch_process = mp.Process(target=utils.ipfs_fetch_worker, args=(_ipfs_fetch_queue, _ipfs_content, event_queue))
        _ipfs_fetch_process.start()
        logger.info("Started IPFS fetch worker process")

        subscription_active = False
        polling_mode = False
        last_head_hash = None
        latest_block_number = 0

        # First do an initial fetch to get started
        success = await _initial_fetch_async(event_queue)
        if not success:
            logger.warning("Initial fetch failed. Switching to polling mode.")
            polling_mode = True
            _update_status("Polling Mode")

        while not _exit_event.is_set():
            substrate = _get_substrate_interface()
            if not substrate:
                _update_status("Connection failed, retrying...")
                await asyncio.sleep(config.SUBSCRIPTION_RETRY_DELAY * 2)
                continue

            if not subscription_active and not polling_mode:
                _update_status("Attempting to subscribe to finalized heads...")
                logger.info("Attempting to subscribe to finalized heads...")
                try:
                    # Create a queue for block headers
                    block_queue = asyncio.Queue()

                    async def sync_subscription_handler(header_obj, update_nr, subscription_id):
                        logger.info(f"Received subscription update: Update #{update_nr}, Subscription ID: {subscription_id}")
                        await block_queue.put(header_obj)

                    sub_id = await _execute_query_async(substrate.subscribe_block_headers, sync_subscription_handler, finalized_only=True)
                    if sub_id and isinstance(sub_id, str):
                        logger.info(f"Successfully subscribed with ID: {sub_id}")
                        _update_status("Subscribed")
                        subscription_active = True
                    else:
                        logger.warning(f"Failed to subscribe to finalized heads (sub_id: {sub_id}). Switching to polling mode.")
                        polling_mode = True
                        _update_status("Polling Mode")
                except Exception as e:
                    logger.error(f"Error during subscription: {e}. Switching to polling mode.\n{traceback.format_exc()}")
                    polling_mode = True
                    _update_status("Polling Mode")

            if subscription_active:
                try:
                    # Process blocks from the subscription
                    header_data = await asyncio.wait_for(block_queue.get(), timeout=5.0)
                    
                    # Ensure we have valid block data
                    if not header_data or 'hash' not in header_data or 'number' not in header_data:
                        logger.warning("Received invalid block header data from subscription")
                        continue
                        
                    block_hash = header_data['hash']
                    block_number = header_data['number']
                    
                    # Convert hash to hex string if needed
                    if isinstance(block_hash, bytes):
                        block_hash = '0x' + block_hash.hex()
                    
                    # Skip if this block is older than our latest processed
                    if block_number <= latest_block_number:
                        logger.info(f"Skipping block #{block_number} (already processed #{latest_block_number})")
                        continue
                        
                    logger.info(f"Processing new block #{block_number} (Hash: {block_hash})")
                    
                    # Process the block data
                    await fetch_all_chain_data(
                        substrate, 
                        block_hash=block_hash, 
                        block_number=block_number,
                        event_queue=event_queue
                    )
                    
                    # Update latest processed block
                    latest_block_number = block_number
                    _update_status(f"Connected (Last fetch: Block #{block_number})")

                except asyncio.TimeoutError:
                    # Check connection status
                    if substrate and hasattr(substrate, 'websocket') and \
                       (not substrate.websocket or not substrate.websocket.connected):
                        logger.warning("Detected disconnection (websocket). Re-initiating.")
                        subscription_active = False
                        _get_substrate_interface(force_reconnect=True)
                except Exception as e:
                    logger.error(f"Error processing block from subscription: {e}\n{traceback.format_exc()}")
                    subscription_active = False
                    _get_substrate_interface(force_reconnect=True)
                    await asyncio.sleep(config.SUBSCRIPTION_RETRY_DELAY)

            if polling_mode:
                try:
                    head_hash = await _get_chain_head_hash()
                    if head_hash and head_hash != last_head_hash:
                        last_head_hash = head_hash
                        block_number = await _get_block_number_by_hash(head_hash)
                        if block_number is not None:
                            # Skip if this block is older than our latest processed
                            if block_number <= latest_block_number:
                                logger.debug(f"Skipping block #{block_number} (already processed #{latest_block_number})")
                                continue
                                
                            logger.info(f"Processing new block #{block_number} (Hash: {head_hash})")
                            
                            # Process the block data
                            await fetch_all_chain_data(
                                substrate, 
                                block_hash=head_hash, 
                                block_number=block_number,
                                event_queue=event_queue
                            )
                            
                            # Update latest processed block
                            latest_block_number = block_number
                            _update_status(f"Connected (Last fetch: Block #{block_number})")
                        else:
                            logger.warning("Failed to get block number for hash, retrying...")
                    else:
                        logger.debug("No new block head hash or same as last, skipping...")
                except Exception as e:
                    logger.error(f"Error in polling loop: {e}\n{traceback.format_exc()}")
                    _get_substrate_interface(force_reconnect=True)  # Reconnect on error
                await asyncio.sleep(5)

    except Exception as e:
        logger.error(f"Critical error in start_fetching_loop_async: {e} (Type: {type(e).__name__})\n{traceback.format_exc()}")
        raise
    finally:
        logger.info("Entering cleanup in start_fetching_loop_async")
        await stop_fetching_async()
        logger.info("Async fetcher loop cleanup completed")

async def stop_fetching_async():
    """Signals the fetching loop to stop and cleans up."""
    logger.info("Stopping async storage fetcher...")
    _exit_event.set()
    if _ipfs_fetch_queue:
        _ipfs_fetch_queue.put(None)  # Send sentinel to stop the worker
    if _ipfs_fetch_process and _ipfs_fetch_process.is_alive():
        logger.info("Waiting for IPFS fetch worker to terminate...")
        _ipfs_fetch_process.join(timeout=5)
        if _ipfs_fetch_process.is_alive():
            logger.warning("Forcing IPFS fetch worker termination...")
            _ipfs_fetch_process.terminate()
            _ipfs_fetch_process.join()
    if _ipfs_fetch_process and hasattr(os, 'waitpid'):
        try:
            os.waitpid(_ipfs_fetch_process.pid, os.WNOHANG)
        except (OSError, ChildProcessError):
            pass
    substrate = _get_substrate_interface()
    if substrate:
        logger.info("Closing Substrate connection...")
        try:
            await _execute_query_async(substrate.close)
        except Exception as e:
            logger.error(f"Error closing substrate connection: {e}")
    _update_status("Stopped")
    logger.info("Async storage fetcher stopped.")