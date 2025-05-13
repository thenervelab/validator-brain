# substrate_fetcher/substrate_fetcher.py

import asyncio
import multiprocessing as mp
from substrateinterface import SubstrateInterface
from substrateinterface.exceptions import SubstrateRequestException
from typing import Any
from . import config
from . import utils

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

# --- Helper Functions ---
def get_status():
    return _current_status

def is_stopping():
    return _exit_event.is_set()

def _update_status(new_status: str):
    global _current_status
    _current_status = new_status

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
        print(f"Cannot execute {fn_name}: No Substrate connection.")
        return None
    loop = asyncio.get_running_loop()
    try:
        result = await loop.run_in_executor(None, lambda: query_fn(*args, **kwargs))
        return result
    except SubstrateRequestException as e:
        fn_name = getattr(query_fn, '__name__', 'query')
        print(f"Substrate request error during {fn_name}: {e}")
        _get_substrate_interface(force_reconnect=True)
        return None
    except Exception as e:
        fn_name = getattr(query_fn, '__name__', 'query')
        print(f"An unexpected error occurred during {fn_name}: {e} (Type: {type(e).__name__})")
        if "ConnectionClosed" in str(e) or "Socket" in str(e):
            _get_substrate_interface(force_reconnect=True)
        return None

async def fetch_all_chain_data(substrate, block_hash=None, block_number=None):
    """Fetches all configured storage items and maps for the given block, then saves to DB."""
    try:
        if not substrate:
            print("Cannot fetch chain data: No Substrate connection.")
            return {}

        # 1. Fetch individual storage items
        if config.STORAGE_ITEMS_TO_FETCH:
            print(f"Fetching individual items at block: {block_hash or 'latest'}")
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
                                print(f"Invalid hex string for params in {module}.{item}: {params}")
                                params = None
                        else:
                            print(f"Warning: Params for {module}.{item} is a string ({params}), treating as literal.")
                    if params:
                        multi_query_params.append((module, item, params))
                    else:
                        multi_query_params.append((module, item))

            if multi_query_params:
                print(f"Query multi params: {multi_query_params}")
                try:
                    results = await _execute_query_async(substrate.query_multi, multi_query_params, block_hash=block_hash)
                    if results is not None:
                        for i, item_config_tuple in enumerate(multi_query_params):
                            storage_key_name = utils.get_storage_key_string(
                                item_config_tuple[0], item_config_tuple[1],
                                item_config_tuple[2] if len(item_config_tuple) == 3 else None
                            )
                            if i < len(results) and results[i] is not None:
                                pass  # No need to store in memory since we're using DB
                            else:
                                print(f"Failed to fetch {storage_key_name}")
                    else:
                        print("query_multi returned None or failed.")
                except Exception as e:
                    print(f"Error during query_multi: {e}")
                    for item_config_tuple in multi_query_params:
                        module, item = item_config_tuple[:2]
                        params = item_config_tuple[2] if len(item_config_tuple) == 3 else None
                        storage_key_name = utils.get_storage_key_string(module, item, params)
                        try:
                            if params:
                                result = await _execute_query_async(substrate.query, module, item, params=params, block_hash=block_hash)
                            else:
                                result = await _execute_query_async(substrate.query, module, item, block_hash=block_hash)
                            if result is not None:
                                pass  # No need to store in memory
                            else:
                                print(f"Failed to fetch {storage_key_name}")
                        except Exception as e:
                            print(f"Error during individual query for {storage_key_name}: {e}")

        # 2. Fetch all entries for specified StorageMaps
        if config.STORAGE_MAPS_TO_FETCH_ALL:
            print(f"Fetching storage maps at block: {block_hash or 'latest'} (Block number: {block_number})")
            for module, map_name in config.STORAGE_MAPS_TO_FETCH_ALL:
                # Skip ExecutionUnit.NodeMetrics unless block_number is a multiple of 300
                if module == "ExecutionUnit" and map_name == "NodeMetrics" and block_number is not None and block_number % 300 != 0:
                    print(f"  Skipping ExecutionUnit.NodeMetrics at block {block_number} (not a multiple of 300)")
                    continue

                map_key_name = utils.get_storage_key_string(module, map_name) + "_ALL"
                print(f"  Querying map: {module}.{map_name}")
                try:
                    map_entries_raw = await _execute_query_async(substrate.query_map, module, map_name, block_hash=block_hash)
                    if map_entries_raw is not None:
                        for key_storage_obj, value_storage_obj in map_entries_raw:
                            if module == "ExecutionUnit" and map_name == "NodeMetrics":
                                pass  # Handled by DB update logic
                            elif module == "ExecutionUnit" and map_name == "BlockNumbers":
                                pass  # Handled by DB save logic below
                            elif module == "IpfsPallet" and map_name in ["MinerProfile", "UserProfile"]:
                                pass  # Handled by DB save logic below
                            elif module == "Registration" and map_name in ["NodeRegistration", "ColdkeyNodeRegistration"]:
                                pass  # Handled by DB save logic below
                    else:
                        print(f"    query_map for {map_key_name} returned None or failed.")
                except Exception as e:
                    print(f"Error during query_map for {map_key_name}: {e}")

        # 3. Save data to the database
        if block_number is not None:
            # Handle ExecutionUnit.NodeMetrics (update, don't delete)
            if module == "ExecutionUnit" and map_name == "NodeMetrics" and block_number % 300 == 0:
                map_entries_raw = await _execute_query_async(substrate.query_map, module, map_name, block_hash=block_hash)
                metrics_data = {}
                if map_entries_raw is not None:
                    for key_storage_obj, value_storage_obj in map_entries_raw:
                        entry_key_param_str = '0x' + key_storage_obj.value.hex() if hasattr(key_storage_obj, 'value') and isinstance(key_storage_obj.value, bytes) else str(key_storage_obj.value)
                        metrics_data[entry_key_param_str] = {
                            "ipfs_storage_max": value_storage_obj.value.get('ipfs_storage_max', 0),
                            "ipfs_zfs_pool_size": value_storage_obj.value.get('ipfs_zfs_pool_size', 0)
                        }
                await utils.update_execution_unit_metrics(config.db_pool, metrics_data)

            # Handle BlockNumbers and MinerProfiles (save on every block)
            block_numbers_result = await _execute_query_async(substrate.query_map, "ExecutionUnit", "BlockNumbers", block_hash=block_hash)
            block_numbers = {}
            if block_numbers_result is not None:
                for key_storage_obj, value_storage_obj in block_numbers_result:
                    entry_key_param_str = '0x' + key_storage_obj.value.hex() if hasattr(key_storage_obj, 'value') and isinstance(key_storage_obj.value, bytes) else str(key_storage_obj.value)
                    block_numbers[entry_key_param_str] = value_storage_obj.value
                    print(f"Processing BlockNumbers entry: {entry_key_param_str} -> {value_storage_obj.value}")
            else:
                print("BlockNumbers query returned None")
            print(f"Block numbers data: {block_numbers}")

            miner_profile_result = await _execute_query_async(substrate.query_map, "IpfsPallet", "MinerProfile", block_hash=block_hash)
            miner_profiles = {}
            if miner_profile_result is not None:
                for key_storage_obj, value_storage_obj in miner_profile_result:
                    entry_key_param_str = '0x' + key_storage_obj.value.hex() if hasattr(key_storage_obj, 'value') and isinstance(key_storage_obj.value, bytes) else str(key_storage_obj.value)
                    miner_profiles[entry_key_param_str] = value_storage_obj.value
                    print(f"Processing MinerProfile entry: {entry_key_param_str} -> {value_storage_obj.value}")
            else:
                print("MinerProfile query returned None")
            print(f"Miner profiles data: {miner_profiles}")

            if not block_numbers and not miner_profiles:
                print("Warning: Both BlockNumbers and MinerProfile data are empty. Skipping save_miners_data.")
            else:
                try:
                    await utils.save_miners_data(config.db_pool, block_numbers, miner_profiles)
                    print("Successfully saved miners data to database.")
                except Exception as e:
                    print(f"Error saving miners data: {e}")
                    raise

            # Handle Registration data
            node_registration = {}
            coldkey_registration = {}
            node_reg_result = await _execute_query_async(substrate.query_map, "Registration", "NodeRegistration", block_hash=block_hash)
            if node_reg_result is not None:
                for key_storage_obj, value_storage_obj in node_reg_result:
                    entry_key_param_str = '0x' + key_storage_obj.value.hex() if hasattr(key_storage_obj, 'value') and isinstance(key_storage_obj.value, bytes) else str(key_storage_obj.value)
                    node_registration[entry_key_param_str] = value_storage_obj.value
            coldkey_reg_result = await _execute_query_async(substrate.query_map, "Registration", "ColdkeyNodeRegistration", block_hash=block_hash)
            if coldkey_reg_result is not None:
                for key_storage_obj, value_storage_obj in coldkey_reg_result:
                    entry_key_param_str = '0x' + key_storage_obj.value.hex() if hasattr(key_storage_obj, 'value') and isinstance(key_storage_obj.value, bytes) else str(key_storage_obj.value)
                    coldkey_registration[entry_key_param_str] = value_storage_obj.value
            print(f"Node registration data: {node_registration}")
            print(f"Coldkey registration data: {coldkey_registration}")
            try:
                await utils.save_registration_data(config.db_pool, node_registration, coldkey_registration)
                print("Successfully saved registration data to database.")
            except Exception as e:
                print(f"Error saving registration data: {e}")
                raise

        # 4. Queue changed CIDs for IPFS content fetch (temporarily disabled)
        
        global _previous_ipfs_profiles
        ipfs_profiles = {}
        miner_profile_result = await _execute_query_async(substrate.query_map, "IpfsPallet", "MinerProfile", block_hash=block_hash)
        if miner_profile_result is not None:
            for key_storage_obj, value_storage_obj in miner_profile_result:
                entry_key_param_str = '0x' + key_storage_obj.value.hex() if hasattr(key_storage_obj, 'value') and isinstance(key_storage_obj.value, bytes) else str(key_storage_obj.value)
                ipfs_profiles[entry_key_param_str] = value_storage_obj.value
        user_profile_result = await _execute_query_async(substrate.query_map, "IpfsPallet", "UserProfile", block_hash=block_hash)
        if user_profile_result is not None:
            for key_storage_obj, value_storage_obj in user_profile_result:
                entry_key_param_str = '0x' + key_storage_obj.value.hex() if hasattr(key_storage_obj, 'value') and isinstance(key_storage_obj.value, bytes) else str(key_storage_obj.value)
                ipfs_profiles[entry_key_param_str] = value_storage_obj.value

        # Identify changed CIDs
        changed_cids = []
        for node_id, cid in ipfs_profiles.items():
            if node_id not in _previous_ipfs_profiles or _previous_ipfs_profiles[node_id] != cid:
                changed_cids.append((node_id, cid))
        _previous_ipfs_profiles = ipfs_profiles.copy()

        # Send changed CIDs to the IPFS fetch process
        if changed_cids and _ipfs_fetch_queue:
            print(f"Queueing {len(changed_cids)} changed CIDs for IPFS fetch")
            _ipfs_fetch_queue.put((changed_cids, set(ipfs_profiles.keys())))
        else:
            print("No changes in CIDs, skipping IPFS content fetch")
        
        # print("IPFS content fetch temporarily disabled for performance testing.")

    except Exception as e:
        print(f"Critical error in fetch_all_chain_data: {e} (Type: {type(e).__name__})")
        raise  # Re-raise to ensure the crash is logged

async def _handle_new_block_data(header_data, update_nr, subscription_id):
    """Async callback for new blocks, fetches data and processes it."""
    try:
        substrate = _get_substrate_interface()
        if _exit_event.is_set() or not substrate:
            return

        block_hash = header_data.get('hash')
        block_number = header_data.get('number')

        if not block_hash or not block_number:
            print("Error: Could not extract block hash or number from header data.")
            return

        if isinstance(block_hash, bytes):
            block_hash_hex = '0x' + block_hash.hex()
        else:
            block_hash_hex = block_hash

        _update_status(f"Processing block #{block_number}")
        print(f"\nNew finalized block: #{block_number} (Hash: {block_hash_hex})")

        await fetch_all_chain_data(substrate, block_hash=block_hash_hex, block_number=block_number)
        print(f"Successfully processed data for block #{block_number}.")
        _update_status(f"Connected (Last fetch: Block #{block_number})")

    except Exception as e:
        print(f"Error in _handle_new_block_data: {e} (Type: {type(e).__name__})")
        raise

async def _get_chain_head_hash() -> str | None:
    """Asynchronously fetches the latest block hash."""
    try:
        substrate = _get_substrate_interface()
        if not substrate:
            print("Cannot fetch chain head hash: No Substrate connection.")
            return None
        result = await _execute_query_async(substrate.get_chain_head)
        if isinstance(result, str):
            print(f"Chain head hash retrieved as string: {result}")
            return result
        elif result and hasattr(result, 'value') and isinstance(result.value, str):
            print(f"Chain head hash retrieved from .value as string: {result.value}")
            return result.value
        elif result and hasattr(result, 'value') and isinstance(result.value, bytes):
            hash_str = '0x' + result.value.hex()
            print(f"Chain head hash retrieved from .value as bytes, converted to: {hash_str}")
            return hash_str
        elif isinstance(result, bytes):
            hash_str = '0x' + result.hex()
            print(f"Chain head hash retrieved as bytes, converted to: {hash_str}")
            return hash_str
        print("Failed to retrieve chain head hash: Result is None or unexpected type.")
        return None
    except Exception as e:
        print(f"Error in _get_chain_head_hash: {e} (Type: {type(e).__name__})")
        return None

async def _get_block_number_by_hash(block_hash: str) -> int | None:
    """Fetches the block number for a given block hash."""
    try:
        substrate = _get_substrate_interface()
        if not substrate:
            print("Cannot fetch block number: No Substrate connection.")
            return None
        block_header_dict = await _execute_query_async(substrate.get_block_header, block_hash=block_hash)
        if block_header_dict and 'header' in block_header_dict and 'number' in block_header_dict['header']:
            print(f"Block number for hash {block_hash}: {block_header_dict['header']['number']}")
            return block_header_dict['header']['number']
        print(f"Could not get block number from header for hash {block_hash}")
        return None
    except Exception as e:
        print(f"Error in _get_block_number_by_hash: {e} (Type: {type(e).__name__})")
        return None

async def _initial_fetch_async():
    """Async initial fetch."""
    substrate = _get_substrate_interface()
    if not substrate:
        print("Cannot perform initial fetch: No Substrate connection.")
        return False
    _update_status("Performing initial fetch")
    print("Performing initial fetch...")

    head_hash = await _get_chain_head_hash()
    if head_hash:
        block_number = await _get_block_number_by_hash(head_hash)
        if block_number is not None:
            header_data_for_handler = {
                'hash': bytes.fromhex(head_hash[2:]),
                'number': block_number
            }
            await _handle_new_block_data(header_data_for_handler, 0, "initial_fetch")
            return True
        else:
            print("Could not get initial block number.")
            _update_status("Error during initial fetch (no block number)")
            return False
    else:
        print("Could not get initial head hash for initial fetch.")
        _update_status("Error during initial fetch (no head hash)")
        return False

async def start_fetching_loop_async():
    """Main async loop to connect, subscribe, and handle reconnections."""
    global _ipfs_fetch_process, _ipfs_fetch_queue, _ipfs_content
    _update_status("Starting async fetcher loop")
    print("Starting fetcher loop...")

    # Start IPFS fetch worker process
    manager = mp.Manager()
    _ipfs_fetch_queue = manager.Queue()
    _ipfs_content = manager.dict()
    _ipfs_fetch_process = mp.Process(target=utils.ipfs_fetch_worker, args=(_ipfs_fetch_queue, _ipfs_content))
    _ipfs_fetch_process.start()
    print("Started IPFS fetch worker process")

    subscription_active = False
    polling_mode = False
    last_head_hash = None

    # Queue to store incoming block headers (for processing only the latest block)
    block_queue = asyncio.Queue()
    latest_block_number = 0

    # Worker to process blocks from the queue
    async def process_blocks():
        nonlocal latest_block_number
        while not _exit_event.is_set():
            try:
                # Get the latest block header from the queue
                header_data = await block_queue.get()
                current_block_number = header_data.get('number')

                # Skip if this block is older than the latest processed block
                if current_block_number <= latest_block_number:
                    print(f"Skipping block #{current_block_number} (older than latest processed block #{latest_block_number})")
                    continue

                # Process the block
                await _handle_new_block_data(header_data, 0, "queue")
                latest_block_number = current_block_number

            except Exception as e:
                print(f"Error processing block from queue: {e}")

    # Start the block processing task
    asyncio.create_task(process_blocks())

    while not _exit_event.is_set():
        substrate = _get_substrate_interface()
        if not substrate:
            _update_status("Connection failed, retrying...")
            await asyncio.sleep(config.SUBSCRIPTION_RETRY_DELAY * 2)
            continue

        if not subscription_active and not polling_mode:
            success = await _initial_fetch_async()
            if not success:
                print("Initial fetch failed. Switching to polling mode.")
                polling_mode = True
                _update_status("Polling Mode")
                continue

            _update_status("Attempting to subscribe to finalized heads...")
            print("Attempting to subscribe to finalized heads...")
            try:
                loop = asyncio.get_running_loop()
                def sync_subscription_handler_wrapper(header_obj, update_nr, subscription_id_from_lib):
                    print(f"Received subscription update: Update #{update_nr}, Subscription ID: {subscription_id_from_lib}")
                    if not loop.is_closed():
                        # Add the block header to the queue
                        asyncio.run_coroutine_threadsafe(block_queue.put(header_obj), loop)

                sub_id = await _execute_query_async(substrate.chain_getFinalisedHead, sync_subscription_handler_wrapper, include_author=False)
                if sub_id:
                    print(f"Successfully subscribed with ID: {sub_id}")
                    _update_status("Subscribed")
                    subscription_active = True
                else:
                    print("Failed to subscribe to finalized heads. Switching to polling mode.")
                    polling_mode = True
                    _update_status("Polling Mode")
            except Exception as e:
                print(f"Error during subscription: {e}. Switching to polling mode.")
                polling_mode = True
                _update_status("Polling Mode")

        if polling_mode:
            head_hash = await _get_chain_head_hash()
            if head_hash and head_hash != last_head_hash:
                last_head_hash = head_hash
                block_number = await _get_block_number_by_hash(head_hash)
                if block_number is not None:
                    header_data_for_handler = {
                        'hash': bytes.fromhex(head_hash[2:]),
                        'number': block_number
                    }
                    await block_queue.put(header_data_for_handler)
            await asyncio.sleep(10)  # Increased polling interval
            continue

        if subscription_active:
            try:
                await asyncio.wait_for(_exit_event.wait(), timeout=5.0)
            except asyncio.TimeoutError:
                if substrate and hasattr(substrate, 'websocket') and \
                   (not substrate.websocket or not substrate.websocket.connected):
                    print("Detected disconnection (websocket). Re-initiating.")
                    subscription_active = False
                    _get_substrate_interface(force_reconnect=True)

    # Cleanup
    if _ipfs_fetch_process:
        print("Stopping IPFS fetch worker process...")
        if _ipfs_fetch_queue:
            _ipfs_fetch_queue.put(None)  # Send sentinel to stop the worker
        _ipfs_fetch_process.join(timeout=5)
        if _ipfs_fetch_process.is_alive():
            print("Forcing IPFS fetch worker process termination...")
            _ipfs_fetch_process.terminate()
            _ipfs_fetch_process.join()
        print("IPFS fetch worker process stopped")

    substrate = _get_substrate_interface()
    if substrate:
        print("Closing Substrate connection in async fetcher loop...")
        try:
            await _execute_query_async(substrate.close)
        except Exception as e:
            print(f"Error closing substrate connection: {e}")
    _update_status("Stopped")
    print("Async fetcher loop has stopped.")

async def stop_fetching_async():
    """Signals the fetching loop to stop and cleans up."""
    print("Stopping async storage fetcher...")
    _exit_event.set()
    await asyncio.sleep(0.1)