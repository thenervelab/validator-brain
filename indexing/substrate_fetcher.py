import asyncio
from substrateinterface import SubstrateInterface
from substrateinterface.exceptions import SubstrateRequestException
import sys
import os
from typing import Any # Ensure Any is imported for type hints

# Adjust path to import config
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))
from config import SUBSTRATE_NODE_URL

# For now, we'll use a global instance. In a more complex app, you might manage this differently.
_substrate_instance = None

def get_substrate_interface():
    """Initializes and returns a SubstrateInterface instance."""
    global _substrate_instance
    if _substrate_instance is None:
        print(f"Connecting to Substrate node at {SUBSTRATE_NODE_URL}...")
        try:
            _substrate_instance = SubstrateInterface(url=SUBSTRATE_NODE_URL)
            print("Successfully connected to Substrate node.")
        except ConnectionRefusedError:
            print(f"Connection refused. Is the Substrate node running at {SUBSTRATE_NODE_URL}?")
            _substrate_instance = None
        except Exception as e:
            print(f"Error connecting to Substrate node: {e}")
            _substrate_instance = None
    return _substrate_instance

async def _execute_query(query_fn, *args, **kwargs):
    """Helper to run synchronous substrate queries in an executor."""
    substrate = get_substrate_interface()
    if not substrate:
        return None
    
    loop = asyncio.get_running_loop()
    try:
        return await loop.run_in_executor(None, lambda: query_fn(*args, **kwargs))
    except SubstrateRequestException as e:
        # Extracting module and method name for better error logging if possible
        # This is a bit simplistic as query_fn is a bound method
        fn_name = getattr(query_fn, '__name__', 'query') 
        print(f"Substrate request error during {fn_name}: {e}")
        return None
    except Exception as e:
        fn_name = getattr(query_fn, '__name__', 'query') 
        print(f"An unexpected error occurred during {fn_name}: {e}")
        return None

async def get_chain_head_hash() -> str | None:
    """Asynchronously fetches the latest block hash."""
    substrate = get_substrate_interface()
    if not substrate:
        return None
    result = await _execute_query(substrate.get_chain_head)
    # If substrate.get_chain_head() directly returns the hex string (as the error suggests)
    # or if it returns an object with a .value attribute that is the hex string.
    if isinstance(result, str):
        return result
    elif result and hasattr(result, 'value') and isinstance(result.value, str):
        return result.value
    elif result and hasattr(result, 'value') and isinstance(result.value, bytes): # Should not happen for H256 normally
        return result.value.hex()
    elif isinstance(result, bytes): # Should not happen for H256 normally
        return result.hex()
    return None

async def get_block_hash_by_number(block_number: int) -> str | None:
    substrate = get_substrate_interface()
    if not substrate: return None
    # substrate.get_block_hash expects block_number as a positional argument
    result_hex_str = await _execute_query(substrate.get_block_hash, block_number)
    return result_hex_str # This is already a hex string or None

async def get_block_number_by_hash(block_hash: str) -> int | None:
    substrate = get_substrate_interface()
    if not substrate: return None
    # get_block_header returns a dict, we need block_header['header']['number']
    block_header_dict = await _execute_query(substrate.get_block_header, block_hash=block_hash)
    if block_header_dict and 'header' in block_header_dict and 'number' in block_header_dict['header']:
        return block_header_dict['header']['number']
    print(f"Could not get block number from header for hash {block_hash}")
    return None

def _extract_profile_cid(value_content: Any) -> str | None:
    cid_str = None
    if isinstance(value_content, str):
        cid_str = value_content
    elif isinstance(value_content, bytes):
        try:
            cid_str = value_content.decode('utf-8')
        except UnicodeDecodeError:
            print(f"Warning: Profile Bytes content is not valid UTF-8, returning hex: {value_content.hex()[:60]}...")
            return value_content.hex() # Fallback for non-utf8 bytes
    elif value_content is None:
        return None
    else:
        print(f"Warning: Unexpected type for profile content: {type(value_content)}. Converting to str.")
        cid_str = str(value_content)
    
    if cid_str:
        # Strip leading/trailing quotes which might come from some on-chain string encodings
        cid_str = cid_str.strip('"\'') 
    return cid_str

# --- Fetching functions based on substrate_storage.md --- 

async def get_all_miner_profiles(block_hash=None):
    """Fetches all miner profiles (ipfsPallet.minerProfile)."""
    substrate = get_substrate_interface()
    if not substrate: return [] # Return empty list on failure for map queries
    # Assuming MinerProfile is a StorageMap: AccountId => Bytes
    # query_map returns a list of tuples: [(key_bytes, value_object), ...]
    # We need to decode keys (AccountId) if needed for processing.
    print(f"Fetching all miner profiles at block: {block_hash or 'latest'}...")
    raw_profiles_data = await _execute_query(substrate.query_map, 'IpfsPallet', 'MinerProfile', block_hash=block_hash)
    if raw_profiles_data is None: return []
    
    profiles = []
    for key_obj, value_obj in raw_profiles_data:
        try:
            miner_id_str = str(key_obj) 
            profile_cid_str = _extract_profile_cid(value_obj.value)
            profiles.append({"miner_id_bytes": miner_id_str, "profile_cid": profile_cid_str})
        except AttributeError as e:
            print(f"AttributeError processing miner profile key/value: {key_obj}, {value_obj}. Error: {e}. Skipping item.")
        except Exception as e:
            print(f"Unexpected error processing miner profile item: {key_obj}, {value_obj}. Error: {e}. Skipping item.")
    print(f"Fetched {len(profiles)} miner profiles.")
    return profiles

async def get_all_miner_states(block_hash=None):
    """Fetches all miner states (ipfsPallet.minerStates)."""
    substrate = get_substrate_interface()
    if not substrate: return []
    # Assuming MinerStates is a StorageMap: AccountId => IpfsPalletMinerState
    print(f"Fetching all miner states at block: {block_hash or 'latest'}...")
    raw_states = await _execute_query(substrate.query_map, 'IpfsPallet', 'MinerStates', block_hash=block_hash)
    if raw_states is None: return []

    states = []
    for key_obj, value_obj in raw_states:
        # value_obj here is the decoded IpfsPalletMinerState structure
        try:
            miner_id_str = str(key_obj)
            # The state data might be complex; value_obj.value gives the decoded structure.
            # For DB storage (if model.state_data is string), you might need to serialize it (e.g. json.dumps(value_obj.value))
            states.append({"miner_id_bytes": miner_id_str, "state": value_obj.value})
        except Exception as e:
            print(f"Unexpected error processing miner state item: {key_obj}, {value_obj}. Error: {e}. Skipping item.")
    print(f"Fetched {len(states)} miner states.")
    return states

async def get_all_user_profiles(block_hash=None):
    """Fetches all user profiles (ipfsPallet.userProfile)."""
    substrate = get_substrate_interface()
    if not substrate: return []
    # Assuming UserProfile is a StorageMap: AccountId => Bytes
    print(f"Fetching all user profiles at block: {block_hash or 'latest'}...")
    raw_profiles_data = await _execute_query(substrate.query_map, 'IpfsPallet', 'UserProfile', block_hash=block_hash)
    if raw_profiles_data is None: return []

    profiles = []
    for key_obj, value_obj in raw_profiles_data:
        try:
            user_id_str = str(key_obj)
            profile_cid_str = _extract_profile_cid(value_obj.value)
            profiles.append({"user_id_bytes": user_id_str, "profile_cid": profile_cid_str})
        except Exception as e:
            print(f"Unexpected error processing user profile item for {str(key_obj)}: {e}. Value object: {value_obj}")
    print(f"Fetched {len(profiles)} user profiles.")
    return profiles

async def get_per_miner_total_files_pinned(block_hash=None):
    substrate = get_substrate_interface()
    if not substrate: return []
    print(f"Fetching per-miner total files pinned at block: {block_hash or 'latest'}...")
    # StorageMap<_, Blake2_128Concat, BoundedVec<u8, ConstU32<MAX_NODE_ID_LENGTH>>, u32, ValueQuery>
    raw_data = await _execute_query(substrate.query_map, 'IpfsPallet', 'MinerTotalFilesPinned', block_hash=block_hash)
    if raw_data is None: return []
    pinned_counts = []
    for key_obj, value_obj in raw_data:
        try:
            miner_id_str = str(key_obj)
            count = value_obj.value # value_obj is a ScaleType obj, .value gives the Python native type (u32 -> int)
            pinned_counts.append({"miner_id_bytes": miner_id_str, "total_files_pinned": count})
        except Exception as e:
            print(f"Unexpected error processing miner total_files_pinned item: {key_obj}, {value_obj}. Error: {e}. Skipping.")
    print(f"Fetched {len(pinned_counts)} per-miner total_files_pinned records.")
    return pinned_counts

async def get_per_miner_total_files_size(block_hash=None):
    substrate = get_substrate_interface()
    if not substrate: return []
    print(f"Fetching per-miner total files size at block: {block_hash or 'latest'}...")
    # StorageMap<_, Blake2_128Concat, BoundedVec<u8, ConstU32<MAX_NODE_ID_LENGTH>>, u128, ValueQuery>
    raw_data = await _execute_query(substrate.query_map, 'IpfsPallet', 'MinerTotalFilesSize', block_hash=block_hash)
    if raw_data is None: return []
    file_sizes = []
    for key_obj, value_obj in raw_data:
        try:
            miner_id_str = str(key_obj)
            size = value_obj.value # u128 -> int
            file_sizes.append({"miner_id_bytes": miner_id_str, "total_files_size": size})
        except Exception as e:
            print(f"Unexpected error processing miner total_files_size item: {key_obj}, {value_obj}. Error: {e}. Skipping.")
    print(f"Fetched {len(file_sizes)} per-miner total_files_size records.")
    return file_sizes

async def get_user_storage_requests(block_hash=None):
    substrate = get_substrate_interface()
    if not substrate: return []
    print(f"Fetching UserStorageRequests (IpfsPallet.UserStorageRequests) at block: {block_hash or 'latest'}...")
    raw_data = await _execute_query(substrate.query_map, 'IpfsPallet', 'UserStorageRequests', block_hash=block_hash)
    if raw_data is None: return []
    requests = []
    for (key1_obj, key2_obj), value_obj in raw_data:
        if value_obj.value is None: 
            continue
        try:
            owner_id_str = str(key1_obj) 
            manifest_cid_str = _extract_profile_cid(key2_obj.value) # Assuming key2_obj (FileHash) also needs robust extraction
            if not manifest_cid_str: # If CID extraction fails for key2
                print(f"Could not extract valid manifest CID from UserStorageRequests key2: {key2_obj}. Skipping.")
                continue
            requests.append({
                "owner_id": owner_id_str,
                "manifest_cid": manifest_cid_str, # Renamed from file_hash for clarity
                "request_details": value_obj.value 
            })
        except Exception as e:
            print(f"Error processing UserStorageRequest item: K1={key1_obj}, K2={key2_obj}, V={value_obj}. Error: {e}")
    print(f"Fetched {len(requests)} UserStorageRequests.")
    return requests

async def get_rebalance_requests(block_hash=None):
    substrate = get_substrate_interface()
    if not substrate: return []
    print(f"Fetching rebalance requests at block: {block_hash or 'latest'}...")
    result = await _execute_query(substrate.query, 'IpfsPallet', 'RebalanceRequest', block_hash=block_hash)
    # result.value will be a list of IpfsPalletRebalanceRequestItem objects
    value = result.value if result and hasattr(result, 'value') else []
    print(f"Fetched {len(value if value is not None else [])} rebalance requests.")
    return value if value is not None else []

async def get_global_unpin_requests(block_hash=None):
    substrate = get_substrate_interface()
    if not substrate: return []
    print(f"Fetching global unpin requests (IpfsPallet.UnpinRequests) at block: {block_hash or 'latest'}...")
    result = await _execute_query(substrate.query, 'IpfsPallet', 'UnpinRequests', block_hash=block_hash)
    raw_value = result.value if result and hasattr(result, 'value') else []
    value = []
    if raw_value: 
        for item_obj in raw_value: # item_obj is a FileHash (BoundedVec)
            cid_str = _extract_profile_cid(item_obj.value) # Use the helper
            if cid_str:
                value.append(cid_str)
            else:
                print(f"Warning: Global UnpinRequest item (FileHash) could not be converted to string CID: {item_obj}")
    print(f"Fetched {len(value)} global unpin requests.")
    return value

async def get_user_unpin_requests(block_hash=None):
    substrate = get_substrate_interface()
    if not substrate: return []
    print(f"Fetching UserUnpinRequests at block: {block_hash or 'latest'}...")
    # StorageValue<_, BoundedVec<StorageUnpinRequest<T::AccountId>, ConstU32<MAX_UNPIN_REQUESTS>>, ValueQuery>
    result = await _execute_query(substrate.query, 'IpfsPallet', 'UserUnpinRequests', block_hash=block_hash)
    value = result.value if result and hasattr(result, 'value') else [] # This is a list of StorageUnpinRequest structs
    # Each item in 'value' will be a dict-like object representing StorageUnpinRequest
    # e.g., item['owner'], item['file_hash'], item['selected_validator']
    # The file_hash within StorageUnpinRequest might need conversion/handling similar to other FileHash types.
    print(f"Fetched {len(value if value is not None else [])} UserUnpinRequests.")
    return value if value is not None else []

async def get_execution_node_metrics(block_hash=None):
    substrate = get_substrate_interface()
    if not substrate: return []
    # *** IMPORTANT: Confirm these names with your actual runtime pallet & storage item names ***
    pallet_name = 'ExecutionUnit'  # Or 'PalletExecutionUnit' or whatever it's named in construct_runtime!
    storage_name = 'NodeMetrics'   # The name of the StorageMap for node metrics
    # **************************************************************************************
    print(f"Fetching node metrics ({pallet_name}.{storage_name}) at block: {block_hash or 'latest'}...")
    raw_data = await _execute_query(substrate.query_map, pallet_name, storage_name, block_hash=block_hash)
    if raw_data is None: 
        print(f"Query for {pallet_name}.{storage_name} returned None.")
        return []
    metrics_list = []
    for key_obj, value_obj in raw_data:
        if value_obj.value is None: # Value is Option<PalletExecutionUnitNodeMetricsData>
            continue
        try:
            node_id_str = str(key_obj) # Assuming key is AccountId or similar decodable to SS58
            metrics_data = value_obj.value # This is the PalletExecutionUnitNodeMetricsData struct
            metrics_list.append({"miner_id_bytes": node_id_str, "metrics_data": metrics_data})
        except Exception as e:
            print(f"Error processing {pallet_name}.{storage_name} item: K={key_obj}, V={value_obj}. Error: {e}")
    print(f"Fetched {len(metrics_list)} {storage_name} records.")
    return metrics_list

async def fetch_all_chain_data(block_hash=None):
    """Fetches all relevant data points from the chain for a given block_hash."""
    print(f"--- Starting full data fetch for block: {block_hash or 'latest'} ---")
    data = {
        "block_hash": block_hash,
        "miner_profiles": await get_all_miner_profiles(block_hash=block_hash),
        "miner_states": await get_all_miner_states(block_hash=block_hash),
        "per_miner_total_files_pinned": await get_per_miner_total_files_pinned(block_hash=block_hash),
        "per_miner_total_files_size": await get_per_miner_total_files_size(block_hash=block_hash),
        "rebalance_requests": await get_rebalance_requests(block_hash=block_hash),
        "user_storage_requests": await get_user_storage_requests(block_hash=block_hash),
        "global_unpin_requests": await get_global_unpin_requests(block_hash=block_hash),
        "user_unpin_requests": await get_user_unpin_requests(block_hash=block_hash),
        "user_profiles": await get_all_user_profiles(block_hash=block_hash),
        "execution_node_metrics": await get_execution_node_metrics(block_hash=block_hash),
    }
    print(f"--- Completed full data fetch for block: {block_hash or 'latest'} ---")
    return data

async def main_test():
    """Test function to demonstrate usage."""
    print(f"Using Substrate node: {SUBSTRATE_NODE_URL}")
    
    # 1. Get current chain head
    latest_block_hash = await get_chain_head_hash()
    if not latest_block_hash:
        print("Could not fetch latest block hash. Exiting test.")
        return
    print(f"Latest block hash: {latest_block_hash}")
    latest_block_number = await get_block_number_by_hash(latest_block_hash)
    if latest_block_number:
        print(f"Latest block number: {latest_block_number}")
        block_hash_from_num = await get_block_hash_by_number(latest_block_number)
        print(f"Block hash from number {latest_block_number}: {block_hash_from_num}")

    # 2. Fetch all data at the latest block hash
    all_data = await fetch_all_chain_data(block_hash=latest_block_hash)

    # You can print parts of all_data to verify, e.g.:
    # print("\nSample of fetched data:")
    # print(f"Total Miner Profiles: {len(all_data.get('miner_profiles', []))}")
    # print(f"Total Files Pinned: {all_data.get('miner_total_files_pinned')}")
    # print(f"Pinning Requests: {all_data.get('pinning_requests')[:5]} ...") # Print first 5 pinning requests

    # Close the substrate connection when done
    global _substrate_instance
    if _substrate_instance:
        print("Closing Substrate connection...")
        _substrate_instance.close()
        _substrate_instance = None
        print("Substrate connection closed.")

if __name__ == '__main__':
    # This allows running this file directly for testing the fetcher.
    # In the full app, these functions would be called by an indexing service.
    asyncio.run(main_test()) 