"""Substrate storage fetcher service to fetch storage from the blockchain."""

from app.services.substrate_client import substrate_client
from app.utils.logging import logger

# Maps are collections of key-value pairs
STORAGE_MAPS_TO_FETCH = [
    ("ExecutionUnit", "NodeMetrics"),
    ("IpfsPallet", "MinerProfile"),
    ("IpfsPallet", "UserStorageRequests"),
    ("IpfsPallet", "MinerTotalFilesSize"),
    ("IpfsPallet", "MinerTotalFilesPinned"),
    ("Registration", "NodeRegistration"),
    ("Registration", "ColdkeyNodeRegistration"),
]

# Single values are not maps but individual storage items
STORAGE_VALUES_TO_FETCH = [
    ("IpfsPallet", "CurrentEpochValidator"),
]


async def fetch_and_store_blockchain_data(block_hash):
    """
    Fetch and store blockchain data from all configured storage maps and values.

    Args:
        block_hash: The block hash to fetch data at

    Returns:
        Dict containing the fetched storage data
    """
    if not substrate_client.connected:
        await substrate_client.connect()

    if not block_hash:
        error_msg = "Cannot fetch blockchain data: Invalid block hash"
        logger.error(error_msg)
        raise ValueError(error_msg)

    storage = {}
    # ===== STORAGE REQUEST TRACING - SUBSTRATE FETCHER START =====
    logger.info(f"🔗 SUBSTRATE_FETCHER: Starting blockchain data fetch at block hash: {block_hash[:16]}...")

    # Fetch storage maps (collections)
    for module, function in STORAGE_MAPS_TO_FETCH:
        logger.info(f"Fetching map {module}::{function}...")
        result = await substrate_client.query_storage_map(
            module=module,
            function=function,
            block_hash=block_hash,
        )
        storage[f"{module}.{function}"] = result
        
        # ===== STORAGE REQUEST TRACING - SUBSTRATE FETCHER =====
        if module == "IpfsPallet" and function == "UserStorageRequests":
            logger.info(f"🔗 SUBSTRATE_FETCHER: Fetched {len(result) if result else 0} UserStorageRequests from blockchain at block {block_hash[:16]}...")
            
            # Log sample storage requests for tracing
            if result:
                for i, item in enumerate(result[:3]):
                    try:
                        if isinstance(item, (list, tuple)) and len(item) >= 2:
                            key_data = item[0]
                            value_data = item[1]
                            if isinstance(key_data, (list, tuple)) and len(key_data) >= 2:
                                account = str(key_data[0])[:16] + "..."
                                request_hash = str(key_data[1])[:16] + "..."
                                logger.info(f"🔗 SUBSTRATE_FETCHER[{i+1}]: account={account} request_hash={request_hash}")
                    except Exception as e:
                        logger.debug(f"Could not log storage request details: {e}")
                
                if len(result) > 3:
                    logger.info(f"🔗 SUBSTRATE_FETCHER: ... and {len(result) - 3} more UserStorageRequests fetched")

    # Fetch storage values (single items)
    for module, function in STORAGE_VALUES_TO_FETCH:
        logger.info(f"Fetching value {module}::{function}...")
        result = await substrate_client.query_storage_value(
            module=module,
            function=function,
            block_hash=block_hash,
        )
        storage[f"{module}.{function}"] = result

    # ===== STORAGE REQUEST TRACING - SUBSTRATE FETCHER COMPLETE =====
    total_user_storage_requests = len(storage.get("IpfsPallet.UserStorageRequests", []))
    logger.info(f"🔗 SUBSTRATE_FETCHER_COMPLETE: Fetched all blockchain data including {total_user_storage_requests} UserStorageRequests")
    
    return storage
