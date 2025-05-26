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
    logger.info(f"Fetching blockchain data at block hash: {block_hash}")

    # Fetch storage maps (collections)
    for module, function in STORAGE_MAPS_TO_FETCH:
        logger.info(f"Fetching map {module}::{function}...")
        result = await substrate_client.query_storage_map(
            module=module,
            function=function,
            block_hash=block_hash,
        )
        storage[f"{module}.{function}"] = result

    # Fetch storage values (single items)
    for module, function in STORAGE_VALUES_TO_FETCH:
        logger.info(f"Fetching value {module}::{function}...")
        result = await substrate_client.query_storage_value(
            module=module,
            function=function,
            block_hash=block_hash,
        )
        storage[f"{module}.{function}"] = result

    return storage
