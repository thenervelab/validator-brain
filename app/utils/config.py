"""Configuration utilities."""

import os

# Blockchain node URL
NODE_URL = os.environ.get("NODE_URL", "wss://rpc.hippius.network")
IPFS_GATEWAY = os.environ.get("IPFS_GATEWAY", "https://get.hippius.network")

def get_ipfs_node_url():
    """
    Get the IPFS node URL from environment variables or use default.

    Returns:
        IPFS node URL as string
    """
    return os.environ.get("IPFS_NODE_URL", "http://localhost:5001").rstrip("/")


def get_ipfs_timeout(operation_type="default"):
    """
    Get timeout duration for IPFS operations based on operation type.

    Args:
        operation_type: Type of operation (default, ping, dht, refs)

    Returns:
        Timeout in seconds
    """
    timeout_map = {
        "default": int(os.environ.get("IPFS_TIMEOUT_SECONDS", 10)),
        "ping": int(os.environ.get("IPFS_TIMEOUT_SECONDS", 10)),
        "dht": int(os.environ.get("IPFS_DHT_TIMEOUT_SECONDS", 60)),
        "refs": int(os.environ.get("IPFS_REFS_TIMEOUT_SECONDS", 30)),
        "fetch": int(os.environ.get("IPFS_FETCH_TIMEOUT", 60)),
    }

    return timeout_map.get(operation_type, timeout_map["default"])


def get_epoch_block_interval():
    """
    Get the epoch block interval from environment variables or use default.

    Returns:
        Epoch block interval as integer
    """
    return int(os.environ.get("EPOCH_BLOCK_INTERVAL", 100))
