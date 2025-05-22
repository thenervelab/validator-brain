"""Configuration utilities."""
import os


def get_ipfs_node_url():
    """
    Get the IPFS node URL from environment variables or use default.
    
    Returns:
        IPFS node URL as string
    """
    return os.environ.get("IPFS_NODE_URL", "http://localhost:5001").rstrip('/')


def get_ipfs_timeout(operation_type="default"):
    """
    Get timeout duration for IPFS operations based on operation type.
    
    Args:
        operation_type: Type of operation (default, ping, dht, refs)
    
    Returns:
        Timeout in seconds
    """
    timeout_map = {"default": int(os.environ.get("IPFS_TIMEOUT_SECONDS", 10)),
        "ping": int(os.environ.get("IPFS_TIMEOUT_SECONDS", 10)),
        "dht": int(os.environ.get("IPFS_DHT_TIMEOUT_SECONDS", 60)),
        "refs": int(os.environ.get("IPFS_REFS_TIMEOUT_SECONDS", 30)),
        "fetch": int(os.environ.get("IPFS_FETCH_TIMEOUT", 60))}

    return timeout_map.get(operation_type, timeout_map["default"])


def parse_env_bool(env_var, default=False):
    """
    Parse a boolean environment variable.
    
    Args:
        env_var: Name of the environment variable
        default: Default value if variable is not set
    
    Returns:
        Boolean value of the environment variable
    """
    value = os.environ.get(env_var)
    if value is None:
        return default

    return value.lower() in ('true', 'yes', '1', 't', 'y')


def get_epoch_block_interval():
    """
    Get the epoch block interval from environment variables or use default.
    
    Returns:
        Epoch block interval as integer
    """
    return int(os.environ.get("EPOCH_BLOCK_INTERVAL", 100))