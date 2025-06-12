"""Substrate client service for connecting to the blockchain node."""

import asyncio
import os
from typing import Any

from pydantic import BaseModel
from substrateinterface import SubstrateInterface

from app.utils.logging import logger

# Type registry for Hippius/IPFS network (exact same as working version)
TYPE_REGISTRY = {
    "types": {
        "AccountId": "AccountId32",
        "BlockNumber": "u32",
        "<T::AccountId, BlockNumberFor<T>>": "(AccountId32, u32)",
        "Option<(T::AccountId, BlockNumberFor<T>)>": "Option<(AccountId32, u32)>",
        "FileHash": "BoundedVec<u8, 350>",
        "FileName": "BoundedVec<u8, 350>",
        "BoundedVec<u8, 350>": "Vec<u8>",
        "BoundedVec<u8, 64>": "Vec<u8>",
        "BoundedVec<u8, ConstU32<64>>": "Vec<u8>",
        "BlockNumbers": "Vec<u32>",
        "MinerProfile": "BoundedVec<u8, ConstU32<64>>",
        "StorageRequest<AccountId, BlockNumber>": {
            "type": "struct",
            "type_mapping": [
                ["total_replicas", "u32"],
                ["owner", "AccountId32"],
                ["file_hash", "BoundedVec<u8, 350>"],
                ["file_name", "BoundedVec<u8, 350>"],
                ["last_charged_at", "u32"],
                ["created_at", "u32"],
                ["miner_ids", "Option<BoundedVec<BoundedVec<u8, 64>, 5>>"],
                ["selected_validator", "AccountId32"],
                ["is_assigned", "bool"],
            ],
        },
        "Option<StorageRequest<AccountId, BlockNumber>>": "Option<StorageRequest<AccountId32, u32>>",
        "NodeMetricsData": {
            "type": "struct",
            "type_mapping": [
                ["miner_id", "Vec<u8>"],
                ["bandwidth_mbps", "u32"],
                ["current_storage_bytes", "u64"],
                ["total_storage_bytes", "u64"],
                ["geolocation", "Vec<u8>"],
                ["successful_pin_checks", "u32"],
                ["total_pin_checks", "u32"],
                ["storage_proof_time_ms", "u32"],
                ["storage_growth_rate", "u32"],
                ["latency_ms", "u32"],
                ["total_latency_ms", "u32"],
                ["total_times_latency_checked", "u32"],
                ["avg_response_time_ms", "u32"],
                ["peer_count", "u32"],
                ["failed_challenges_count", "u32"],
                ["successful_challenges", "u32"],
                ["total_challenges", "u32"],
                ["uptime_minutes", "u32"],
                ["total_minutes", "u32"],
                ["consecutive_reliable_days", "u32"],
                ["recent_downtime_hours", "u32"],
                ["is_sev_enabled", "bool"],
                ["zfs_info", "Vec<Vec<u8>>"],
                ["ipfs_zfs_pool_size", "u128"],
                ["ipfs_zfs_pool_alloc", "u128"],
                ["ipfs_zfs_pool_free", "u128"],
                ["raid_info", "Vec<Vec<u8>>"],
                ["vm_count", "u32"],
                ["primary_network_interface", "Option<NetworkInterfaceInfo>"],
                ["disks", "Vec<DiskInfo>"],
                ["ipfs_repo_size", "u64"],
                ["ipfs_storage_max", "u64"],
                ["cpu_model", "Vec<u8>"],
                ["cpu_cores", "u32"],
                ["memory_mb", "u64"],
                ["free_memory_mb", "u64"],
                ["gpu_name", "Option<Vec<u8>>"],
                ["gpu_memory_mb", "Option<u32>"],
                ["hypervisor_disk_type", "Option<Vec<u8>>"],
                ["vm_pool_disk_type", "Option<Vec<u8>>"],
                ["disk_info", "Vec<DiskDetails>"],
            ],
        },
    },
}


def bounded_vec_to_string(bounded_vec: Any) -> str:
    """Converts a BoundedVec (list of integers, bytes, or hex string) to a UTF-8 string, with double-decoding for hex strings."""
    try:
        # Handle list/tuple of integers (BoundedVec as list of bytes)
        if isinstance(bounded_vec, (list, tuple)) and all(
            isinstance(x, int) for x in bounded_vec
        ):
            logger.debug(f"BoundedVec is list of integers: {bounded_vec}")
            byte_data = bytes(bounded_vec)
            return byte_data.decode("utf-8")

        # Handle bytes directly
        if isinstance(bounded_vec, bytes):
            logger.debug(f"BoundedVec is bytes: {bounded_vec}")
            return bounded_vec.decode("utf-8")

        # Handle string input
        if isinstance(bounded_vec, str):
            logger.debug(f"BoundedVec is string: {bounded_vec}")
            try:
                return bytes.fromhex(bounded_vec).decode("utf-8")
            except ValueError:
                # logger.warning(f"Invalid hex string format: {bounded_vec}, returning as string")
                return bounded_vec

    except Exception as e:
        logger.warning(
            f"Error converting BoundedVec to string: {e}, returning str representation"
        )
        return str(bounded_vec)


class Block(BaseModel):
    hash: str
    number: int


class SubstrateClient:
    """Client for interacting with a Substrate-based blockchain."""

    def __init__(self):
        """Initialize the Substrate client."""
        self.node_url = os.environ.get("NODE_URL", "wss://rpc.hippius.network")
        self.substrate = None
        self.connected = False
        self._shutdown_event = asyncio.Event()

    async def connect(self):
        """Connect to the Substrate node via WebSocket."""
        if self.connected:
            return

        logger.info(f"Connecting to Substrate node at {self.node_url}")
        self.substrate = SubstrateInterface(url=self.node_url, use_remote_preset=True)
        self.connected = True
        logger.info("Successfully connected to Substrate node")

    async def disconnect(self):
        """Disconnect from the Substrate node."""
        logger.info("Disconnecting from Substrate node")
        self._shutdown_event.set()
        if self.substrate:
            self.substrate.close()
            self.substrate = None
        self.connected = False
        logger.info("Disconnected from Substrate node")

    async def _execute_query_async(self, query_fn, *args, **kwargs):
        """Helper to run synchronous substrate queries in an executor for async context."""
        if not self.connected:
            await self.connect()

        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(None, lambda: query_fn(*args, **kwargs))

    async def _fetch_all_storage_and_filter(self, module, function, block_hash=None):
        """
        Fetch entire block storage and filter for specific storage function entries.
        This is the recommended approach for StorageDoubleMap when you want all entries.
        """
        if not self.connected:
            await self.connect()

        logger.info(
            f"Fetching entire storage for block {block_hash} to filter {module}.{function}"
        )

        # First, get the storage key prefix for IpfsPallet::UserStorageRequests
        # Substrate uses two x128 hash for pallet and storage function names
        storage_prefix = self._get_storage_key_prefix(module, function)

        logger.info(f"Using storage prefix: {storage_prefix}")

        # Get all keys with this prefix using state_getKeysPaged
        keys_result = self.substrate.rpc_request(
            method="state_getKeysPaged",
            params=[storage_prefix, 1000, storage_prefix, block_hash],
        )

        if "error" in keys_result:
            raise RuntimeError(f"Error fetching storage keys: {keys_result['error']}")

        storage_keys = keys_result["result"]
        logger.info(f"Found {len(storage_keys)} storage keys for {module}.{function}")

        # Fetch the values for these keys
        values_result = self.substrate.rpc_request(
            method="state_queryStorageAt",
            params=[storage_keys, block_hash],
        )

        if "error" in values_result:
            raise RuntimeError(
                f"Error fetching storage values: {values_result['error']}"
            )

        # Process the results without complex decoding - mimic query_map format
        processed_results = []

        for result_group in values_result["result"]:
            for change in result_group["changes"]:
                storage_key_hex = change[0]
                value_hex = change[1]

                if value_hex and value_hex != "0x":  # Skip empty values
                    # Extract the double map keys from the storage key
                    decoded_keys = self._decode_double_map_storage_key(
                        storage_key_hex,
                        storage_prefix,
                    )

                    # Create a simple value object that mimics what substrate returns
                    # We'll let the downstream processing handle the conversion
                    class SimpleValue:
                        def __init__(self, hex_data):
                            self.value = hex_data

                    value_obj = SimpleValue(value_hex)

                    # Return in query_map format: (key_tuple, value_obj)
                    processed_results.append((decoded_keys, value_obj))

        logger.info(
            f"Successfully processed {len(processed_results)} {module}.{function} entries"
        )
        return processed_results

    def _get_storage_key_prefix(self, module, function):
        """
        Generate the storage key prefix for a given module and function using substrate's own logic.
        """
        # Create storage key for the function (this will include the correct prefix)
        storage_key_obj = self.substrate.create_storage_key(module, function)

        # Extract just the prefix (pallet + function hash = 32 bytes = 64 hex chars)
        return storage_key_obj.to_hex()[:66]

    def _decode_double_map_storage_key(self, storage_key_hex, prefix):
        """
        Decode a StorageDoubleMap key to extract the two key components.
        For UserStorageRequests: (owner_account_id, file_hash)
        """
        # Remove the prefix to get the key data
        key_data_hex = storage_key_hex[len(prefix) :]

        # The first 32 bytes (64 hex chars) should be the AccountId32
        account_id_hex = key_data_hex[:64]

        # Skip the hash prefix (next 32 hex chars for blake2b_256 hasher) and get file hash
        file_hash_data = key_data_hex[96:]

        # Convert file hash using bounded_vec_to_string
        file_hash = bounded_vec_to_string(file_hash_data)

        return (account_id_hex, file_hash)

    async def query_storage_map(self, module, function, block_hash=None, **_kwargs):
        """
        Query chain storage map (collection).

        Args:
            module: Storage module name
            function: Storage function name
            block_hash: Optional block hash to query at specific block
            **kwargs: Additional parameters for the query

        Returns:
            The query result as a list of (key, value) tuples
        """
        if not self.connected:
            await self.connect()

        # Special handling for UserStorageRequests - fetch entire storage and filter
        if module == "IpfsPallet" and function == "UserStorageRequests":
            result = await self._fetch_all_storage_and_filter(
                module,
                function,
                block_hash,
            )
        else:
            result = self.substrate.query_map(
                module,
                function,
                block_hash=block_hash,
            )

        processed_result = []
        for key_storage_obj, value_storage_obj in result:
            # Convert ScaleType objects to Python dictionaries where possible
            key = (
                key_storage_obj.value
                if hasattr(key_storage_obj, "value")
                else key_storage_obj
            )
            value = (
                value_storage_obj.value
                if hasattr(value_storage_obj, "value")
                else value_storage_obj
            )

            # Special handling for UserStorageRequests double map keys (like working version)
            if module == "IpfsPallet" and function == "UserStorageRequests":
                # Handle StorageDoubleMap: key_storage_obj is a tuple (owner_account_id, file_hash)
                if (
                    isinstance(key_storage_obj, (tuple, list))
                    and len(key_storage_obj) == 2
                ):
                    owner_account_id = str(key_storage_obj[0])  # SS58 address
                    file_hash = str(
                        key_storage_obj[1],
                    )  # Already converted in _decode_double_map_storage_key
                    key = (owner_account_id, file_hash)
                    logger.debug(f"UserStorageRequests key: {key}, value: {value}")
                else:
                    logger.warning(
                        f"Unexpected key format for UserStorageRequests: {key_storage_obj}",
                    )

            processed_result.append((key, value))

        logger.info(
            f"Found {len(processed_result)} results for map {module}.{function}"
        )
        return processed_result

    async def query_storage_value(self, module, function, block_hash=None, **kwargs):
        """
        Query single chain storage value.

        Args:
            module: Storage module name
            function: Storage function name
            block_hash: Optional block hash to query at specific block
            **kwargs: Additional parameters for the query

        Returns:
            The query result as a single value
        """
        if not self.connected:
            await self.connect()

        result = self.substrate.query(
            module=module,
            storage_function=function,
            block_hash=block_hash,
            **kwargs,
        )

        # Convert ScaleType objects to Python values
        value = result.value if hasattr(result, "value") else result

        logger.info(f"Retrieved value for {module}.{function}")
        return value

    async def check_user_balance(self, account_id: str) -> int:
        """
        Check user account balance (free balance) from System.Account storage.

        Args:
            account_id: The account ID to check

        Returns:
            Free balance as integer (0 if account doesn't exist)
        """
        if not self.connected:
            await self.connect()

        result = await self._execute_query_async(
            self.substrate.query,
            module="Credits",
            storage_function="FreeCredits",
            params=[account_id],

        )

        if not result or result.value is None:
            return 0

        try:
            balance_value = result.value
            if hasattr(balance_value, 'value'):
                balance_value = balance_value.value
            return int(balance_value)
        except Exception as e:
            logger.exception(f"Balance parsing error for {account_id}: {e}")
            return 0

    async def check_multiple_user_balances(self, account_ids: list[str]) -> dict[str, int]:
        """
        Check balances for multiple users in parallel.
        
        Args:
            account_ids: List of account IDs to check
            
        Returns:
            Dictionary mapping account_id -> balance (0 if account doesn't exist)
        """
        if not self.connected:
            await self.connect()

        semaphore = asyncio.Semaphore(20)  # Limit concurrent queries

        async def _check_single_balance(account_id: str) -> tuple[str, int]:
            async with semaphore:
                balance = await self.check_user_balance(account_id)
                return account_id, balance

        tasks = [_check_single_balance(account_id) for account_id in account_ids]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        balance_map = {}
        failed_count = 0

        for result in results:
            if isinstance(result, Exception):
                failed_count += 1
                logger.error(f"Failed to check balance: {result}")
            else:
                account_id, balance = result
                balance_map[account_id] = balance

        if failed_count > 0:
            logger.warning(f"Failed to check balances for {failed_count} accounts")

        logger.info(f"Successfully checked balances for {len(balance_map)} accounts in parallel")
        return balance_map


substrate_client = SubstrateClient()


async def init_substrate_client():
    """Initialize and connect the Substrate client."""
    await substrate_client.connect()


async def close_substrate_client():
    """Disconnect the Substrate client."""
    await substrate_client.disconnect()


async def fetch_current_block() -> Block:
    """
    Get the current block information from the chain.

    Returns:
        Block object containing hash and number
    """
    if not substrate_client.connected:
        await substrate_client.connect()

    # Get the latest block hash
    response = substrate_client.substrate.rpc_request(
        "chain_getBlockHash",
        [],
    )

    block_hash = response.get("result")

    # Get the block header
    header_response = substrate_client.substrate.rpc_request(
        "chain_getHeader",
        [block_hash],
    )

    header = header_response.get("result")
    block_number = int(header["number"], 16)
    logger.info(f"Current block: #{block_number} ({block_hash})")

    return Block(hash=block_hash, number=block_number)
