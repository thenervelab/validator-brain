"""Substrate client service for connecting to the blockchain node."""

import asyncio
import os

from pydantic import BaseModel
from substrateinterface import SubstrateInterface

from app.utils.logging import logger


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
        self.substrate = SubstrateInterface(
            url=self.node_url,
            ss58_format=42,
            type_registry_preset="substrate-node-template",
        )
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

    async def query_storage_map(self, module, function, block_hash=None, **kwargs):
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

        try:
            result = self.substrate.query_map(
                module=module,
                storage_function=function,
                block_hash=block_hash,
                **kwargs,
            )

            processed_result = []
            for key_storage_obj, value_storage_obj in result:
                # Convert ScaleType objects to Python dictionaries where possible
                key = key_storage_obj.value if hasattr(key_storage_obj, 'value') else key_storage_obj
                value = value_storage_obj.value if hasattr(value_storage_obj, 'value') else value_storage_obj
                
                processed_result.append((key, value))
                
            logger.info(
                f"Found {len(processed_result)} results for map {module}.{function}"
            )
            return processed_result
        except Exception as e:
            logger.error(f"Error querying storage map {module}.{function}: {str(e)}")
            raise
            
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

        try:
            result = self.substrate.query(
                module=module,
                storage_function=function,
                block_hash=block_hash,
                **kwargs,
            )
            
            # Convert ScaleType objects to Python values
            value = result.value if hasattr(result, 'value') else result
            
            logger.info(f"Retrieved value for {module}.{function}")
            return value
        except Exception as e:
            logger.error(f"Error querying storage value {module}.{function}: {str(e)}")
            raise

    async def get_latest_block_hash(self):
        """
        Get the latest block hash directly from the chain.

        Returns:
            The latest block hash as a string
        """
        if not self.connected:
            await self.connect()

        result = self.substrate.rpc_request("chain_getBlockHash", []).get("result")
        logger.info(f"Latest block hash: {result}")
        return result


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
