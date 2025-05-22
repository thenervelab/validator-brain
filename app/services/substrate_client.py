"""Substrate client service for connecting to the blockchain node."""
import asyncio
import os
from typing import Optional, Dict, Any, List, Tuple

from substrateinterface import SubstrateInterface

from app.db.connection import get_db_pool
from app.db.sql import load_query
from app.utils.logging import logger


class SubstrateClient:
    """Client for interacting with a Substrate-based blockchain."""
    
    def __init__(self):
        """Initialize the Substrate client."""
        self.node_url = os.environ.get("NODE_URL", "wss://rpc.hippius.network")
        self.substrate = None
        self.connected = False
        self._shutdown_event = asyncio.Event()
        self._subscription_task = None
        self._subscription_id = None
        
    async def connect(self):
        """Connect to the Substrate node via WebSocket."""
        if self.connected:
            return
            
        logger.info(f"Connecting to Substrate node at {self.node_url}")
        
        # Create SubstrateInterface instance
        # Since SubstrateInterface is synchronous, run it in a thread
        loop = asyncio.get_event_loop()
        self.substrate = await loop.run_in_executor(
            None,
            lambda: SubstrateInterface(
                url=self.node_url,
                ss58_format=42,  # Default Substrate format
                type_registry_preset='substrate-node-template'
            )
        )
        
        # Test connection by getting chain properties
        chain_props = await loop.run_in_executor(
            None,
            lambda: self.substrate.get_chain_properties()
        )
        
        chain_name = chain_props.get('ss58Format', 'unknown')
        logger.info(f"Connected to chain with SS58 format: {chain_name}")
        
        self.connected = True
        self._shutdown_event.clear()
        logger.info("Successfully connected to Substrate node")
        
    async def disconnect(self):
        """Disconnect from the Substrate node."""
        if not self.connected:
            return
            
        logger.info("Disconnecting from Substrate node")
        
        # Cancel subscription if active
        if self._subscription_id and self.substrate:
            await asyncio.get_event_loop().run_in_executor(
                None,
                lambda: self.substrate.rpc_request(
                    'chain_unsubscribeNewHeads',
                    [self._subscription_id]
                )
            )
        
        # Signal block subscription to stop
        self._shutdown_event.set()
        
        # Close the connection
        if self.substrate:
            self.substrate.close()
            self.substrate = None
        
        self.connected = False
        logger.info("Disconnected from Substrate node")
    
    async def subscribe_to_blocks(self):
        """
        Subscribe to finalized blocks from the Substrate chain.
        """
        if not self.connected:
            await self.connect()
            
        logger.info("Subscribing to finalized blocks")
        
        # Start the subscription task
        self._subscription_task = asyncio.create_task(self._handle_block_subscription())
        return self._subscription_task
    
    async def _handle_block_subscription(self):
        """
        Handle the block subscription.
        
        This continuously listens for new finalized blocks and processes them.
        """
        loop = asyncio.get_event_loop()
        db_pool = get_db_pool()
        
        # Subscribe to finalized heads
        result = await loop.run_in_executor(
            None,
            lambda: self.substrate.rpc_request('chain_subscribeFinalizedHeads', [])
        )
        
        self._subscription_id = result.get('result')
        logger.info(f"Subscribed to finalized blocks with ID: {self._subscription_id}")
        
        # Process incoming blocks until shutdown
        while not self._shutdown_event.is_set():
            # Get the next block from the subscription
            response = await loop.run_in_executor(
                None,
                lambda: self.substrate.rpc_request('chain_getFinalizedHead', [])
            )
            
            block_hash = response.get('result')
            
            # Get block details
            block = await loop.run_in_executor(
                None,
                lambda: self.substrate.get_block(block_hash=block_hash)
            )
            
            block_number = block['header']['number']
            logger.info(f"Processing finalized block {block_number} with hash {block_hash}")
            
            # Store block in database
            async with db_pool.acquire() as conn:
                await conn.execute(
                    load_query("store_block"),
                    block_number,
                    block_hash
                )
                
                # Check for epoch change
                epoch_length = int(os.environ.get("EPOCH_BLOCK_INTERVAL", "100"))
                if block_number % epoch_length == 1:
                    await self._process_epoch_change(conn, block_number, epoch_length)
    
    async def _process_epoch_change(self, conn, block_number, epoch_length):
        """
        Process an epoch change.
        
        Args:
            conn: Database connection
            block_number: The current block number
            epoch_length: The number of blocks in an epoch
        """
        # Calculate the epoch
        epoch = block_number // epoch_length
        
        # Use the validator account ID from environment
        validator_id = os.environ.get("VALIDATOR_ACCOUNT_ID")
        
        if validator_id:
            # Store the current validator
            await conn.execute(
                load_query("set_current_validator"),
                epoch,
                validator_id,
                block_number
            )
            
            logger.info(f"Set validator {validator_id} for epoch {epoch}")
    
    async def query_storage(self, module, function, block_hash=None, **kwargs):
        """
        Query chain storage.
        
        Args:
            module: Storage module name
            function: Storage function name
            block_hash: Optional block hash to query at specific block
            **kwargs: Additional parameters for the query
            
        Returns:
            The query result
        """
        if not self.connected:
            await self.connect()
            
        loop = asyncio.get_event_loop()
        result = await loop.run_in_executor(
            None,
            lambda: self.substrate.query(
                module=module,
                storage_function=function,
                block_hash=block_hash,
                **kwargs
            )
        )
        return result
    
    async def get_block_hash(self, block_number):
        """
        Get the block hash for a specific block number.
        
        Args:
            block_number: The block number
            
        Returns:
            The block hash
        """
        if not self.connected:
            await self.connect()
            
        loop = asyncio.get_event_loop()
        result = await loop.run_in_executor(
            None,
            lambda: self.substrate.get_block_hash(block_number)
        )
        return result
    
    async def get_current_validator(self) -> Tuple[bool, Optional[str]]:
        """
        Check if the current node is the validator for the current epoch.
        
        Returns:
            Tuple of (is_validator, validator_account_id)
        """
        if not self.connected:
            await self.connect()
            
        # Get the current validator from the chain
        local_account_id = os.environ.get("VALIDATOR_ACCOUNT_ID")
        
        if not local_account_id:
            logger.warning("No validator account ID provided")
            return False, None
            
        # Query the chain for current validator
        current_validator = await self.query_storage(
            module="Validator",
            function="CurrentValidator"
        )
        
        # Convert the result to a usable format
        if current_validator:
            validator_account_id = current_validator.value
            
            # Check if our account is the validator
            is_validator = validator_account_id == local_account_id
            return is_validator, validator_account_id
            
        return False, None


# Singleton instance
substrate_client = SubstrateClient()


async def init_substrate_client():
    """Initialize and connect the Substrate client."""
    await substrate_client.connect()


async def close_substrate_client():
    """Disconnect the Substrate client."""
    await substrate_client.disconnect()