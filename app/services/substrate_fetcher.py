"""Substrate storage fetcher service to fetch storage from the blockchain."""
import asyncio
import os
from typing import Dict, List, Any, Optional

from app.db.connection import get_db_pool
from app.db.sql import load_query
from app.services.substrate_client import substrate_client
from app.utils.logging import logger


async def fetch_storage_miners():
    """
    Fetch storage miners from the blockchain and store them in the database.
    """
    if not substrate_client.connected:
        logger.warning("Cannot fetch storage miners: Substrate client not connected")
        return
    
    logger.info("Fetching storage miners from blockchain")
    
    # Try multiple possible pallet and storage item combinations for Hippius network
    possible_storage_paths = [
        ("StorageMiners", "Miners"),
        ("Storage", "Miners"),
        ("IpfsStorage", "StorageMiners"),
        ("IpfsStorage", "Miners")
    ]
    
    miners_found = False
    miners_storage = None
    
    for module, function in possible_storage_paths:
        try:
            logger.info(f"Trying to fetch miners from {module}::{function}")
            miners_storage = await substrate_client.query_storage(
                module=module,
                function=function
            )
            
            if miners_storage and miners_storage.value and len(miners_storage.value) > 0:
                miners_found = True
                logger.info(f"Found miners in {module}::{function}")
                break
                
        except Exception as e:
            logger.warning(f"Could not fetch miners from {module}::{function}: {e}")
    
    if not miners_found:
        logger.warning("Could not find miners in any known storage path")
        
        # Attempt to dump all pallets and functions to find the right one
        try:
            logger.info("Dumping metadata to find storage items")
            loop = asyncio.get_event_loop()
            metadata = await loop.run_in_executor(
                None,
                lambda: substrate_client.substrate.get_metadata_modules()
            )
            
            logger.info("Available pallets and their storage items:")
            for module in metadata:
                if hasattr(module, 'storage'):
                    logger.info(f"Pallet: {module.name}")
                    for storage_item in module.storage:
                        logger.info(f"  - {storage_item.name}")
        except Exception as e:
            logger.error(f"Error dumping metadata: {e}")
            
        return
    
    logger.info(f"Found {len(miners_storage.value if miners_storage.value else [])} storage miners on chain")
    
    # Process miners and store in database
    db_pool = get_db_pool()
    async with db_pool.acquire() as conn:
        async with conn.transaction():
            if miners_storage.value:
                for miner_id, miner_data in miners_storage.value.items():
                    # Convert miner_id from bytes to string if needed
                    if isinstance(miner_id, bytes):
                        miner_id = miner_id.hex()
                    
                    logger.info(f"Processing miner data: {miner_data}")
                    
                    # Extract miner details from storage with flexible field names
                    # Hippius network may use different field names
                    ipfs_peer_id = None
                    for possible_field in ['peerId', 'peer_id', 'ipfs_peer_id']:
                        if possible_field in miner_data:
                            ipfs_peer_id = miner_data[possible_field]
                            break
                    
                    owner_account = None
                    for possible_field in ['owner', 'account', 'owner_account']:
                        if possible_field in miner_data:
                            owner_account = miner_data[possible_field]
                            break
                            
                    registered_at = 0
                    for possible_field in ['registeredAt', 'registered_at', 'block']:
                        if possible_field in miner_data:
                            registered_at = miner_data[possible_field]
                            break
                    
                    if not ipfs_peer_id:
                        logger.warning(f"Could not find peer ID for miner {miner_id}")
                        # Try to extract from object if it's nested differently
                        if isinstance(miner_data, dict) and len(miner_data) > 0:
                            # Just use the first property if we can't find the exact field
                            for key, val in miner_data.items():
                                if isinstance(val, str) and len(val) > 10:
                                    ipfs_peer_id = val
                                    logger.info(f"Using {key}={val} as peer ID")
                                    break
                    
                    # Fall back to empty string if still not found
                    ipfs_peer_id = ipfs_peer_id or ""
                    owner_account = owner_account or ""
                    
                    logger.info(f"Miner {miner_id}: peer_id={ipfs_peer_id}, owner={owner_account}")
                    
                    # Store in registration table
                    await conn.execute(
                        """
                        INSERT INTO registration 
                        (node_id, ipfs_peer_id, node_type, owner_account, registered_at)
                        VALUES ($1, $2, 'StorageMiner', $3, $4)
                        ON CONFLICT (node_id) 
                        DO UPDATE SET 
                            ipfs_peer_id = $2,
                            owner_account = $3,
                            updated_at = NOW()
                        """,
                        miner_id, ipfs_peer_id, owner_account, registered_at
                    )
                    
                    # Initialize miner_stats if not exists
                    await conn.execute(
                        """
                        INSERT INTO miner_stats 
                        (node_id, storage_capacity_bytes, available_space_bytes)
                        VALUES ($1, $2, $3)
                        ON CONFLICT (node_id) DO NOTHING
                        """,
                        miner_id, 
                        1000000000000,  # 1TB default capacity
                        1000000000000   # Initially all available
                    )
                    
                    logger.info(f"Registered miner {miner_id} with IPFS peer ID {ipfs_peer_id}")
    
    logger.info("Storage miners fetched and stored successfully")


async def fetch_storage_requests():
    """
    Fetch storage requests from the blockchain and store them in the database.
    """
    if not substrate_client.connected:
        logger.warning("Cannot fetch storage requests: Substrate client not connected")
        return
    
    logger.info("Fetching storage requests from blockchain")
    
    try:
        # Query the chain for storage requests
        requests_storage = await substrate_client.query_storage(
            module="StorageRequests",
            function="Requests"
        )
        
        logger.info(f"Found {len(requests_storage.value if requests_storage.value else [])} storage requests on chain")
        
        # Process requests and store in database
        db_pool = get_db_pool()
        async with db_pool.acquire() as conn:
            async with conn.transaction():
                if requests_storage.value:
                    for req_id, req_data in requests_storage.value.items():
                        # Convert req_id from bytes to string if needed
                        if isinstance(req_id, bytes):
                            req_id = req_id.hex()
                        
                        # Extract request details
                        owner = req_data.get('owner', '')
                        file_hash = req_data.get('fileHash', '')
                        file_name = req_data.get('fileName', 'unnamed')
                        file_size = req_data.get('fileSize', 0)
                        replicas = req_data.get('replicas', 1)
                        created_at = req_data.get('createdAt', 0)
                        
                        # Store in storage_requests table
                        await conn.execute(
                            """
                            INSERT INTO storage_requests 
                            (owner_account, file_hash, file_name, file_size_bytes, total_replicas, 
                             last_charged_at, created_at, selected_validator, status)
                            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
                            ON CONFLICT (owner_account, file_hash) 
                            DO UPDATE SET 
                                file_name = $3,
                                file_size_bytes = $4,
                                total_replicas = $5,
                                updated_at = NOW()
                            """,
                            owner, file_hash, file_name, file_size, replicas,
                            created_at, created_at, 
                            os.environ.get("VALIDATOR_ACCOUNT_ID", ""), "pending"
                        )
                        
                        logger.info(f"Stored storage request for file {file_hash} from {owner}")
        
        logger.info("Storage requests fetched and stored successfully")
        
    except Exception as e:
        logger.error(f"Error fetching storage requests: {e}")
        raise


async def fetch_and_store_blockchain_data():
    """
    Fetch all necessary data from the blockchain and store it in the database.
    This should be run periodically to keep the database in sync with the blockchain.
    """
    logger.info("Starting blockchain data fetch")
    
    # Ensure we're connected
    if not substrate_client.connected:
        await substrate_client.connect()
    
    # Fetch miners
    await fetch_storage_miners()
    
    # Fetch storage requests
    await fetch_storage_requests()
    
    logger.info("Blockchain data fetch completed")


async def periodic_blockchain_sync():
    """
    Periodically sync data from the blockchain to the database.
    """
    while True:
        try:
            await fetch_and_store_blockchain_data()
        except Exception as e:
            logger.error(f"Error in blockchain sync: {e}")
        
        # Wait before next sync
        await asyncio.sleep(60)  # Sync every minute