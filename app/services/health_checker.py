"""Health checker service."""
import asyncio
import random
from typing import List, Dict, Tuple

from app.db.connection import get_db_pool
from app.db.sql import load_query
from app.services.ipfs_api import ping_ipfs_node, check_cid_is_provided, get_cid_blocks
from app.utils.logging import logger


async def check_miner_health(miners: List[Dict]) -> List[Dict]:
    """
    Check the health of multiple miners in parallel.
    
    Args:
        miners: List of miner dictionaries with at least node_id and ipfs_peer_id
        
    Returns:
        List of miner dictionaries with health check results added
    """
    logger.info(f"Starting health check for {len(miners)} miners")

    # Create tasks for all miners
    tasks = [check_single_miner(miner) for miner in miners]

    # Execute all tasks in parallel
    results = await asyncio.gather(*tasks)
    logger.info(f"Completed health checks for {len(results)} miners")

    return results


async def check_single_miner(miner: Dict) -> Dict:
    """
    Check health for a single miner and return results.
    
    Args:
        miner: Dictionary with miner information
        
    Returns:
        Dictionary with health check results
    """
    node_id = miner['node_id']
    ipfs_peer_id = miner['ipfs_peer_id']
    logger.info(f"Checking health for miner {node_id} with IPFS peer ID {ipfs_peer_id}")

    # Initialize health results
    health_results = {'node_id': node_id, 'ipfs_peer_id': ipfs_peer_id, 'is_online': False,
        'ping_success': False, 'ping_time_ms': None,
        'content_verification': {'success_rate': 0, 'total_cids': 0, 'successful_cids': 0,
            'nested_content_checks': []}}

    try:
        # Step 1: Check if miner is online with ping
        ping_result = await ping_ipfs_node(ipfs_peer_id)
        health_results['ping_success'] = ping_result['success']
        health_results['ping_time_ms'] = ping_result['time_ms']
        health_results['is_online'] = ping_result['success']

        # If ping fails, no need to continue with content verification
        if not ping_result['success']:
            logger.warning(f"Miner {node_id} is offline - ping failed")
            return health_results

        # Step 2: Verify content availability for CIDs assigned to this miner
        cids_to_check = await get_cids_for_miner(node_id)

        if not cids_to_check:
            logger.info(f"No CIDs to verify for miner {node_id}")
            health_results['content_verification']['total_cids'] = 0
            return health_results

        # Process each CID in parallel
        verification_tasks = []
        for cid in cids_to_check:
            task = verify_content_availability(ipfs_peer_id, cid)
            verification_tasks.append(task)

        verification_results = await asyncio.gather(*verification_tasks)

        # Process verification results
        successful_cids = 0
        nested_content_checks = []

        for i, (success, stats) in enumerate(verification_results):
            if success:
                successful_cids += 1
            nested_content_checks.extend(stats['nested_content_checks'])

        # Update health results
        health_results['content_verification'] = {
            'success_rate': successful_cids / len(cids_to_check) if cids_to_check else 0,
            'total_cids': len(cids_to_check), 'successful_cids': successful_cids,
            'nested_content_checks': nested_content_checks}

        logger.info(f"Health check completed for miner {node_id}: "
                    f"online={health_results['is_online']}, "
                    f"success_rate={health_results['content_verification']['success_rate']}")

    except Exception as e:
        logger.error(f"Error during health check for miner {node_id}: {e}")
        health_results['error'] = str(e)

    return health_results


async def get_cids_for_miner(node_id: str) -> List[str]:
    """
    Get the list of CIDs assigned to a miner from the database.
    
    Args:
        node_id: The miner's node ID
        
    Returns:
        List of CIDs assigned to the miner
    """
    cids = []

    try:
        db_pool = get_db_pool()
        async with db_pool.acquire() as conn:
            rows = await conn.fetch(
                load_query("get_cids_for_miner"), 
                node_id
            )

            cids = [row['file_hash'] for row in rows]
            logger.info(f"Retrieved {len(cids)} CIDs for miner {node_id}")

    except Exception as e:
        logger.error(f"Error retrieving CIDs for miner {node_id}: {e}")

    return cids


async def verify_content_availability(ipfs_peer_id: str, cid: str) -> Tuple[bool, Dict]:
    """
    Verify CID availability by checking if a random block from the CID
    is provided by the specified peer.
    
    Args:
        ipfs_peer_id: The IPFS peer ID to check
        cid: The CID to verify
        
    Returns:
        Tuple of (success, stats):
        - success: True if the peer is providing the random block
        - stats: Dictionary with check results for health_results
    """
    # First get the blocks that make up the CID
    block_cids = await get_cid_blocks(cid)

    # Select a single random block to check
    random_block = random.choice(block_cids)
    logger.info(f"Selected random block {random_block} from {len(block_cids)} blocks to verify")

    # Check if the peer is providing this random block
    is_provided = await check_cid_is_provided(random_block, ipfs_peer_id)

    # Prepare results
    content_checks = [{'cid': random_block, 'available': is_provided}]

    stats = {'nested_content_checks': content_checks, 'total_nested_cids': 1,
        'successful_nested_cids': 1 if is_provided else 0}

    logger.info(
        f"Block verification: Peer {ipfs_peer_id} {'IS' if is_provided else 'IS NOT'} providing random block {random_block}")

    return is_provided, stats


async def update_miner_health_metrics(health_results: List[Dict]):
    """
    Update miner health metrics in the database.
    
    Args:
        health_results: List of miner health check results
    """
    db_pool = get_db_pool()
    async with db_pool.acquire() as conn:
        # Start a transaction
        async with conn.transaction():
            for result in health_results:
                node_id = result['node_id']
                is_online = result['is_online']

                # Extract content verification stats
                content_verification = result.get('content_verification', {})
                total_cids = content_verification.get('total_cids', 0)
                successful_cids = content_verification.get('successful_cids', 0)

                # Update miner_epoch_health table
                await conn.execute(
                    load_query("update_miner_epoch_health"),
                    node_id, is_online, successful_cids,
                    total_cids - successful_cids
                )

                # Update miner_stats table
                await conn.execute(
                    load_query("update_miner_stats"),
                    node_id, is_online, successful_cids,
                    total_cids - successful_cids
                )

                logger.info(f"Updated health metrics for miner {node_id}")

    logger.info(f"Successfully updated health metrics for {len(health_results)} miners")


async def get_offline_miners():
    """
    Get a list of miners that are offline.
    
    Returns:
        List of dictionaries with offline miner information
    """
    db_pool = get_db_pool()
    async with db_pool.acquire() as conn:
        rows = await conn.fetch(load_query("get_offline_miners"))
        
        offline_miners = []
        for row in rows:
            # Verify the miner is actually offline with a ping
            ipfs_peer_id = row['ipfs_peer_id']
            if ipfs_peer_id:
                ping_result = await ping_ipfs_node(ipfs_peer_id)

                if not ping_result['success']:
                    offline_miners.append({
                        'node_id': row['node_id'], 
                        'ipfs_peer_id': ipfs_peer_id,
                        'profile_cid': row['profile_cid'],
                        'last_online_block': row['last_online_block']
                    })
                    logger.info(f"Confirmed offline miner: {row['node_id']}")

        logger.info(f"Found {len(offline_miners)} offline miners")
        return offline_miners