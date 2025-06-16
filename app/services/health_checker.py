"""Health checker service."""

import asyncio
import random
from typing import Dict

from app.services.ipfs_api import (
    BlockCheckResult,
    check_cid_is_provided,
    get_block_cids,
    get_child_cids,
    ping_ipfs_node,
)
from app.utils.logging import logger


async def check_miner_health(miners: list):
    """Check the health of multiple miners in parallel."""
    if not miners:
        return []
    
    semaphore = asyncio.Semaphore(20)
    
    async def _check_miner_with_semaphore(miner):
        async with semaphore:
            return await check_single_miner(miner)
    
    tasks = [_check_miner_with_semaphore(miner) for miner in miners]
    results = await asyncio.gather(*tasks, return_exceptions=True)
    
    successful_results = [r for r in results if not isinstance(r, Exception)]
    failed_count = len(results) - len(successful_results)
    
    if failed_count > 0:
        logger.warning(f"Health checks: {len(successful_results)} succeeded, {failed_count} failed")
    else:
        logger.info(f"Completed health checks for {len(successful_results)}/{len(miners)} miners")

    return successful_results


async def check_single_miner(miner) -> Dict:
    """
    Check health for a single miner and return results.

    Args:
        miner: MinerProfile Pydantic object

    Returns:
        Dictionary with health check results
    """
    # Extract required fields from miner profile
    ipfs_peer_id = miner.ipfs_peer_id
    profile_cid = getattr(miner, "profile_cid", None)

    health_results = {
        "ipfs_peer_id": ipfs_peer_id,
        "ping_success": False,
        "ping_time_ms": None,
        "content_verification": {
            "success_rate": 0,
            "total_cids": 0,
            "successful_cids": 0,
            "nested_content_checks": [],
        },
    }

    ping_task = ping_ipfs_node(ipfs_peer_id)
    profile_task = get_child_cids(profile_cid) if profile_cid else []
    
    ping_result, cids_to_check = await asyncio.gather(
        ping_task, 
        profile_task if profile_cid else [],
        return_exceptions=True
    )
    
    if isinstance(ping_result, Exception):
        logger.error(f"Ping failed for {ipfs_peer_id}: {ping_result}")
        return health_results
        
    health_results["ping_success"] = ping_result.success
    health_results["ping_time_ms"] = ping_result.time_ms or 0.0

    if not ping_result.success:
        return health_results

    if isinstance(cids_to_check, Exception):
        logger.error(f"Profile fetching failed for {profile_cid}: {cids_to_check}")
        cids_to_check = []

    # Process each CID in parallel
    verification_tasks = []
    for cid in cids_to_check:
        task = verify_content_availability(ipfs_peer_id, cid)
        verification_tasks.append(task)
    verification_results = await asyncio.gather(*verification_tasks)

    # Process verification results
    successful_cids = 0
    nested_content_checks = []

    for i, check_result in enumerate(verification_results):
        logger.info(f"{i=} {check_result=}")
        if check_result.success:
            successful_cids += 1
        nested_content_checks.append(check_result)

    # Update health results
    health_results["content_verification"] = {
        "success_rate": successful_cids / len(cids_to_check) if cids_to_check else 0,
        "total_cids": len(cids_to_check),
        "successful_cids": successful_cids,
        "nested_content_checks": nested_content_checks,
    }

    logger.info(f"Health check completed for miner {ipfs_peer_id=} {health_results=} ")

    return health_results


async def verify_content_availability(ipfs_peer_id: str, cid: str) -> BlockCheckResult:
    """
    Verify CID availability by checking if a random block from the CID
    is provided by the specified peer.

    Args:
        ipfs_peer_id: The IPFS peer ID to check
        cid: The CID to verify

    Returns: BlockCheckResult
    """
    block_cids = await get_block_cids(cid)
    random_block = random.choice(block_cids)
    return await check_cid_is_provided(random_block, ipfs_peer_id)
