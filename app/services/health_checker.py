"""Health checker service."""

import asyncio
import random
from typing import List, Dict

from app.db.connection import get_db_pool
from app.db.sql import load_query
from app.services.ipfs_api import (
    ping_ipfs_node,
    check_cid_is_provided,
    get_block_cids,
    get_child_cids,
    BlockCheckResult,
)
from app.utils.logging import logger


async def check_miner_health(miners: list):
    """
    Check the health of multiple miners in parallel.

    Returns:
        List of miner dictionaries with health check results added
    """
    results = await asyncio.gather(
        *[check_single_miner(miner) for miner in miners[:3]],
    )
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
    # Extract required fields from miner profile
    ipfs_peer_id = miner["ipfs_peer_id"]
    profile_cid = miner.get("profile_cid")

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

    # Step 1: Check if miner is online with ping
    ping_result = await ping_ipfs_node(ipfs_peer_id)
    health_results["ping_success"] = ping_result.success
    health_results["ping_time_ms"] = ping_result.time_ms

    # If ping fails, no need to continue with content verification
    if not ping_result.success:
        return health_results

    # Step 2: Verify content availability for CIDs assigned to this miner
    cids_to_check = await get_child_cids(profile_cid) if profile_cid else []

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


async def update_miner_health_metrics(health_results: List[Dict]):
    """
    Update miner health metrics in the database.

    Args:
        health_results: List of miner health check results
    """
    db_pool = get_db_pool()
    async with db_pool.acquire() as conn:
        async with conn.transaction():
            for result in health_results:
                node_id = result["node_id"]
                is_online = result["is_online"]

                # Extract content verification stats
                content_verification = result.get("content_verification", {})
                total_cids = content_verification.get("total_cids", 0)
                successful_cids = content_verification.get("successful_cids", 0)

                # Update miner_epoch_health table
                await conn.execute(
                    load_query("update_miner_epoch_health"),
                    node_id,
                    is_online,
                    successful_cids,
                    total_cids - successful_cids,
                )

                # Update miner_stats table
                await conn.execute(
                    load_query("update_miner_stats"),
                    node_id,
                    is_online,
                    successful_cids,
                    total_cids - successful_cids,
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
            ipfs_peer_id = row["ipfs_peer_id"]
            if ipfs_peer_id:
                ping_result = await ping_ipfs_node(ipfs_peer_id)

                if not ping_result["success"]:
                    offline_miners.append(
                        {
                            "node_id": row["node_id"],
                            "ipfs_peer_id": ipfs_peer_id,
                            "profile_cid": row["profile_cid"],
                            "last_online_block": row["last_online_block"],
                        }
                    )
                    logger.info(f"Confirmed offline miner: {row['node_id']}")

        logger.info(f"Found {len(offline_miners)} offline miners")
        return offline_miners
