"""Miner profile management for IPFS service validation.

Handles reconstruction and management of miner profiles, especially during
late epoch processing (around block 90).
"""

from typing import Dict, List

from pydantic import BaseModel

from app.services.ipfs_api import upload_json_to_ipfs
from app.utils.logging import logger


class MinerStorageFile(BaseModel):
    """File pinned by a miner."""

    file_hash: str
    file_size: int
    owner: str
    status: str = "pinned"


class MinerStorageProfile(BaseModel):
    """Comprehensive miner storage profile."""

    node_id: str
    ipfs_peer_id: str
    total_files_pinned: int = 0
    total_files_size_bytes: int = 0
    storage_capacity_bytes: int = 1000000000  # 1GB default
    health_score: int = 100
    is_online: bool = True
    pinned_files: List[MinerStorageFile] = []


async def reconstruct_miner_profiles(miner_data: List, health_data: List[Dict]) -> List[Dict]:
    """
    Reconstruct complete miner profiles from blockchain data and health checks.
    Specifically optimized for block 90 processing.

    Args:
        miner_data: List of miner profiles from blockchain
        health_data: List of miner health check results

    Returns:
        List of reconstructed miner profiles
    """
    # Create mapping of health data by miner ID
    health_by_miner = {}
    for health in health_data:
        peer_id = health.get("ipfs_peer_id")
        if peer_id:
            health_by_miner[peer_id] = health

    # Reconstruct profiles
    reconstructed_profiles = []

    for miner in miner_data:
        # Basic profile with defaults
        profile = MinerStorageProfile(
            node_id=miner.node_id,
            ipfs_peer_id=miner.ipfs_peer_id,
            storage_capacity_bytes=miner.storage_capacity_bytes,
            total_files_pinned=miner.total_files_pinned,
            total_files_size_bytes=miner.total_files_size_bytes,
        )

        # Add health data if available
        peer_id = miner.ipfs_peer_id
        if peer_id in health_by_miner:
            health = health_by_miner[peer_id]
            profile.is_online = health.get("ping_success", False)

            # Calculate health score from content verification
            verification = health.get("content_verification", {})
            total = verification.get("total_cids", 0)
            success = verification.get("successful_cids", 0)

            if total > 0:
                profile.health_score = int((success / total) * 100)

        # Get files from DB if needed
        # For now, we're just using the data in memory
        # In a full implementation, we'd query the DB for all files pinned by this miner

        # Convert to dict and add to results
        reconstructed_profiles.append(profile.dict())

    logger.info(f"Reconstructed {len(reconstructed_profiles)} miner profiles at block 90")
    return reconstructed_profiles


async def prepare_comprehensive_miner_updates(profiles: List[Dict]) -> List[Dict]:
    """
    Prepare comprehensive miner profile updates for blockchain submission.

    Args:
        profiles: List of reconstructed miner profiles

    Returns:
        List of miner updates for blockchain submission
    """
    miner_updates = []

    for profile in profiles:
        # Upload profile to IPFS
        result = await upload_json_to_ipfs(data=profile)

        if result["success"]:
            miner_update = {
                "miner_node_id": profile["node_id"],
                "cid": result["cid"],
                "files_count": profile["total_files_pinned"],
                "files_size": profile["total_files_size_bytes"],
                "health_score": profile["health_score"],
                "is_online": profile["is_online"],
            }
            miner_updates.append(miner_update)

    return miner_updates
