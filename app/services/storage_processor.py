"""Storage request processor service."""

import random
from typing import Dict, List

from app.utils.logging import logger


async def score_miners(miners: List[Dict]) -> List[Dict]:
    """
    Score miners based on capacity, current load, and reliability.

    Args:
        miners: List of miner dictionaries

    Returns:
        List of scored miners sorted by score (highest first)
    """
    scored_miners = []

    for miner in miners:
        node_id = miner["node_id"]

        # Use health score from database instead of real-time ping
        health_score = miner.get("health_score", 0)
        if health_score is None or health_score < 20.0:  # Skip miners with very low health
            logger.warning(f"Miner {node_id} has low health score {health_score} (available keys: {list(miner.keys())}), skipping from assignment")
            continue

        # Calculate miner's available storage
        max_storage = miner["storage_capacity_bytes"] or 1000000000  # 1GB default
        used_storage = miner["total_files_size_bytes"] or 0
        available_storage = max(0, max_storage - used_storage)

        # Calculate score - higher is better
        # Score = Available storage percentage * 0.6 + (1 - normalized file count) * 0.3 + success rate * 0.1
        storage_score = available_storage / max_storage if max_storage > 0 else 0
        file_count = miner["total_files_pinned"] or 0
        file_count_normalized = min(
            1.0, file_count / 1000,
        )  # Normalize to 0-1 range, assuming 1000 files as max
        file_score = 1.0 - file_count_normalized  # Fewer files is better
        success_rate = miner.get("health_score", 100) / 100  # Convert health_score to 0-1 range

        total_score = (storage_score * 0.6) + (file_score * 0.3) + (success_rate * 0.1)

        scored_miners.append(
            {
                "node_id": node_id,
                "ipfs_peer_id": miner["ipfs_peer_id"],
                "available_storage": available_storage,
                "max_storage": max_storage,
                "file_count": file_count,
                "success_rate": success_rate,
                "score": total_score,
            },
        )

    # Sort miners by score (highest first)
    scored_miners.sort(key=lambda m: m["score"], reverse=True)
    logger.info(f"scored_miners=={scored_miners}")

    return scored_miners


def select_miners_for_request(scored_miners: List[Dict], _file_size: int) -> List[str]:
    """
    Select miners for a storage request based on capacity and scoring.

    Args:
        scored_miners: List of scored miners
        file_size: Size of the file to be stored

    Returns:
        List of selected miner IDs
    """
    # Take top 15 miners by score
    top_miners = scored_miners

    # Pick 5 randomly from top 15 to distribute load
    selected_miners = random.sample(top_miners, min(5, len(top_miners)))

    # Extract just the node IDs
    return [m["node_id"] for m in selected_miners]


def update_miner_scores(scored_miners: List[Dict], selected_miner_ids: List[str], file_size: int):
    """
    Update miner scores after a storage assignment.

    Args:
        scored_miners: List of scored miners
        selected_miner_ids: List of selected miner IDs
        file_size: Size of the file assigned
    """
    for miner in scored_miners:
        if miner["node_id"] in selected_miner_ids:
            # Update miner's available storage
            miner["available_storage"] = max(0, miner["available_storage"] - file_size)
            miner["file_count"] += 1

            # Recalculate score
            storage_score = (
                miner["available_storage"] / miner["max_storage"] if miner["max_storage"] > 0 else 0
            )
            file_count_normalized = min(1.0, miner["file_count"] / 1000)
            file_score = 1.0 - file_count_normalized
            miner["score"] = (
                (storage_score * 0.6) + (file_score * 0.3) + (miner["success_rate"] * 0.1)
            )

    # Re-sort miners by updated scores
    scored_miners.sort(key=lambda m: m["score"], reverse=True)
