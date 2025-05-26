"""Synchronization mechanisms for validator decisions with blockchain state.

This module provides utilities for keeping validator decisions in sync with
the blockchain state, handling conflicts between local decisions and blockchain,
and ensuring consistency across validators.
"""

import json
from typing import Dict, List, Optional

from pydantic import BaseModel

from app.services.substrate_client import fetch_current_block
from app.utils.logging import logger


class SyncStatus(BaseModel):
    """Status of the blockchain synchronization."""

    in_sync: bool = False
    conflicts_detected: int = 0
    conflicts_resolved: int = 0
    last_block_checked: Optional[int] = None
    last_sync_time: Optional[str] = None
    actions_taken: List[str] = []


async def detect_blockchain_conflicts(local_data: Dict, blockchain_data: Dict) -> List[Dict]:
    """
    Detect conflicts between local decisions and blockchain state.

    Args:
        local_data: Dictionary of local validator decisions
        blockchain_data: Dictionary of blockchain state

    Returns:
        List of detected conflicts
    """
    conflicts = []

    # Check miner profiles for conflicts
    local_miners = {m.node_id: m for m in local_data.get("miner_profiles", [])}
    blockchain_miners = {m.node_id: m for m in blockchain_data.get("miner_profiles", [])}

    # Check for miners that exist locally but not in blockchain
    for miner_id, local_miner in local_miners.items():
        if miner_id not in blockchain_miners:
            conflicts.append(
                {
                    "type": "miner_missing_in_blockchain",
                    "miner_id": miner_id,
                    "local_data": local_miner,
                },
            )
        else:
            # Check for profile CID conflicts
            blockchain_miner = blockchain_miners[miner_id]
            local_cid = getattr(local_miner, "profile_cid", None)
            blockchain_cid = getattr(blockchain_miner, "profile_cid", None)
            if local_cid != blockchain_cid:
                conflicts.append(
                    {
                        "type": "miner_profile_conflict",
                        "miner_id": miner_id,
                        "local_cid": local_cid,
                        "blockchain_cid": blockchain_cid,
                    },
                )

    # Check storage requests for conflicts
    local_requests = {
        (r.owner_account_id, r.file_hash): r for r in local_data.get("storage_requests", [])
    }
    blockchain_requests = {
        (r.owner_account_id, r.file_hash): r for r in blockchain_data.get("storage_requests", [])
    }

    # Check for requests that exist locally but not in blockchain
    for key, local_request in local_requests.items():
        if key not in blockchain_requests:
            conflicts.append(
                {
                    "type": "request_missing_in_blockchain",
                    "owner": key[0],
                    "file_hash": key[1],
                    "local_data": local_request,
                },
            )

    logger.info(f"Detected {len(conflicts)} conflicts with blockchain state")
    return conflicts


async def resolve_conflicts(conflicts: List[Dict], db_pool) -> SyncStatus:
    """
    Resolve conflicts between local decisions and blockchain state.

    Args:
        conflicts: List of detected conflicts
        db_pool: Database connection pool

    Returns:
        SyncStatus object with resolution status
    """
    status = SyncStatus()

    if not conflicts:
        status.in_sync = True
        return status

    # Categorize conflicts
    profile_conflicts = [c for c in conflicts if c["type"] == "miner_profile_conflict"]
    missing_miners = [c for c in conflicts if c["type"] == "miner_missing_in_blockchain"]
    missing_requests = [c for c in conflicts if c["type"] == "request_missing_in_blockchain"]

    # Resolve each type of conflict
    resolved_count = 0

    # Get current block for logging
    current_block = await fetch_current_block()
    status.last_block_checked = current_block.number

    # Handle profile conflicts - trust blockchain
    if profile_conflicts:
        async with db_pool.acquire() as conn:
            async with conn.transaction():
                for conflict in profile_conflicts:
                    miner_id = conflict["miner_id"]
                    blockchain_cid = conflict["blockchain_cid"]

                    # Ensure we have valid values
                    if not miner_id:
                        logger.error(f"Invalid miner_id in conflict: {conflict}")
                        continue
                    if not blockchain_cid:
                        logger.warning(
                            f"Missing blockchain_cid for miner {miner_id}, using placeholder",
                        )
                        blockchain_cid = "missing"

                    # Update local profile to match blockchain
                    await conn.execute(
                        """
                        UPDATE miner_profile 
                        SET file_hash = $1, updated_at = NOW()
                        WHERE miner_node_id = $2
                        """,
                        blockchain_cid,
                        miner_id,
                    )

                    resolved_count += 1
                    status.actions_taken.append(
                        f"Updated miner {miner_id} profile CID to match blockchain",
                    )

    # Handle missing miners - prepare submission for next epoch
    if missing_miners:
        async with db_pool.acquire() as conn:
            async with conn.transaction():
                for conflict in missing_miners:
                    miner_id = conflict["miner_id"]
                    local_data = conflict["local_data"]

                    # Convert Pydantic object to JSON string for JSONB storage
                    if hasattr(local_data, "dict"):
                        data_to_store = json.dumps(local_data.dict())
                    elif isinstance(local_data, dict):
                        data_to_store = json.dumps(local_data)
                    else:
                        data_to_store = str(local_data)

                    # Mark for submission in next epoch
                    # First delete any existing submission for this node and type
                    await conn.execute(
                        "DELETE FROM pending_submissions WHERE node_id = $1 AND submission_type = $2",
                        miner_id,
                        "miner_profile",
                    )
                    # Then insert the new one
                    await conn.execute(
                        """
                        INSERT INTO pending_submissions
                        (submission_id, node_id, submission_type, data, created_at)
                        VALUES ($1, $2, $3, $4, NOW())
                        """,
                        f"mp_{hash(f'{miner_id}_{blockchain_cid}') % 1000000}",  # Generate shorter unique submission_id
                        miner_id,
                        "miner_profile",
                        data_to_store,
                    )

                    resolved_count += 1
                    status.actions_taken.append(
                        f"Marked miner {miner_id} for submission in next epoch",
                    )

    # Handle missing requests - prepare submission for next epoch
    if missing_requests:
        async with db_pool.acquire() as conn:
            async with conn.transaction():
                for conflict in missing_requests:
                    owner = conflict["owner"]
                    file_hash = conflict["file_hash"]
                    local_data = conflict["local_data"]

                    # Convert Pydantic object to JSON string for JSONB storage
                    if hasattr(local_data, "dict"):
                        data_to_store = json.dumps(local_data.dict())
                    elif isinstance(local_data, dict):
                        data_to_store = json.dumps(local_data)
                    else:
                        data_to_store = str(local_data)

                    # Mark for submission in next epoch
                    # First delete any existing submission for this request
                    await conn.execute(
                        "DELETE FROM pending_submissions WHERE submission_id = $1 AND submission_type = $2",
                        file_hash,
                        "storage_request",
                    )
                    # Then insert the new one
                    await conn.execute(
                        """
                        INSERT INTO pending_submissions
                        (submission_id, owner_id, submission_type, data, created_at)
                        VALUES ($1, $2, $3, $4, NOW())
                        """,
                        file_hash,
                        owner,
                        "storage_request",
                        data_to_store,
                    )

                    resolved_count += 1
                    status.actions_taken.append(
                        f"Marked storage request {file_hash} for submission in next epoch",
                    )

    # Update status
    status.conflicts_detected = len(conflicts)
    status.conflicts_resolved = resolved_count
    status.in_sync = resolved_count == len(conflicts)
    status.last_sync_time = "NOW()"

    return status


async def synchronize_with_blockchain(
    local_data: Dict, blockchain_data: Dict, db_pool,
) -> SyncStatus:
    """
    Synchronize local validator decisions with blockchain state.

    Args:
        local_data: Dictionary of local validator decisions
        blockchain_data: Dictionary of blockchain state
        db_pool: Database connection pool

    Returns:
        SyncStatus object with synchronization status
    """
    # Detect conflicts
    conflicts = await detect_blockchain_conflicts(local_data, blockchain_data)

    # Resolve conflicts
    status = await resolve_conflicts(conflicts, db_pool)

    # Log synchronization status
    if status.in_sync:
        logger.info("Local state is in sync with blockchain")
    else:
        logger.warning(
            f"Detected {status.conflicts_detected} conflicts, resolved {status.conflicts_resolved}",
        )

    return status
