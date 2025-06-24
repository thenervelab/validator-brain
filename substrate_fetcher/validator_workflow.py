"""Validator workflow for IPFS service validation.

This module provides a structured workflow for IPFS validator operations,
organizing the validation process into distinct phases based on epoch position.
"""

import asyncio
import json
import os
from typing import Dict, List, Optional

from pydantic import BaseModel

from app.db.sql import load_query
from app.services.health_checker import check_miner_health
from app.services.profile_manager import (
    prepare_miner_profile_updates,
    upload_json_to_ipfs,
)
from app.services.storage_processor import (
    score_miners,
    select_miners_for_request,
    update_miner_scores,
)
from app.services.substrate_client import fetch_current_block
from app.services.substrate_fetcher import fetch_and_store_blockchain_data
from app.utils.config import get_epoch_block_interval
from app.utils.logging import logger
from app.utils.epoch_validator import get_epoch_block_position, calculate_epoch_from_block, get_epoch_start_block


class StorageRequest(BaseModel):
    """Standardized storage request model."""

    owner_account_id: str
    file_hash: str
    file_size: int = 1000000  # 1MB default
    created_at: str = ""

    @classmethod
    def from_substrate(cls, data):
        """Create StorageRequest from substrate data tuple format.

        Expected format: ((<owner_obj>, <file_hash_obj>), additional_data)
        Where owner_obj and file_hash_obj are objects with value attribute,
        and additional_data is either None or a dictionary with metadata.
        """
        # Handle the specific tuple format we've identified from logs
        if isinstance(data, tuple) and len(data) == 2:
            key_tuple, additional_data = data

            if isinstance(key_tuple, tuple) and len(key_tuple) == 2:
                owner_obj, file_hash_obj = key_tuple

                # Extract string values
                owner = str(owner_obj.value) if hasattr(owner_obj, "value") else str(owner_obj)
                file_hash = (
                    str(file_hash_obj.value)
                    if hasattr(file_hash_obj, "value")
                    else str(file_hash_obj)
                )

                # Base model with defaults
                model = cls(owner_account_id=owner, file_hash=file_hash)

                # Add additional data if available
                if isinstance(additional_data, dict):
                    if "file_size" in additional_data:
                        model.file_size = additional_data["file_size"]
                    elif "total_replicas" in additional_data:
                        # Estimate size based on replicas
                        model.file_size = additional_data["total_replicas"] * 1000000

                    if "created_at" in additional_data:
                        model.created_at = str(additional_data["created_at"])

                return model

        # Handle dictionary format directly
        if isinstance(data, dict):
            return cls(
                owner_account_id=data["owner_account_id"],
                file_hash=data["file_hash"],
                file_size=data.get("file_size", 1000000),
                created_at=str(data.get("created_at", "")),
            )

        # For other formats, return a simple string representation
        return cls(owner_account_id="unknown", file_hash=str(data))


class NodeRegistration(BaseModel):
    """Standardized node registration model."""

    node_id: str
    ipfs_node_id: str = ""
    owner: str = ""
    registered_at: int = 0
    node_type: str = "miner"

    @classmethod
    def from_substrate(cls, data):
        """Create NodeRegistration from substrate data (tuple or dict)."""
        if isinstance(data, tuple) and len(data) == 2:
            # Tuple: (node_id, reg_data)
            return cls(
                node_id=data[0],
                ipfs_node_id=data[0],  # Default to node_id if no specific IPFS ID
            )
        # Dictionary format
        if isinstance(data, dict):
            return cls(
                node_id=data["node_id"],
                ipfs_node_id=data.get("ipfs_node_id", data["node_id"]),
                owner=data.get("owner", ""),
                registered_at=data.get("registered_at", 0),
                node_type=data.get("node_type", "miner"),
            )
        # Fallback
        return cls(node_id=str(data))


class MinerProfile(BaseModel):
    """Standardized miner profile model."""

    node_id: str
    ipfs_peer_id: str
    profile_cid: Optional[str] = None
    storage_capacity_bytes: int = 1000000000  # 1GB default
    total_files_pinned: int = 0
    total_files_size_bytes: int = 0
    health_score: int = 100
    is_online: bool = True

    @classmethod
    def from_substrate(cls, data):
        """Create MinerProfile from substrate data (tuple or dict)."""
        if isinstance(data, tuple) and len(data) == 2:
            # Tuple: (node_id, profile_cid)
            return cls(node_id=data[0], ipfs_peer_id=data[0], profile_cid=data[1])
        # Dictionary format - must have at least node_id
        if isinstance(data, dict):
            return cls(
                node_id=data["node_id"],
                ipfs_peer_id=data.get("ipfs_peer_id", data["node_id"]),
                profile_cid=data.get("profile_cid"),
            )
        # Fallback
        return cls(node_id=str(data), ipfs_peer_id=str(data))


# For backward compatibility
class EpochPosition:
    """Epoch position constants (Rust-style intervals)."""

    EARLY = "early"  # Position 0-5: Storage processing and table refresh
    MID = "mid"  # Position 5-40: IPFS data fetching
    ASSIGNMENT = "assignment"  # Position 40-98: Assignment and submission
    CLEANUP = "cleanup"  # Position 98+: Profile cleanup

    # Legacy aliases
    LATE = "assignment"  # Map old LATE to ASSIGNMENT phase


class ValidatorWorkflow:
    """Validator workflow management."""

    def __init__(self, validator_account_id: str = None):
        """Initialize validator workflow."""
        self.validator_account_id = validator_account_id or os.environ.get("VALIDATOR_ACCOUNT_ID")
        self.shutdown_event = asyncio.Event()

    def determine_epoch_position(self, block_number: int, epoch_start_block: int = None) -> str:
        """
        Determine position within the epoch based on block position (Rust-style).

        Args:
            block_number: Current block number
            epoch_start_block: Block number when epoch started (optional)

        Returns:
            String indicating epoch position ('early', 'mid', or 'late')
        """
        epoch_length = get_epoch_block_interval()

        # Calculate position within the epoch (0 to epoch_length-1)
        if epoch_start_block is not None:
            position_in_epoch = block_number - epoch_start_block
        else:
            # Use the correct epoch calculation that handles blocks ending in 38
            position_in_epoch = get_epoch_block_position(block_number)

        # Rust-style epoch intervals:
        # Early epoch (blocks 0-5): Storage request processing and profile table refresh
        # Mid epoch (blocks 5-40): IPFS data fetching
        # Assignment phase (blocks 40-98): Reconstruction, assignment, and submission
        # Cleanup phase (block 98+): Profile table cleanup

        if position_in_epoch < 5:
            return "early"
        if position_in_epoch < 40:
            return "mid"
        if position_in_epoch < 98:
            return "assignment"
        return "cleanup"

    async def process_storage_requests(
        self,
        storage_requests: list,
        miner_profiles: list,
        node_registration: list = None,
    ) -> tuple:
        """
        Process storage requests and assign to miners.

        Args:
            storage_requests: List of storage requests from blockchain
            miner_profiles: List of miner profiles from blockchain
            node_registration: Optional list of node registration data from blockchain

        Returns:
            Tuple of (user_profiles, miner_profiles)
        """
        # ===== STORAGE REQUEST TRACING - VALIDATOR WORKFLOW PROCESSING =====
        logger.info(f"🎯 VALIDATOR_WORKFLOW: Processing {len(storage_requests)} storage requests from blockchain data")
        
        # Log sample storage requests for tracing
        for i, req in enumerate(storage_requests[:3]):
            try:
                if hasattr(req, 'owner_account_id') and hasattr(req, 'file_hash'):
                    account = req.owner_account_id[:16] + "..."
                    file_hash = req.file_hash[:20] + "..."
                    logger.info(f"🎯 VALIDATOR_WORKFLOW[{i+1}]: account={account} file_hash={file_hash}")
                else:
                    logger.info(f"🎯 VALIDATOR_WORKFLOW[{i+1}]: storage_request={str(req)[:50]}...")
            except Exception as e:
                logger.debug(f"Could not log storage request details: {e}")
        
        if len(storage_requests) > 3:
            logger.info(f"🎯 VALIDATOR_WORKFLOW: ... and {len(storage_requests) - 3} more storage requests to process")

        # Process miner profiles from blockchain
        miners = []
        if miner_profiles and len(miner_profiles) > 0:
            logger.info(f"Using {len(miner_profiles)} miners from blockchain data")

            # Keep the original node registration models in a dictionary for lookup
            registered_miners = {}
            if node_registration:
                for node in node_registration:
                    registered_miners[node.node_id] = node
                logger.info(f"Found {len(registered_miners)} registered miners with IPFS peer IDs")

            # Process each miner profile using our MinerProfile model
            for profile_data in miner_profiles:
                # Convert to MinerProfile object
                profile = MinerProfile.from_substrate(profile_data)

                # Update ipfs_peer_id from registration if available
                if registered_miners and profile.node_id in registered_miners:
                    profile.ipfs_peer_id = registered_miners[profile.node_id].ipfs_node_id

                # Add to miners list
                miners.append(profile.dict())
        else:
            # No fallback to DB - we'll only use substrate data
            logger.warning(
                "No miner profiles from blockchain, cannot proceed without substrate data",
            )
            miners = []

        # Score miners based on capacity, current load, and health
        scored_miners = await score_miners(miners)
        logger.info(f"Scored {len(scored_miners)} miners for assignment")

        # Convert storage requests to Pydantic models using our improved model
        storage_requests_models = [StorageRequest.from_substrate(req) for req in storage_requests]
        logger.info(f"🎯 VALIDATOR_WORKFLOW: Converted {len(storage_requests_models)} storage requests to models")
        
        # Log converted models for tracing
        for i, req_model in enumerate(storage_requests_models[:3]):
            account = req_model.owner_account_id[:16] + "..."
            file_hash = req_model.file_hash[:20] + "..."
            file_size = req_model.file_size
            logger.info(f"🎯 VALIDATOR_WORKFLOW_MODEL[{i+1}]: account={account} file_hash={file_hash} size={file_size:,} bytes")

        # Group storage requests by owner for easier profile handling
        user_requests = {}
        for request in storage_requests_models:
            owner = request.owner_account_id
            if owner not in user_requests:
                user_requests[owner] = []
            user_requests[owner].append(request)

        logger.info(f"🎯 VALIDATOR_WORKFLOW: Grouped storage requests for {len(user_requests)} users")
        
        # Log user groupings for tracing
        for i, (owner, requests) in enumerate(list(user_requests.items())[:3]):
            account = owner[:16] + "..."
            logger.info(f"🎯 VALIDATOR_WORKFLOW_GROUP[{i+1}]: account={account} has {len(requests)} storage requests")
        
        if len(user_requests) > 3:
            logger.info(f"🎯 VALIDATOR_WORKFLOW: ... and {len(user_requests) - 3} more users with storage requests")

        # Check if we have a large number of requests
        total_requests = sum(len(reqs) for reqs in user_requests.values())
        use_bulk_processing = total_requests > 5000

        if use_bulk_processing:
            logger.info(f"Using bulk processing for {total_requests} storage requests")
            # Import bulk processing utilities
            from substrate_fetcher.bulk_processing import process_in_chunks

            # Define chunk processing function
            async def process_request_chunk(chunk):
                chunk_user_profiles = {}
                chunk_miner_profiles = {}

                for request in chunk:
                    owner = request.owner_account_id
                    file_hash = request.file_hash
                    file_size = request.file_size

                    if owner not in chunk_user_profiles:
                        chunk_user_profiles[owner] = []

                    # Select miners for this request based on scoring
                    selected_miners = select_miners_for_request(scored_miners, file_size)

                    if not selected_miners:
                        continue

                    # Update miner scores to reflect this assignment
                    update_miner_scores(scored_miners, selected_miners, file_size)

                    # Create user profile entry
                    user_profile_entry = {
                        "file_hash": file_hash,
                        "file_size_in_bytes": file_size,
                        "assigned_miners": selected_miners,
                        "status": "assigned",
                        "main_req_hash": file_hash,
                        "created_at": request.created_at,
                    }

                    chunk_user_profiles[owner].append(user_profile_entry)

                    # Update miner profiles
                    for miner_id in selected_miners:
                        if miner_id not in chunk_miner_profiles:
                            chunk_miner_profiles[miner_id] = []

                        chunk_miner_profiles[miner_id].append(
                            {
                                "file_hash": file_hash,
                                "file_size_in_bytes": file_size,
                                "owner": owner,
                                "status": "pinned",
                            },
                        )

                # Convert to flattened list for return
                flattened_results = []
                for owner, profiles in chunk_user_profiles.items():
                    for profile in profiles:
                        flattened_results.append(
                            {"type": "user_profile", "owner": owner, "data": profile},
                        )

                for miner_id, profiles in chunk_miner_profiles.items():
                    for profile in profiles:
                        flattened_results.append(
                            {
                                "type": "miner_profile",
                                "miner_id": miner_id,
                                "data": profile,
                            },
                        )

                return flattened_results

            # Flatten requests into a single list for bulk processing
            all_requests = []
            for _owner, requests in user_requests.items():
                all_requests.extend(requests)

            # Process in chunks
            chunk_results = await process_in_chunks(
                all_requests, process_request_chunk, chunk_size=100,
            )

            # Convert chunk results back to the expected format
            processed_user_profiles = {}
            processed_miner_profiles = {}

            for result in chunk_results:
                if result["type"] == "user_profile":
                    owner = result["owner"]
                    if owner not in processed_user_profiles:
                        processed_user_profiles[owner] = []
                    processed_user_profiles[owner].append(result["data"])
                elif result["type"] == "miner_profile":
                    miner_id = result["miner_id"]
                    if miner_id not in processed_miner_profiles:
                        processed_miner_profiles[miner_id] = []
                    processed_miner_profiles[miner_id].append(result["data"])

            logger.info(
                f"Bulk processed {total_requests} requests into "
                f"{len(processed_user_profiles)} user profiles and "
                f"{len(processed_miner_profiles)} miner profiles",
            )
        else:
            # Process each user's storage requests normally
            processed_user_profiles = {}
            processed_miner_profiles = {}

            for owner, requests in user_requests.items():
                if owner not in processed_user_profiles:
                    processed_user_profiles[owner] = []

                for request in requests:
                    # Get data directly from the Pydantic model
                    file_hash = request.file_hash
                    file_size = request.file_size

                    # Select miners for this request based on scoring
                    selected_miners = select_miners_for_request(scored_miners, file_size)

                    if not selected_miners:
                        account = owner[:16] + "..."
                        file_hash_short = file_hash[:20] + "..."
                        logger.warning(f"🎯 VALIDATOR_WORKFLOW_NO_MINERS: account={account} file_hash={file_hash_short} - no suitable miners found")
                        continue

                    account = owner[:16] + "..."
                    file_hash_short = file_hash[:20] + "..."
                    logger.info(f"🎯 VALIDATOR_WORKFLOW_MINERS: account={account} file_hash={file_hash_short} - selected {len(selected_miners)} miners: {selected_miners}")

                    # Update miner scores to reflect this assignment
                    update_miner_scores(scored_miners, selected_miners, file_size)

                    # Create user profile entry
                    user_profile_entry = {
                        "file_hash": file_hash,
                        "file_size_in_bytes": file_size,
                        "assigned_miners": selected_miners,
                        "status": "assigned",
                        "main_req_hash": file_hash,
                        "created_at": request.created_at,
                    }

                    processed_user_profiles[owner].append(user_profile_entry)

                    # Update miner profiles
                    for miner_id in selected_miners:
                        if miner_id not in processed_miner_profiles:
                            processed_miner_profiles[miner_id] = []

                        processed_miner_profiles[miner_id].append(
                            {
                                "file_hash": file_hash,
                                "file_size_in_bytes": file_size,
                                "owner": owner,
                                "status": "pinned",
                            },
                        )

        # Convert user profiles to list format for return
        user_profiles = []
        for owner, entries in processed_user_profiles.items():
            for entry in entries:
                user_profile = {"user_id": owner, **entry}
                user_profiles.append(user_profile)

        # Convert miner profiles to list format for return
        miner_profiles = []
        for miner_id, entries in processed_miner_profiles.items():
            for entry in entries:
                miner_profile = {
                    "miner_id": miner_id,
                    "pinned_file": entry["file_hash"],
                    "owner": entry["owner"],
                    "file_size": entry["file_size_in_bytes"],
                }
                miner_profiles.append(miner_profile)

        # ===== STORAGE REQUEST TRACING - VALIDATOR WORKFLOW COMPLETE =====
        logger.info(
            f"🎯 VALIDATOR_WORKFLOW_COMPLETE: Processed {len(storage_requests)} storage requests into {len(user_profiles)} user profiles and {len(miner_profiles)} miner profiles",
        )
        
        # Log summary of processed profiles
        if user_profiles:
            for i, profile in enumerate(user_profiles[:3]):
                account = profile.get('user_id', 'unknown')[:16] + "..."
                file_hash = profile.get('file_hash', 'unknown')[:20] + "..."
                miners = profile.get('assigned_miners', [])
                logger.info(f"🎯 VALIDATOR_WORKFLOW_USER[{i+1}]: account={account} file_hash={file_hash} miners={len(miners)}")
        
        return user_profiles, miner_profiles

    async def prepare_pin_requests(self, user_profiles: List[Dict]) -> List[Dict]:
        """
        Prepare pin requests for blockchain submission directly from user profiles.

        Args:
            user_profiles: List of user profiles

        Returns:
            List of pin requests for blockchain submission
        """
        pin_requests = []

        # Group user profiles by user
        user_profiles_by_owner = {}
        for profile in user_profiles:
            owner = profile["user_id"]
            if owner not in user_profiles_by_owner:
                user_profiles_by_owner[owner] = []
            user_profiles_by_owner[owner].append(profile)

        # Process each user's profiles
        for owner, profiles in user_profiles_by_owner.items():
            # Encode all file data for blockchain submission
            encoded_entries = []
            total_file_size = 0
            total_files_pinned = 0

            for profile in profiles:
                # Calculate total file size and count
                file_size = profile["file_size_in_bytes"]
                total_file_size += file_size
                total_files_pinned += 1

                # Encode file_hash for blockchain submission
                file_hash = profile["file_hash"]
                file_hash_encoded = list(bytes.fromhex(file_hash.encode("utf-8").hex()))

                # Create encoded entry
                encoded_entry = {
                    "file_hash": file_hash_encoded,
                    "file_size_in_bytes": file_size,
                    "assigned_miners": profile["assigned_miners"],
                    "status": profile["status"],
                    "main_req_hash": file_hash_encoded,  # Same as file_hash for simplicity
                }

                encoded_entries.append(encoded_entry)

            # Upload to IPFS
            result = await upload_json_to_ipfs(data=encoded_entries)
            if result["success"]:
                user_profile_cid = result["cid"]
                logger.info(f"Uploaded user profile for {owner} to IPFS: {user_profile_cid}")

                # Get main request hash from first profile
                main_req_hash = profiles[0]["file_hash"]
                encoded_main_req_hash = list(bytes.fromhex(main_req_hash.encode("utf-8").hex()))

                # Add to pin_requests for blockchain submission
                pin_request = {
                    "storage_request_owner": owner,
                    "storage_request_file_hash": encoded_main_req_hash,
                    "file_size": total_file_size,
                    "user_profile_cid": user_profile_cid,
                    "total_files_pinned": total_files_pinned,
                }
                pin_requests.append(pin_request)

        logger.info(f"Prepared {len(pin_requests)} pin requests for blockchain submission")
        return pin_requests

    async def process_blockchain_data(self, substrate_data: Dict) -> Dict[str, List]:
        """
        Process and standardize blockchain data for validator operations.

        Args:
            substrate_data: Raw data fetched from blockchain

        Returns:
            Dictionary of processed data models
        """
        processed_data = {
            "node_registration": [],
            "miner_profiles": [],
            "storage_requests": [],
            "current_validator": None,
        }

        # Process node registration data - merge both hotkey and coldkey registrations
        hotkey_registration_data = substrate_data["Registration.NodeRegistration"]
        coldkey_registration_data = substrate_data["Registration.ColdkeyNodeRegistration"]

        all_registrations = []

        # Process hotkey registrations
        for reg_entry in hotkey_registration_data:
            registration = NodeRegistration.from_substrate(reg_entry)
            registration.node_type = "hotkey"  # Mark as hotkey
            all_registrations.append(registration)

        # Process coldkey registrations
        for reg_entry in coldkey_registration_data:
            registration = NodeRegistration.from_substrate(reg_entry)
            registration.node_type = "coldkey"  # Mark as coldkey
            all_registrations.append(registration)

        processed_data["node_registration"] = all_registrations
        logger.info(
            f"Processed {len(hotkey_registration_data)} hotkey and {len(coldkey_registration_data)} coldkey registrations = {len(all_registrations)} total",
        )

        # Process miner profiles
        miner_profile_data = substrate_data["IpfsPallet.MinerProfile"]
        node_metrics_raw = substrate_data["ExecutionUnit.NodeMetrics"]
        miner_files_size_raw = substrate_data["IpfsPallet.MinerTotalFilesSize"]
        miner_files_pinned_raw = substrate_data["IpfsPallet.MinerTotalFilesPinned"]

        # Convert storage map lists to dictionaries
        def convert_storage_map_to_dict(storage_list, map_name):
            """Convert storage map list of (key, value) tuples to dictionary."""
            result_dict = {}
            logger.info(f"Converting {map_name}: found {len(storage_list)} entries")

            if storage_list and len(storage_list) > 0:
                logger.info(f"Sample {map_name} entry: {storage_list[0]}")

                for entry in storage_list:
                    if isinstance(entry, (tuple, list)) and len(entry) >= 2:
                        key, value = entry[0], entry[1]
                        result_dict[str(key)] = value

                logger.info(f"Converted {map_name} to dictionary with {len(result_dict)} entries")
                if result_dict:
                    sample_key = next(iter(result_dict.keys()))
                    logger.info(
                        f"Sample {map_name} data for {sample_key}: {result_dict[sample_key]}",
                    )

            return result_dict

        # Convert all storage maps to dictionaries
        node_metrics = convert_storage_map_to_dict(node_metrics_raw, "ExecutionUnit.NodeMetrics")
        miner_files_size = convert_storage_map_to_dict(
            miner_files_size_raw, "IpfsPallet.MinerTotalFilesSize",
        )
        miner_files_pinned = convert_storage_map_to_dict(
            miner_files_pinned_raw, "IpfsPallet.MinerTotalFilesPinned",
        )

        miner_profiles = []
        for profile_entry in miner_profile_data:
            # Convert to MinerProfile object
            profile = MinerProfile.from_substrate(profile_entry)
            node_id = profile.node_id

            # Add metrics if available
            if node_id in node_metrics:
                metrics = node_metrics[node_id]
                logger.debug(f"Found metrics for node {node_id}: {metrics}")

                # Try different possible field names for storage capacity
                storage_capacity = (
                    metrics.get("ipfs_storage_max")
                    or metrics.get("storage_capacity")
                    or metrics.get("storage_max")
                    or metrics.get("ipfs_storage_capacity")
                    or metrics.get("max_storage")
                    or metrics.get("total_storage_bytes")
                )

                if storage_capacity:
                    profile.storage_capacity_bytes = storage_capacity
                    logger.debug(f"Set storage capacity for {node_id}: {storage_capacity} bytes")
                else:
                    logger.debug(
                        f"No storage capacity found in metrics for {node_id}, available keys: {list(metrics.keys()) if isinstance(metrics, dict) else 'not a dict'}",
                    )
            else:
                logger.debug(f"No metrics found for node {node_id} in ExecutionUnit.NodeMetrics")

            # Add file size metrics if available
            if node_id in miner_files_size:
                profile.total_files_size_bytes = miner_files_size[node_id]

            if node_id in miner_files_pinned:
                profile.total_files_pinned = miner_files_pinned[node_id]

            miner_profiles.append(profile)

        processed_data["miner_profiles"] = miner_profiles
        logger.info(f"Processed {len(miner_profiles)} miner profiles")

        # Process storage requests
        storage_request_data = substrate_data["IpfsPallet.UserStorageRequests"]
        processed_data["storage_requests"] = [
            StorageRequest.from_substrate(req) for req in storage_request_data
        ]
        logger.info(f"Processed {len(processed_data['storage_requests'])} storage requests")

        # Process current validator
        current_validator_data = substrate_data["IpfsPallet.CurrentEpochValidator"]
        if isinstance(current_validator_data, dict) and "account_id" in current_validator_data:
            processed_data["current_validator"] = current_validator_data["account_id"]
        elif isinstance(current_validator_data, tuple) and len(current_validator_data) == 2:
            processed_data["current_validator"] = current_validator_data[
                0
            ]  # First element is account_id

        return processed_data

    async def perform_early_epoch_actions(
        self, substrate_data: Dict, block_position: int = None,
    ) -> Dict:
        """
        Perform actions for early epoch position (blocks 0-5).
        Refresh profile tables and prepare for data fetching.

        Args:
            substrate_data: Processed blockchain data
            block_position: Position within the epoch (0-4)

        Returns:
            Dictionary with results of actions taken
        """
        logger.info(f"Performing early epoch actions (block position {block_position})")

        # Clear DB at the start of a new epoch to avoid conflicts
        from app.db.connection import get_db_pool
        from app.db.sql import load_query

        db_pool = get_db_pool()
        async with db_pool.acquire() as conn:
            async with conn.transaction():
                await conn.execute(load_query("clear_epoch_data"))
                logger.info("Cleared DB tables for new epoch")

        # For blocks 0-5, focus on refreshing profile tables
        # Storage request processing will happen in assignment phase
        return {"profile_tables_refreshed": True, "block_position": block_position}

        # Verify file sizes before processing storage requests
        from substrate_fetcher.file_verification import (
            get_slashing_recommendations,
            verify_file_sizes,
        )

        # Verify claimed file sizes
        size_verifications = await verify_file_sizes(substrate_data["storage_requests"])

        # Get slashing recommendations for users who lied about file sizes
        slashing_recommendations = get_slashing_recommendations(size_verifications)

        # Log slashing recommendations
        if slashing_recommendations:
            logger.warning(
                f"Found {len(slashing_recommendations)} users to slash for misrepresenting file sizes",
            )

            # Store slashing recommendations in DB
            async with db_pool.acquire() as conn:
                async with conn.transaction():
                    for recommendation in slashing_recommendations:
                        await conn.execute(
                            load_query("insert_slashing_recommendation"),
                            recommendation["owner"],
                            "file_size_misrepresentation",
                            json.dumps(recommendation),
                        )

        # Process storage requests and assign to miners
        user_profiles, miner_profiles = await self.process_storage_requests(
            substrate_data["storage_requests"],
            substrate_data["miner_profiles"],
            substrate_data["node_registration"],
        )

        return {
            "user_profiles": user_profiles,
            "miner_profiles": miner_profiles,
            "size_verifications": size_verifications,
            "slashing_recommendations": slashing_recommendations,
        }

    async def perform_mid_epoch_actions(
        self, substrate_data: Dict, block_position: int = None,
    ) -> Dict:
        """
        Perform actions for mid-epoch position (blocks 5-40).
        Fetch IPFS-related data from blockchain.

        Args:
            substrate_data: Processed blockchain data
            block_position: Position within the epoch (5-39)

        Returns:
            Dictionary with results of actions taken
        """
        logger.info(
            f"Performing mid-epoch actions - IPFS data fetching (block position {block_position})",
        )

        # For blocks 5-40, focus on fetching IPFS data and performing health checks
        miner_health = await check_miner_health(substrate_data["miner_profiles"])

        return {
            "miner_health": miner_health,
            "ipfs_data_fetched": True,
            "block_position": block_position,
        }

    async def perform_assignment_epoch_actions(
        self, substrate_data: Dict, results: Dict, block_position: int = None,
    ) -> Dict:
        """
        Perform actions for assignment epoch position (blocks 40-98).
        Process storage requests, assign miners, and prepare blockchain submissions.

        Args:
            substrate_data: Processed blockchain data
            results: Results from previous epoch actions
            block_position: Position within the epoch (40-97)

        Returns:
            Dictionary with results of actions taken
        """
        logger.info(f"Performing assignment epoch actions (block position {block_position})")

        # Process storage requests during assignment phase
        # Verify file sizes before processing storage requests
        from substrate_fetcher.file_verification import (
            get_slashing_recommendations,
            verify_file_sizes,
        )

        # Verify claimed file sizes
        size_verifications = await verify_file_sizes(substrate_data["storage_requests"])

        # Get slashing recommendations for users who lied about file sizes
        slashing_recommendations = get_slashing_recommendations(size_verifications)

        # Log slashing recommendations
        if slashing_recommendations:
            logger.warning(
                f"Found {len(slashing_recommendations)} users to slash for misrepresenting file sizes",
            )

            # Store slashing recommendations in DB
            from app.db.connection import get_db_pool

            db_pool = get_db_pool()
            async with db_pool.acquire() as conn:
                async with conn.transaction():
                    for recommendation in slashing_recommendations:
                        await conn.execute(
                            load_query("insert_slashing_recommendation"),
                            recommendation["owner"],
                            "file_size_misrepresentation",
                            json.dumps(recommendation),
                        )

        # Process storage requests and assign to miners
        user_profiles, miner_profiles = await self.process_storage_requests(
            substrate_data["storage_requests"],
            substrate_data["miner_profiles"],
            substrate_data["node_registration"],
        )

        # Special handling for final 10 blocks - comprehensive miner profile reconstruction
        # if block_position and block_position >= 90:
        if True:
            from substrate_fetcher.miner_profile_manager import (
                prepare_comprehensive_miner_updates,
                reconstruct_miner_profiles,
            )

            logger.info(
                f"Block position {block_position} detected: Performing comprehensive miner profile reconstruction",
            )

            # Use health data from previous checks if available
            health_data = results.get("miner_health", [])

            # Reconstruct miner profiles completely
            reconstructed_profiles = await reconstruct_miner_profiles(
                substrate_data["miner_profiles"], health_data,
            )

            # Debug: Check reconstructed_profiles structure
            logger.info(f"Reconstructed profiles count: {len(reconstructed_profiles)}")
            if reconstructed_profiles:
                sample_profile = reconstructed_profiles[0]
                logger.info(f"Sample reconstructed profile: {sample_profile}")
                logger.info(f"Sample profile type: {type(sample_profile)}")
                if isinstance(sample_profile, dict):
                    logger.info(f"Sample profile keys: {list(sample_profile.keys())}")

            # Prepare comprehensive miner updates
            miner_updates = await prepare_comprehensive_miner_updates(reconstructed_profiles)

            # Pin request processing with current user profiles
            pin_requests = await self.prepare_pin_requests(user_profiles)

            # Handle file redistribution from offline miners
            from substrate_fetcher.file_redistribution import (
                identify_files_for_redistribution,
                redistribute_files,
                update_profiles_after_redistribution,
            )

            # Identify offline miners
            offline_miners = [
                profile for profile in reconstructed_profiles if not profile["is_online"]
            ]

            # Only redistribute files if we have offline miners
            if offline_miners:
                # Identify files that need redistribution
                files_to_redistribute = await identify_files_for_redistribution(
                    offline_miners, reconstructed_profiles,
                )

                # Score miners for redistribution (use miner profiles)
                from app.services.storage_processor import score_miners

                scored_miners = await score_miners(
                    [p for p in reconstructed_profiles if p["is_online"]],
                )

                # Redistribute files
                redistribution_results = await redistribute_files(
                    files_to_redistribute, scored_miners,
                )

                # Update profiles after redistribution
                user_profiles_dict = {}
                for profile in results.get("user_profiles", []):
                    user_id = profile.get("user_id")
                    if user_id not in user_profiles_dict:
                        user_profiles_dict[user_id] = []
                    user_profiles_dict[user_id].append(profile)

                miner_profiles_dict = {}
                for profile in results.get("miner_profiles", []):
                    miner_id = profile.get("miner_id")
                    if miner_id not in miner_profiles_dict:
                        miner_profiles_dict[miner_id] = []
                    miner_profiles_dict[miner_id].append(profile)

                # Create dictionaries for redistribution
                user_profiles_dict = {}
                for profile in user_profiles:
                    user_id = profile.get("user_id")
                    if user_id not in user_profiles_dict:
                        user_profiles_dict[user_id] = []
                    user_profiles_dict[user_id].append(profile)

                miner_profiles_dict = {}
                for profile in miner_profiles:
                    miner_id = profile.get("miner_id")
                    if miner_id not in miner_profiles_dict:
                        miner_profiles_dict[miner_id] = []
                    miner_profiles_dict[miner_id].append(profile)

                # Update profiles with redistribution results
                (
                    updated_user_profiles,
                    updated_miner_profiles,
                ) = await update_profiles_after_redistribution(
                    redistribution_results, user_profiles_dict, miner_profiles_dict,
                )

                # Convert back to list format
                updated_user_profiles_list = []
                for user_id, profiles in updated_user_profiles.items():
                    for profile in profiles:
                        profile["user_id"] = user_id
                        updated_user_profiles_list.append(profile)

                updated_miner_profiles_list = []
                for miner_id, profiles in updated_miner_profiles.items():
                    for profile in profiles:
                        profile["miner_id"] = miner_id
                        updated_miner_profiles_list.append(profile)

                # Update results with redistributed profiles
                pin_requests = await self.prepare_pin_requests(updated_user_profiles_list)

                # Update miner updates - use the original reconstructed_profiles
                # since prepare_comprehensive_miner_updates expects the format from reconstruct_miner_profiles
                # not the storage processing format from updated_miner_profiles_list
                miner_updates = await prepare_comprehensive_miner_updates(reconstructed_profiles)

                # Update user_profiles and miner_profiles for return
                user_profiles = updated_user_profiles_list
                miner_profiles = updated_miner_profiles_list

            return {
                "user_profiles": user_profiles,
                "miner_profiles": miner_profiles,
                "pin_requests": pin_requests,
                "miner_updates": miner_updates,
                "redistribution_performed": bool(offline_miners),
                "offline_miners": offline_miners,
                "size_verifications": size_verifications,
                "slashing_recommendations": slashing_recommendations,
                "is_block_90": True,
                "block_position": block_position,
            }

        # Normal assignment epoch processing
        # Prepare pin requests
        pin_requests = await self.prepare_pin_requests(user_profiles)

        # Convert miner_profiles list to dictionary format
        miner_profiles_dict = {}
        for profile in miner_profiles:
            miner_id = profile["miner_id"]
            if miner_id not in miner_profiles_dict:
                miner_profiles_dict[miner_id] = []
            miner_profiles_dict[miner_id].append(profile)

        # Prepare miner profile updates
        miner_updates = await prepare_miner_profile_updates(miner_profiles_dict)

        # Check for offline miners
        offline_miners = [
            profile for profile in substrate_data["miner_profiles"] if not profile.is_online
        ]

        return {
            "user_profiles": user_profiles,
            "miner_profiles": miner_profiles,
            "pin_requests": pin_requests,
            "miner_updates": miner_updates,
            "offline_miners": offline_miners,
            "size_verifications": size_verifications,
            "slashing_recommendations": slashing_recommendations,
            "block_position": block_position,
        }

    async def perform_cleanup_epoch_actions(
        self, _substrate_data: Dict, _results: Dict, block_position: int = None,
    ) -> Dict:
        """
        Perform actions for cleanup epoch position (block 98+).
        Clear profile tables and prepare for next epoch.

        Args:
            substrate_data: Processed blockchain data
            results: Results from previous epoch actions
            block_position: Position within the epoch (98+)

        Returns:
            Dictionary with results of actions taken
        """
        logger.info(f"Performing cleanup epoch actions (block position {block_position})")

        # Clear profile tables to prepare for next epoch
        from app.db.connection import get_db_pool
        from app.db.sql import load_query

        db_pool = get_db_pool()
        async with db_pool.acquire() as conn:
            async with conn.transaction():
                await conn.execute(load_query("clear_epoch_data"))
                logger.info("Cleared profile tables for next epoch")

        return {"profile_tables_cleared": True, "block_position": block_position}

    async def perform_validator_actions(
        self, substrate_data: Dict, block_number: int, epoch_start_block: int = None,
    ) -> Dict:
        """
        Perform validator actions based on position in epoch (Rust-style).

        Args:
            substrate_data: Raw blockchain data
            block_number: Current block number
            epoch_start_block: Block number when epoch started (optional)

        Returns:
            Dictionary with results of actions taken
        """
        # Calculate epoch for metrics
        epoch_length = get_epoch_block_interval()
        current_epoch = calculate_epoch_from_block(block_number)

        # Calculate epoch start if not provided
        if epoch_start_block is None:
            epoch_start_block = get_epoch_start_block(current_epoch)

        # Calculate position within epoch (0 to 99)
        block_position = get_epoch_block_position(block_number)

        # Initialize performance tracking
        from substrate_fetcher.monitoring import (
            create_performance_tracker,
            record_validation_metrics,
        )

        performance_tracker = create_performance_tracker(
            name="validator_actions",
            validator_id=self.validator_account_id,
            epoch=current_epoch,
            block_number=block_number,
            phase="all",
        )

        # Add starting tags
        performance_tracker.add_tag("block_number", str(block_number))
        performance_tracker.add_tag("block_position", str(block_position))
        performance_tracker.add_tag("epoch_start_block", str(epoch_start_block))

        try:
            # Calculate position within epoch using new method
            epoch_position = self.determine_epoch_position(block_number, epoch_start_block)

            # Update performance tracker phase
            performance_tracker.phase = epoch_position

            # Log phase information
            logger.info(
                f"Block {block_number} (position {block_position} in epoch): {epoch_position} phase",
            )

            # Process blockchain data into standardized format
            processed_data = await self.process_blockchain_data(substrate_data)

            # Synchronize with blockchain state
            from app.db.connection import get_db_pool
            from substrate_fetcher.blockchain_sync import synchronize_with_blockchain

            # Get local data from DB
            db_pool = get_db_pool()
            local_data = await self.get_local_data_from_db(db_pool)

            # Synchronize local and blockchain state
            sync_status = await synchronize_with_blockchain(local_data, processed_data, db_pool)

            results = {"sync_status": sync_status.dict()}

            # Execute actions based on epoch position (Rust-style)
            # if epoch_position == "early":
            logger.info(f"Executing early epoch actions (blocks 0-5, position {block_position})")
            early_results = await self.perform_early_epoch_actions(processed_data, block_position)
            results.update(early_results)

            # elif epoch_position == "mid":
            logger.info(f"Executing mid-epoch actions (blocks 5-40, position {block_position})")
            mid_results = await self.perform_mid_epoch_actions(processed_data, block_position)
            results.update(mid_results)

            # elif epoch_position == "assignment":
            logger.info(
                f"Executing assignment epoch actions (blocks 40-98, position {block_position})",
            )
            assignment_results = await self.perform_assignment_epoch_actions(
                processed_data, results, block_position,
            )
            results.update(assignment_results)

            # elif epoch_position == "cleanup":
            logger.info(f"Executing cleanup epoch actions (block 98+, position {block_position})")
            cleanup_results = await self.perform_cleanup_epoch_actions(
                processed_data, results, block_position,
            )
            results.update(cleanup_results)

            # Submit to blockchain during assignment phase (once per epoch)
            # if epoch_position == "assignment" and "pin_requests" in results:
            logger.info(
                f"Submitting to blockchain during assignment phase (block position {block_position})",
            )

            # Import blockchain submission
            from substrate_fetcher.blockchain_submission import (
                submit_pending_data_to_blockchain,
            )

            # Submit pending data to blockchain
            submission_result = await submit_pending_data_to_blockchain(
                db_pool, block_number, current_epoch,
            )

            # Add submission result to results
            results["blockchain_submission"] = submission_result.dict()

            # Add phase information to results
            results["epoch_phase"] = epoch_position
            results["block_number"] = block_number
            results["block_position"] = block_position
            results["epoch_start_block"] = epoch_start_block

            # Complete performance tracking
            metrics = performance_tracker.complete(success=True)
            await record_validation_metrics(metrics)

            return results

        except Exception as e:
            # Record failure in metrics
            if "performance_tracker" in locals():
                metrics = performance_tracker.complete(success=False, error_message=str(e))
                await record_validation_metrics(metrics)

            # Re-raise exception
            raise

    async def get_local_data_from_db(self, db_pool) -> Dict:
        """
        Get local data from the database for synchronization with blockchain.

        Args:
            db_pool: Database connection pool

        Returns:
            Dictionary of local data from the database
        """
        local_data = {"miner_profiles": [], "storage_requests": []}

        async with db_pool.acquire() as conn:
            # Get miner profiles from DB (join registration, miner_stats, and miner_profile)
            miner_profiles = await conn.fetch(load_query("local_miner_profiles"))

            for profile in miner_profiles:
                # Convert database row to MinerProfile object
                miner_profile = MinerProfile(
                    node_id=profile["node_id"],
                    ipfs_peer_id=profile["ipfs_peer_id"],
                    storage_capacity_bytes=profile["storage_capacity_bytes"],
                    total_files_pinned=profile["total_files_pinned"],
                    total_files_size_bytes=profile["total_files_size_bytes"],
                    health_score=profile["health_score"],
                    is_online=profile["is_online"],
                )
                local_data["miner_profiles"].append(miner_profile)

            # Get storage requests from DB
            storage_requests = await conn.fetch(load_query("local_storage_requests"))

            for request in storage_requests:
                # Convert database row to StorageRequest object
                storage_request = StorageRequest(
                    owner_account_id=request["owner_account_id"],
                    file_hash=request["file_hash"],
                    file_size=request["file_size"],
                    created_at=str(request["created_at"]),
                )
                local_data["storage_requests"].append(storage_request)

        return local_data

    async def validator_main_loop(self):
        """Main loop for validator operations."""
        logger.info(f"Starting validator main loop with account {self.validator_account_id}")

        while not self.shutdown_event.is_set():
            logger.info("Starting blockchain monitor task")

            # Get the latest block
            current_block = await fetch_current_block()
            logger.info(f"Processing block #{current_block.number} with hash: {current_block.hash}")

            # Fetch blockchain data
            substrate_data = await fetch_and_store_blockchain_data(current_block.hash)

            # Process blockchain data into standardized format
            processed_data = await self.process_blockchain_data(substrate_data)

            # Always perform health checks on miners, regardless of validator status
            if processed_data["miner_profiles"]:
                miner_health = await check_miner_health(processed_data["miner_profiles"])

                # Calculate current epoch
                epoch_length = get_epoch_block_interval()
                current_epoch = current_block.number // epoch_length

                logger.info(f"Performed health checks for {len(miner_health)} miners")

                # Store blockchain data in DB regardless of validator status
                # IMPORTANT: Store registration data FIRST before health checks
                from app.db.connection import get_db_pool
                from app.db.sql import load_query

                db_pool = get_db_pool()
                async with db_pool.acquire() as conn:
                    async with conn.transaction():
                        # Store current block
                        await conn.execute(
                            load_query("update_latest_block"),
                            current_block.number,
                            current_block.hash,
                        )

                        # Store node registrations FIRST
                        for node in processed_data["node_registration"]:
                            await conn.execute(
                                load_query("store_miner_registration"),
                                node.node_id,
                                node.ipfs_node_id,
                                node.registered_at or 0,
                                node.node_type,
                                node.owner or "",
                            )

                        # Now store health check results (after registration is done)
                        for result in miner_health:
                            node_id = result["ipfs_peer_id"]
                            ipfs_peer_id = result["ipfs_peer_id"]
                            is_online = result["ping_success"]
                            content_verification = result.get("content_verification", {})
                            successful_cids = content_verification.get("successful_cids", 0)
                            total_cids = content_verification.get("total_cids", 0)
                            failed_cids = total_cids - successful_cids

                            await conn.execute(
                                load_query("update_miner_health"),
                                node_id,
                                ipfs_peer_id,
                                current_epoch,
                                is_online,
                                successful_cids,
                                failed_cids,
                            )

                            await conn.execute(
                                load_query("update_miner_stats"),
                                node_id,
                                is_online,
                                successful_cids,
                                failed_cids,
                            )

                        # Store miner profiles and auto-register nodes if needed
                        for miner in processed_data["miner_profiles"]:
                            if hasattr(miner, "profile_cid") and miner.profile_cid:
                                # Check if the node is registered first
                                node_exists = await conn.fetchval(
                                    load_query("check_node_exists"),
                                    miner.node_id,
                                )

                                if not node_exists:
                                    # Auto-register the miner since they have a profile
                                    logger.info(
                                        f"Auto-registering miner with profile: {miner.node_id}",
                                    )
                                    await conn.execute(
                                        load_query("store_miner_registration"),
                                        miner.node_id,
                                        miner.ipfs_peer_id,  # Use ipfs_peer_id from profile
                                        current_block.number,  # registered_at
                                        "miner",  # node_type
                                        "",  # owner - empty since we don't have this info
                                    )

                                # Now store the profile
                                await conn.execute(
                                    load_query("store_miner_profile"),
                                    miner.node_id,
                                    miner.profile_cid,
                                    0,  # file_size_bytes - using 0 for profile CID
                                    current_block.number,  # created_at
                                    self.validator_account_id,  # selected_validator
                                )

                logger.info(f"Stored blockchain data for block #{current_block.number}")

                # Calculate epoch boundaries (Rust-style)
                epoch_length = get_epoch_block_interval()
                current_epoch = calculate_epoch_from_block(current_block.number)
                epoch_start_block = get_epoch_start_block(current_epoch)

                # Check if we've already submitted in this epoch
                async with db_pool.acquire() as conn:
                    last_submission_epoch = await conn.fetchval(
                        load_query("get_last_submission_epoch"),
                    )

                    # If we haven't submitted in this epoch yet and we're in the assignment phase, submit
                    epoch_position = self.determine_epoch_position(
                        current_block.number, epoch_start_block,
                    )

                    if (
                        last_submission_epoch != current_epoch
                        and epoch_position == EpochPosition.ASSIGNMENT
                    ):
                        logger.info(f"Preparing blockchain submission for epoch {current_epoch}")

                        # Get data from DB for submission
                        # This would include miner profiles, health data, etc.
                        # For simplicity, just log that we would submit here

                        logger.info(f"Submitting to blockchain for epoch {current_epoch}")

                        # Update last submission epoch
                        await conn.execute(
                            load_query("update_last_submission_epoch"), current_epoch,
                        )

                # Check if we're the current validator (MOCKED - always true for testing)
                current_validator_data = substrate_data["IpfsPallet.CurrentEpochValidator"]
                current_validator = None

                # Handle various formats of CurrentEpochValidator
                if (
                    isinstance(current_validator_data, dict)
                    and "account_id" in current_validator_data
                ):
                    current_validator = current_validator_data["account_id"]
                elif isinstance(current_validator_data, tuple) and len(current_validator_data) == 2:
                    current_validator = current_validator_data[0]  # First element is account_id
                else:
                    logger.warning(
                        f"Unexpected CurrentEpochValidator format: {current_validator_data}",
                    )

                # MOCKED: Always act as the current validator for testing
                logger.info(
                    f"MOCK: Acting as current validator at block #{current_block.number} (actual validator: {current_validator})",
                )

                # if not current_validator or current_validator != self.validator_account_id:
                #     logger.info(
                #         f"Not the current validator at block #{current_block.number}, current validator is {current_validator}"
                #     )
                #     # Wait before next iteration
                #     await asyncio.sleep(5)
                #     continue

                logger.info(f"We are the current validator at block #{current_block.number}")

                # Perform validator actions (Rust-style)
                await self.perform_validator_actions(
                    substrate_data,
                    current_block.number,
                    epoch_start_block,
                )

            # Wait before next iteration
            await asyncio.sleep(5)

    async def start(self):
        """Start the validator workflow."""
        logger.info("Starting validator workflow")
        await self.validator_main_loop()

    async def stop(self):
        """Stop the validator workflow."""
        logger.info("Stopping validator workflow")
        self.shutdown_event.set()


# Singleton instance
validator = ValidatorWorkflow()


async def start_validator(validator_account_id: str = None):
    """Start the validator with the given account ID."""
    global validator
    validator = ValidatorWorkflow(validator_account_id)
    await validator.start()


async def stop_validator():
    """Stop the validator."""
    await validator.stop()
