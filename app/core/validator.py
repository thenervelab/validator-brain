"""Core validator functionality."""

import asyncio
import os
from typing import List, Optional, Dict, Any

from pydantic import BaseModel

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
                owner = str(owner_obj.value) if hasattr(owner_obj, 'value') else str(owner_obj)
                file_hash = str(file_hash_obj.value) if hasattr(file_hash_obj, 'value') else str(file_hash_obj)
                
                # Base model with defaults
                model = cls(
                    owner_account_id=owner,
                    file_hash=file_hash
                )
                
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
                created_at=str(data.get("created_at", ""))
            )
            
        # For other formats, return a simple string representation
        return cls(
            owner_account_id="unknown",
            file_hash=str(data)
        )


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
                ipfs_node_id=data[0]  # Default to node_id if no specific IPFS ID
            )
        # Dictionary format
        if isinstance(data, dict):
            return cls(
                node_id=data["node_id"],
                ipfs_node_id=data.get("ipfs_node_id", data["node_id"]),
                owner=data.get("owner", ""),
                registered_at=data.get("registered_at", 0),
                node_type=data.get("node_type", "miner")
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
            return cls(
                node_id=data[0],
                ipfs_peer_id=data[0],
                profile_cid=data[1]
            )
        # Dictionary format - must have at least node_id
        if isinstance(data, dict):
            return cls(
                node_id=data["node_id"],
                ipfs_peer_id=data.get("ipfs_peer_id", data["node_id"]),
                profile_cid=data.get("profile_cid")
            )
        # Fallback
        return cls(node_id=str(data), ipfs_peer_id=str(data))

# Use an event for controlled shutdown
shutdown_event = asyncio.Event()


async def process_storage_requests(
    storage_requests: list, miner_profiles: list, node_registration: list = None
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
    logger.info(f"Processing {len(storage_requests)} storage requests")

    # Process miner profiles from blockchain
    miners = []
    if miner_profiles and len(miner_profiles) > 0:
        logger.info(f"Using {len(miner_profiles)} miners from blockchain data")

        # Keep the original node registration models in a dictionary for lookup
        registered_miners = {}
        if node_registration:
            for node in node_registration:
                registered_miners[node.node_id] = node
            logger.info(
                f"Found {len(registered_miners)} registered miners with IPFS peer IDs"
            )

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
            "No miner profiles from blockchain, cannot proceed without substrate data"
        )
        miners = []

    # Score miners based on capacity, current load, and health
    scored_miners = await score_miners(miners)
    logger.info(f"Scored {len(scored_miners)} miners for assignment")

    # Convert storage requests to Pydantic models using our improved model
    storage_requests_models = [
        StorageRequest.from_substrate(req) 
        for req in storage_requests
    ]
    logger.info(f"Converted {len(storage_requests_models)} storage requests to models")
    
    # Group storage requests by owner for easier profile handling
    user_requests = {}
    for request in storage_requests_models:
        owner = request.owner_account_id
        if owner not in user_requests:
            user_requests[owner] = []
        user_requests[owner].append(request)

    logger.info(f"Grouped storage requests for {len(user_requests)} users")

    # Process each user's storage requests
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
                logger.warning(f"No suitable miners found for request: {file_hash}")
                continue

            logger.info(f"Selected {len(selected_miners)} miners for file {file_hash}")

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
                    }
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

    logger.info(
        f"Processed {len(storage_requests)} storage requests into {len(user_profiles)} user profile entries and {len(miner_profiles)} miner profile entries"
    )
    return user_profiles, miner_profiles


async def perform_validator_actions(
    substrate_data: dict,
    block_number: int,
    epoch_end_block: int,
):
    """
    Perform validator actions based on position in epoch.

    Args:
        substrate_data: Collected substrate data.
        block_number: Current block number
        epoch_end_block: Block number when epoch ends
    """
    # Calculate position within epoch (0-99 for 100 block epochs)
    epoch_length = get_epoch_block_interval()
    position_in_epoch = (epoch_end_block - block_number) % epoch_length

    logger.info(
        f"Position in epoch: {position_in_epoch}/{epoch_length} (block {block_number}, epoch ends at {epoch_end_block})"
    )

    # Log raw data formats for debugging
    logger.info("Raw data formats:")
    for key, value in substrate_data.items():
        if isinstance(value, list) and value:
            logger.info(f"{key}: type={type(value)}, count={len(value)}")
            logger.info(f"{key} first item type: {type(value[0])}")
            logger.info(f"{key} first item: {value[0]}")
            if hasattr(value[0], 'value'):
                logger.info(f"{key} first item value type: {type(value[0].value)}")
                logger.info(f"{key} first item value: {value[0].value}")
                if hasattr(value[0].value, '__dict__'):
                    logger.info(f"{key} first item value dict: {value[0].value.__dict__}")

    # Process all blockchain data using direct access
    # Process node registration data using Pydantic model
    node_registration_data = substrate_data["Registration.NodeRegistration"]
    logger.info(f"Processing {len(node_registration_data)} node registration entries")
    
    # Convert registration data to standardized format using Pydantic model
    node_registration = [
        NodeRegistration.from_substrate(reg_entry) 
        for reg_entry in node_registration_data
    ]
    
    logger.info(f"Processed {len(node_registration)} node registration entries")

    # Process node metrics data - direct access
    node_metrics = substrate_data["ExecutionUnit.NodeMetrics"]
    miner_files_size = substrate_data["IpfsPallet.MinerTotalFilesSize"]
    miner_files_pinned = substrate_data["IpfsPallet.MinerTotalFilesPinned"]

    # Convert substrate profiles to standardized MinerProfile objects
    logger.info("Processing miner profiles from substrate data")
    miner_profiles_list = []
    
    for profile_entry in substrate_data["IpfsPallet.MinerProfile"]:
        logger.info(f"{profile_entry=}")
        
        # Convert to MinerProfile object
        profile = MinerProfile.from_substrate(profile_entry)
        node_id = profile.node_id
        
        # Add metrics if available
        if node_id in node_metrics:
            metrics = node_metrics[node_id]
            profile.storage_capacity_bytes = metrics.get("ipfs_storage_max", profile.storage_capacity_bytes)
            
        # Add file size metrics if available
        if node_id in miner_files_size:
            profile.total_files_size_bytes = miner_files_size[node_id]
            
        if node_id in miner_files_pinned:
            profile.total_files_pinned = miner_files_pinned[node_id]
            
        miner_profiles_list.append(profile.dict())
    
    logger.info(f"Processed {len(miner_profiles_list)} miner profiles")
    substrate_data["IpfsPallet.MinerProfile"] = miner_profiles_list

    # Check miner health
    logger.info(
        f"Checking health of {len(substrate_data['IpfsPallet.MinerProfile'])} miners"
    )
    miner_health = await check_miner_health(substrate_data["IpfsPallet.MinerProfile"])

    # Process storage requests using our Pydantic models
    user_profiles, miner_profiles = await process_storage_requests(
        substrate_data["IpfsPallet.UserStorageRequests"],
        substrate_data["IpfsPallet.MinerProfile"],
        node_registration,  # Already converted to NodeRegistration models
    )

    # Don't use database, directly process profiles from substrate data

    # Prepare blockchain submissions without database
    pin_requests = await prepare_pin_requests(user_profiles)
    
    # Convert miner_profiles list to dictionary format expected by prepare_miner_profile_updates
    miner_profiles_dict = {}
    for profile in miner_profiles:
        miner_id = profile["miner_id"]
        if miner_id not in miner_profiles_dict:
            miner_profiles_dict[miner_id] = []
        miner_profiles_dict[miner_id].append(profile)
    
    miner_updates = await prepare_miner_profile_updates(miner_profiles_dict)

    # Submit to blockchain
    logger.info(
        f"Submitting {len(pin_requests)} pin requests and {len(miner_updates)} miner updates to blockchain"
    )

    # Check for offline miners directly from miner profiles
    offline_miners = [
        profile
        for profile in substrate_data["IpfsPallet.MinerProfile"]
        if not profile["is_online"]
    ]
    if offline_miners:
        logger.info(f"Detected {len(offline_miners)} offline miners")


async def prepare_pin_requests(user_profiles):
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
            logger.info(
                f"Uploaded user profile for {owner} to IPFS: {user_profile_cid}"
            )

            # Get main request hash from first profile
            main_req_hash = profiles[0]["file_hash"]
            encoded_main_req_hash = list(
                bytes.fromhex(main_req_hash.encode("utf-8").hex())
            )

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


async def validator_main_loop():
    """Main loop for validator operations."""
    validator_account_id = os.environ.get("VALIDATOR_ACCOUNT_ID")
    logger.info(f"Starting main loop with validator account {validator_account_id}")

    while not shutdown_event.is_set():
        logger.info("Starting blockchain monitor task")

        # Get the latest block
        current_block = await fetch_current_block()
        logger.info(
            f"Processing block #{current_block.number} with hash: {current_block.hash}"
        )

        substrate_data = await fetch_and_store_blockchain_data(current_block.hash)

        # Check if we're the current validator directly from substrate data
        # CurrentEpochValidator is a struct with account_id and block_number fields
        current_validator_data = substrate_data["IpfsPallet.CurrentEpochValidator"]
        current_validator = None

        # Handle various possible formats of CurrentEpochValidator
        if (
            isinstance(current_validator_data, dict)
            and "account_id" in current_validator_data
        ):
            current_validator = current_validator_data["account_id"]
        elif (
            isinstance(current_validator_data, tuple)
            and len(current_validator_data) == 2
        ):
            current_validator = current_validator_data[0]  # First element is account_id
        else:
            logger.warning(
                f"Unexpected CurrentEpochValidator format: {current_validator_data}"
            )

        if not current_validator or current_validator != validator_account_id:
            logger.info(
                f"Not the current validator at block #{current_block.number}, current validator is {current_validator}"
            )
            # Wait before next iteration
            await asyncio.sleep(5)
            # continue
            # LEAVE THIS COMMENTED, WE WANNA PRETEND TO BE THE CURRENT VALI

        logger.info(f"We are the current validator at block #{current_block.number}")

        epoch_length = get_epoch_block_interval()
        current_epoch = current_block.number // epoch_length
        epoch_end_block = (current_epoch + 1) * epoch_length

        await perform_validator_actions(
            substrate_data,
            current_block.number,
            epoch_end_block,
        )

        # Wait before next iteration
        await asyncio.sleep(5)
