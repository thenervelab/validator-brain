from typing import Optional

from pydantic import BaseModel

from app.services.storage_processor import (
    score_miners,
    select_miners_for_request,
    update_miner_scores,
)
from app.utils.logging import logger


class StorageRequest(BaseModel):
    """Standardized storage request model."""

    owner_account_id: str
    file_hash: str
    file_size: int
    created_at: str

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
                file_hash = str(file_hash_obj.value) if hasattr(file_hash_obj, "value") else str(file_hash_obj)

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


async def process_storage_requests(
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

    # Group storage requests by owner for easier profile handling
    user_requests = {}
    for request in storage_requests_models:
        owner = request.owner_account_id
        if owner not in user_requests:
            user_requests[owner] = []
        user_requests[owner].append(request)

    # Check if we have a large number of requests
    total_requests = sum(len(reqs) for reqs in user_requests.values())
    use_bulk_processing = total_requests > 5000

    if use_bulk_processing:
        logger.info(f"Processed {total_requests} storage requests")
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
            all_requests,
            process_request_chunk,
            chunk_size=100,
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

    return user_profiles, miner_profiles
