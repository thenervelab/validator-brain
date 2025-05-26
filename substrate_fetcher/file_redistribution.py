"""File redistribution system for missing/offline miners.

Handles identifying files on offline miners and redistributing them to healthy miners.
"""

from typing import Dict, List

from pydantic import BaseModel

from app.utils.logging import logger


class FileToRedistribute(BaseModel):
    """Model for a file that needs to be redistributed."""

    file_hash: str
    file_size: int
    owner: str
    current_miners: List[str]
    offline_miners: List[str]


class RedistributionResult(BaseModel):
    """Result of a file redistribution operation."""

    file_hash: str
    new_miners: List[str]
    removed_miners: List[str]
    success: bool = True


async def identify_files_for_redistribution(
    offline_miners: List[Dict], miner_profiles: List[Dict],
) -> List[FileToRedistribute]:
    """
    Identify files that need to be redistributed from offline miners.

    Args:
        offline_miners: List of offline miner profiles
        miner_profiles: List of all miner profiles

    Returns:
        List of FileToRedistribute objects
    """
    # Extract offline miner IDs
    offline_miner_ids = [m["node_id"] for m in offline_miners]

    # Build dictionary of files by hash for quick lookup
    files_by_hash = {}
    for miner in miner_profiles:
        for file_data in miner.get("pinned_files", []):
            file_hash = file_data.get("file_hash")
            if not file_hash:
                continue

            if file_hash not in files_by_hash:
                files_by_hash[file_hash] = {
                    "file_hash": file_hash,
                    "file_size": file_data.get("file_size", 0),
                    "owner": file_data.get("owner", "unknown"),
                    "miners": [],
                }

            files_by_hash[file_hash]["miners"].append(miner["node_id"])

    # Identify files that need redistribution
    files_to_redistribute = []

    for file_hash, file_data in files_by_hash.items():
        # Check if any offline miners have this file
        offline_with_file = [m for m in file_data["miners"] if m in offline_miner_ids]

        # If any offline miners have this file, add to redistribution list
        if offline_with_file:
            files_to_redistribute.append(
                FileToRedistribute(
                    file_hash=file_hash,
                    file_size=file_data["file_size"],
                    owner=file_data["owner"],
                    current_miners=file_data["miners"],
                    offline_miners=offline_with_file,
                ),
            )

    logger.info(f"Identified {len(files_to_redistribute)} files needing redistribution")
    return files_to_redistribute


async def redistribute_files(
    files_to_redistribute: List[FileToRedistribute],
    scored_miners: List[Dict],
    min_replicas: int = 5,
) -> List[RedistributionResult]:
    """
    Redistribute files from offline miners to healthy miners.

    Args:
        files_to_redistribute: List of files to redistribute
        scored_miners: List of scored miners to use for redistribution
        min_replicas: Minimum number of replicas for each file

    Returns:
        List of RedistributionResult objects
    """
    from app.services.storage_processor import select_miners_for_request

    results = []

    for file in files_to_redistribute:
        # Calculate how many new miners we need
        current_healthy_miners = [m for m in file.current_miners if m not in file.offline_miners]

        needed_miners = max(0, min_replicas - len(current_healthy_miners))

        if needed_miners == 0:
            # We already have enough healthy miners for this file
            continue

        # Select new miners for this file
        new_miners = select_miners_for_request(
            scored_miners,
            file.file_size,
            count=needed_miners,
            exclude_miners=current_healthy_miners,  # Don't select miners that already have the file
        )

        # Create result
        result = RedistributionResult(
            file_hash=file.file_hash,
            new_miners=new_miners,
            removed_miners=file.offline_miners,
        )

        results.append(result)

    logger.info(f"Redistributed {len(results)} files to new miners")
    return results


async def update_profiles_after_redistribution(
    redistribution_results: List[RedistributionResult],
    user_profiles: Dict[str, List[Dict]],
    miner_profiles: Dict[str, List[Dict]],
) -> (Dict[str, List[Dict]], Dict[str, List[Dict]]):
    """
    Update user and miner profiles after file redistribution.

    Args:
        redistribution_results: Results of file redistribution
        user_profiles: Dictionary of user profiles to update
        miner_profiles: Dictionary of miner profiles to update

    Returns:
        Tuple of (updated_user_profiles, updated_miner_profiles)
    """
    # Process each redistribution result
    for result in redistribution_results:
        file_hash = result.file_hash

        # Update user profiles
        for _user_id, user_files in user_profiles.items():
            for file_entry in user_files:
                if file_entry.get("file_hash") == file_hash:
                    # Remove offline miners
                    assigned_miners = file_entry.get("assigned_miners", [])
                    for miner_id in result.removed_miners:
                        if miner_id in assigned_miners:
                            assigned_miners.remove(miner_id)

                    # Add new miners
                    assigned_miners.extend(result.new_miners)

                    # Update entry
                    file_entry["assigned_miners"] = assigned_miners

        # Update miner profiles - remove from offline miners
        for miner_id in result.removed_miners:
            if miner_id in miner_profiles:
                # Filter out the redistributed file
                miner_profiles[miner_id] = [
                    f for f in miner_profiles[miner_id] if f.get("file_hash") != file_hash
                ]

        # Update miner profiles - add to new miners
        for miner_id in result.new_miners:
            if miner_id not in miner_profiles:
                miner_profiles[miner_id] = []

            # Find file details from user profiles
            file_details = {}
            for user_files in user_profiles.values():
                for file_entry in user_files:
                    if file_entry.get("file_hash") == file_hash:
                        file_details = {
                            "file_hash": file_hash,
                            "file_size": file_entry.get("file_size_in_bytes", 0),
                            "owner": file_entry.get("user_id", "unknown"),
                            "status": "pinned",
                        }
                        break

            if file_details:
                miner_profiles[miner_id].append(file_details)

    return user_profiles, miner_profiles
