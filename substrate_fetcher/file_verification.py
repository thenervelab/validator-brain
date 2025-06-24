"""File size verification system.

Provides mechanisms to verify actual file sizes versus what users claim.
"""

from typing import Dict, List, Optional

from pydantic import BaseModel

from app.services.ipfs_api import get_file_size
from app.utils.logging import logger


class FileSizeVerification(BaseModel):
    """Result of a file size verification."""

    file_hash: str
    claimed_size: int
    actual_size: Optional[int] = None
    owner: str
    is_accurate: bool = False
    difference_bytes: int = 0
    difference_percentage: float = 0.0
    verification_success: bool = False
    error_message: Optional[str] = None


async def verify_file_sizes(storage_requests: List[Dict]) -> List[FileSizeVerification]:
    """
    Verify actual file sizes versus what users claim.

    Args:
        storage_requests: List of storage requests to verify

    Returns:
        List of FileSizeVerification results
    """
    verification_results = []

    for request in storage_requests:
        file_hash = request.file_hash
        claimed_size = getattr(request, "file_size", 0)
        owner = request.owner_account_id

        # Initialize verification result
        verification = FileSizeVerification(
            file_hash=file_hash, claimed_size=claimed_size, owner=owner,
        )

        try:
            # Get actual file size from IPFS
            actual_size = await get_file_size(file_hash)
            verification.actual_size = actual_size
            verification.verification_success = True

            # Calculate difference
            if actual_size is not None:
                difference = abs(actual_size - claimed_size)
                verification.difference_bytes = difference

                # Calculate percentage difference
                if claimed_size > 0:
                    verification.difference_percentage = (difference / claimed_size) * 100

                # Determine if size is accurate (within 5% tolerance)
                verification.is_accurate = verification.difference_percentage <= 5.0

        except Exception as e:
            verification.error_message = str(e)

        verification_results.append(verification)

    logger.info(f"Verified sizes for {len(verification_results)} files")
    return verification_results


def get_slashing_recommendations(verifications: List[FileSizeVerification]) -> List[Dict]:
    """
    Get slashing recommendations for users who lied about file sizes.

    Args:
        verifications: List of file size verification results

    Returns:
        List of slashing recommendations
    """
    slashing_recommendations = []

    # Group by owner
    owners_to_slash = {}

    for verification in verifications:
        # Skip if verification failed or size is accurate
        if not verification.verification_success or verification.is_accurate:
            continue

        owner = verification.owner

        if owner not in owners_to_slash:
            owners_to_slash[owner] = {
                "owner": owner,
                "misrepresented_files": [],
                "total_difference_bytes": 0,
            }

        # Add file to owner's misrepresented files
        owners_to_slash[owner]["misrepresented_files"].append(
            {
                "file_hash": verification.file_hash,
                "claimed_size": verification.claimed_size,
                "actual_size": verification.actual_size,
                "difference_percentage": verification.difference_percentage,
            },
        )

        # Update total difference
        owners_to_slash[owner]["total_difference_bytes"] += verification.difference_bytes

    # Convert to list and filter only serious offenders (more than 10% total difference)
    for _owner, data in owners_to_slash.items():
        if len(data["misrepresented_files"]) >= 3:
            slashing_recommendations.append(data)

    logger.info(f"Generated {len(slashing_recommendations)} slashing recommendations")
    return slashing_recommendations
