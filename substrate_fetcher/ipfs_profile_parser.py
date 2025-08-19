"""IPFS profile parser for miner profile files."""

import json
import logging
from typing import Any

import httpx

from app.utils.config import get_ipfs_node_url

logger = logging.getLogger(__name__)

IPFS_URL = get_ipfs_node_url()


async def publish_to_ipfs(
    http_client: httpx.AsyncClient,
    profile_json: list,
):
    try:
        json_data = json.dumps(
            profile_json,
            indent=2,
        )

        files = {
            "file": (
                "user_profile.json",
                json_data,
                "application/json",
            ),
        }

        response = await http_client.post(
            f"{IPFS_URL}/api/v0/add",
            files=files,
            params={"pin": "true"},
        )

        response.raise_for_status()
        cid = response.json()["Hash"]
        logger.info(f"Successfully published user profile to IPFS: {cid}")

        return cid

    except Exception:
        logger.exception(f"Error publishing to IPFS {IPFS_URL}:")
        return None


def bytes_to_ipfs_cid(byte_array: list[int]) -> str:
    """
    Convert a byte array to an IPFS CID string.

    The byte array contains ASCII values that represent a hex-encoded string.
    We need to:
    1. Convert ASCII values to characters
    2. Decode the resulting hex string to get the actual CID

    Args:
        byte_array: List of integers representing ASCII values

    Returns:
        The IPFS CID as a string
    """

    # Convert each byte (ASCII value) to its corresponding character
    hex_string = "".join(chr(byte) for byte in byte_array)

    # The hex string appears to be the CID in hex format
    # Try to decode it as hex to bytes, then back to string
    try:
        # Decode hex to bytes
        cid_bytes = bytes.fromhex(hex_string)
        # Convert to string (assuming UTF-8 encoding)
        cid_string = cid_bytes.decode("utf-8")
        return cid_string
    except (ValueError, UnicodeDecodeError):
        # If decoding fails, return the hex string as-is
        # It might already be the CID in a different format
        return hex_string


def parse_miner_profile_files(
    profile_files: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    """
    Parse a miner's profile files, converting file_hash byte arrays to IPFS CIDs.

    Args:
        profile_files: List of file dictionaries from a miner's profile

    Returns:
        List of parsed file dictionaries with file_hash converted to CID strings
    """
    parsed_files = []

    for file_entry in profile_files:
        # Create a copy of the file entry to avoid modifying the original
        parsed_file = file_entry.copy()

        # Convert file_hash byte array to IPFS CID if it exists
        if "file_hash" in parsed_file and isinstance(parsed_file["file_hash"], list):
            parsed_file["file_hash"] = bytes_to_ipfs_cid(parsed_file["file_hash"])

        parsed_files.append(parsed_file)

    return parsed_files


def parse_profile_files_from_file(file_path: str) -> list[dict[str, Any]]:
    """
    Parse miner profile files from a JSON file.

    Args:
        file_path: Path to the JSON file containing the list of files in a miner's profile

    Returns:
        List of parsed file dictionaries
    """
    with open(file_path) as f:
        profile_files = json.load(f)

    return parse_miner_profile_files(profile_files)


def get_file_info(parsed_file: dict[str, Any]) -> str:
    """
    Get a formatted string with file information.

    Args:
        parsed_file: A parsed file dictionary

    Returns:
        Formatted string with file details
    """
    info = []
    info.append(f"File Hash (CID): {parsed_file.get('file_hash', 'N/A')}")
    info.append(f"File Size: {parsed_file.get('file_size_in_bytes', 0):,} bytes")
    info.append(f"Created At: Block {parsed_file.get('created_at', 'N/A')}")
    info.append(f"Owner: {parsed_file.get('owner', 'N/A')}")
    info.append(f"Miner Node ID: {parsed_file.get('miner_node_id', 'N/A')}")
    info.append(f"Selected Validator: {parsed_file.get('selected_validator', 'N/A')}")

    return "\n".join(info)


def parse_user_profile_files(user_files: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """
    Parse a user's profile files, converting file_hash and main_req_hash byte arrays to strings.

    Args:
        user_files: List of file dictionaries from a user's profile

    Returns:
        List of parsed file dictionaries with hashes converted to strings
    """
    parsed_files = []

    for file_entry in user_files:
        # Create a copy of the file entry to avoid modifying the original
        parsed_file = file_entry.copy()

        # Convert file_hash byte array to IPFS CID if it exists
        if "file_hash" in parsed_file and isinstance(parsed_file["file_hash"], list):
            parsed_file["file_hash"] = bytes_to_ipfs_cid(parsed_file["file_hash"])

        # Note: main_req_hash is already a hex string, not a byte array, so no conversion needed

        parsed_files.append(parsed_file)

    return parsed_files


def parse_user_profile_from_file(file_path: str) -> list[dict[str, Any]]:
    """
    Parse user profile files from a JSON file.

    Args:
        file_path: Path to the JSON file containing the list of files in a user's profile

    Returns:
        List of parsed file dictionaries
    """
    with open(file_path) as f:
        user_files = json.load(f)

    return parse_user_profile_files(user_files)


def get_user_file_info(parsed_file: dict[str, Any]) -> str:
    """
    Get a formatted string with user file information.

    Args:
        parsed_file: A parsed user file dictionary

    Returns:
        Formatted string with file details
    """
    info = []
    info.append(f"File Name: {parsed_file.get('file_name', 'N/A')}")
    info.append(f"File Hash (CID): {parsed_file.get('file_hash', 'N/A')}")
    info.append(f"File Size: {parsed_file.get('file_size_in_bytes', 0):,} bytes")
    info.append(f"Created At: Block {parsed_file.get('created_at', 'N/A')}")
    info.append(f"Owner: {parsed_file.get('owner', 'N/A')}")
    info.append(f"Is Assigned: {parsed_file.get('is_assigned', False)}")
    info.append(f"Total Replicas: {parsed_file.get('total_replicas', 0)}")
    info.append(f"Selected Validator: {parsed_file.get('selected_validator', 'N/A')}")
    info.append(f"Last Charged At: Block {parsed_file.get('last_charged_at', 'N/A')}")
    info.append(f"Main Request Hash: {parsed_file.get('main_req_hash', 'N/A')[:32]}...")  # Truncate for display

    # Display miner IDs
    miner_ids = parsed_file.get("miner_ids", [])
    if miner_ids:
        info.append(f"Assigned Miners ({len(miner_ids)}):")
        for i, miner_id in enumerate(miner_ids, 1):
            info.append(f"  {i}. {miner_id}")
    else:
        info.append("Assigned Miners: None")

    return "\n".join(info)
