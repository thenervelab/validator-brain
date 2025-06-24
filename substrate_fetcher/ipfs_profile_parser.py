"""IPFS profile parser for miner profile files."""

import json
from typing import Dict, List, Any


def bytes_to_ipfs_cid(byte_array: List[int]) -> str:
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
    hex_string = ''.join(chr(byte) for byte in byte_array)
    
    # The hex string appears to be the CID in hex format
    # Try to decode it as hex to bytes, then back to string
    try:
        # Decode hex to bytes
        cid_bytes = bytes.fromhex(hex_string)
        # Convert to string (assuming UTF-8 encoding)
        cid_string = cid_bytes.decode('utf-8')
        return cid_string
    except (ValueError, UnicodeDecodeError):
        # If decoding fails, return the hex string as-is
        # It might already be the CID in a different format
        return hex_string


def parse_miner_profile_files(profile_files: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
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
        if 'file_hash' in parsed_file and isinstance(parsed_file['file_hash'], list):
            parsed_file['file_hash'] = bytes_to_ipfs_cid(parsed_file['file_hash'])
        
        parsed_files.append(parsed_file)
    
    return parsed_files


def parse_profile_files_from_file(file_path: str) -> List[Dict[str, Any]]:
    """
    Parse miner profile files from a JSON file.
    
    Args:
        file_path: Path to the JSON file containing the list of files in a miner's profile
        
    Returns:
        List of parsed file dictionaries
    """
    with open(file_path, 'r') as f:
        profile_files = json.load(f)
    
    return parse_miner_profile_files(profile_files)


def get_file_info(parsed_file: Dict[str, Any]) -> str:
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
    
    return '\n'.join(info)


def parse_user_profile_files(user_files: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
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
        if 'file_hash' in parsed_file and isinstance(parsed_file['file_hash'], list):
            parsed_file['file_hash'] = bytes_to_ipfs_cid(parsed_file['file_hash'])
        
        # Note: main_req_hash is already a hex string, not a byte array, so no conversion needed
        
        parsed_files.append(parsed_file)
    
    return parsed_files


def parse_user_profile_from_file(file_path: str) -> List[Dict[str, Any]]:
    """
    Parse user profile files from a JSON file.
    
    Args:
        file_path: Path to the JSON file containing the list of files in a user's profile
        
    Returns:
        List of parsed file dictionaries
    """
    with open(file_path, 'r') as f:
        user_files = json.load(f)
    
    return parse_user_profile_files(user_files)


def get_user_file_info(parsed_file: Dict[str, Any]) -> str:
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
    miner_ids = parsed_file.get('miner_ids', [])
    if miner_ids:
        info.append(f"Assigned Miners ({len(miner_ids)}):")
        for i, miner_id in enumerate(miner_ids, 1):
            info.append(f"  {i}. {miner_id}")
    else:
        info.append("Assigned Miners: None")
    
    return '\n'.join(info)


# Example usage and testing
if __name__ == "__main__":
    # Example data - a list of files from a miner's profile
    example_miner_files = [
        {
            "created_at": 698359,
            "file_hash": [
                54, 50, 54, 49, 54, 54, 55, 57, 54, 50, 54, 53, 54, 57, 54, 51,
                54, 55, 54, 99, 51, 55, 55, 49, 54, 53, 51, 51, 54, 52, 54, 55,
                51, 55, 54, 101, 54, 51, 55, 52, 55, 57, 54, 54, 55, 49, 54, 100,
                51, 51, 54, 97, 54, 97, 54, 50, 51, 54, 54, 57, 54, 51, 55, 49,
                51, 54, 54, 100, 54, 52, 55, 54, 54, 56, 55, 48, 54, 97, 54, 99,
                55, 48, 54, 53, 51, 55, 54, 98, 51, 50, 55, 54, 54, 49, 55, 48,
                55, 48, 55, 54, 51, 55, 54, 99, 54, 55, 55, 50, 51, 54, 54, 55,
                55, 53, 51, 54, 54, 53
            ],
            "file_size_in_bytes": 3495254,
            "miner_node_id": "12D3KooWKnhGPbTtCgEPWRxGJhtFFcbMTEerfSKMpVnbpLQzByPx",
            "owner": "5GeakAuWoJDYhcGCXoQaqsGGTF7BSdef1hmCraNvpHGL33zB",
            "selected_validator": "5G1Qj93Fy22grpiGKq6BEvqqmS2HVRs3jaEdMhq9absQzs6g"
        }
    ]
    
    # Example user profile data
    example_user_files = [
        {
            "created_at": 697874,
            "file_hash": [
                54, 50, 54, 49, 54, 54, 54, 98, 55, 50, 54, 53, 54, 57, 54, 53,
                51, 53, 54, 52, 54, 51, 54, 51, 54, 102, 55, 53, 55, 53, 55, 50,
                55, 54, 54, 57, 54, 100, 54, 99, 55, 51, 55, 55, 54, 52, 55, 53,
                51, 52, 54, 102, 55, 50, 55, 53, 54, 49, 55, 53, 55, 50, 54, 53,
                55, 56, 55, 49, 54, 54, 54, 98, 55, 53, 55, 55, 55, 50, 54, 52,
                54, 101, 55, 48, 55, 52, 55, 50, 54, 101, 51, 54, 55, 48, 54, 97,
                55, 48, 54, 97, 51, 55, 54, 101, 54, 101, 54, 54, 54, 56, 55, 51,
                54, 49, 54, 100, 51, 52
            ],
            "file_name": "AlphaTokenMIM.png",
            "file_size_in_bytes": 8176,
            "is_assigned": True,
            "last_charged_at": 697874,
            "main_req_hash": "6261666b72656967783533376c61796e366e7163746369766c796a6a6c7968756e67377132787a7361616f73696c743232776c347a77653337706d",
            "miner_ids": [
                "12D3KooWGdpNbq7wHeDGshSJ38NVtbZYLEH2dj6BxoBocZKas9Rj",
                "12D3KooWL7V1VF7QiUv1iRhmm573PRSrmZ9moVokoBukRD4ZNPq3",
                "12D3KooWEhtr4ME4KBXJ1wr9EREKjYvmuao3APT4BP5HxihS5MQb",
                "12D3KooWJREh7pqyaG37D3FfcyKxbTQqGPCbjNdukMbXMbvcMYAi",
                "12D3KooWPSwtt9x86ESZx1VdNpxihjHnNBNpVoyGi9jyqFzTBrLX"
            ],
            "owner": "5DCgTSdRbhoS1U3eDtYv4asT9Ljg8yBmendVKMF94tWtM7af",
            "selected_validator": "5FLcxzsKzaynqMvXcX4pwCD4GV8Cndx5WCqzTfL7LLuwoyWq",
            "total_replicas": 5
        }
    ]
    
    print("="*60)
    print("MINER PROFILE FILES")
    print("="*60)
    
    # Parse the miner's files
    parsed_miner_files = parse_miner_profile_files(example_miner_files)
    
    for i, file_entry in enumerate(parsed_miner_files, 1):
        print(f"\nMiner File #{i}:")
        print("-" * 40)
        print(get_file_info(file_entry))
    
    print("\n" + "="*60)
    print("USER PROFILE FILES")
    print("="*60)
    
    # Parse the user's files
    parsed_user_files = parse_user_profile_files(example_user_files)
    
    for i, file_entry in enumerate(parsed_user_files, 1):
        print(f"\nUser File #{i}:")
        print("-" * 40)
        print(get_user_file_info(file_entry)) 