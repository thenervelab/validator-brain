import logging
from substrateinterface import SubstrateInterface, Keypair
from substrateinterface.exceptions import SubstrateRequestException
from typing import List, Dict, Any
import asyncio
import os
import sys
from . import config

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Ensure parent directory is in path so imports work from anywhere
script_path = os.path.abspath(os.path.dirname(__file__))
parent_dir = os.path.dirname(script_path)
if parent_dir not in sys.path:
    sys.path.insert(0, parent_dir)



def string_to_bounded_vec(s: str) -> List[int]:
    """Converts a string to a list of byte values (BoundedVec<u8, ...> equivalent).

    Args:
        s (str): The input string (e.g., an IPFS CID or node ID).

    Returns:
        List[int]: A list of integers representing the byte values of the string.
    """
    return list(s.encode('utf-8'))

def find_hips_key(keystore_path: str) -> str:
    """Finds the HIPS key file in the keystore directory by looking for a file starting with '68697073'.

    Args:
        keystore_path (str): Path to the keystore directory.

    Returns:
        str: The filename of the HIPS key file.

    Raises:
        Exception: If the keystore directory cannot be read or no HIPS key file is found.
    """
    target_prefix = "68697073"  # "hips" in hex
    try:
        for entry in os.scandir(keystore_path):
            if entry.is_file():
                file_name = entry.name
                if file_name.startswith(target_prefix):
                    logger.info(f"Found HIPS key file: {file_name}")
                    return file_name
        raise Exception("HIPS key not found in keystore")
    except Exception as e:
        logger.error(f"Failed to find HIPS key in {keystore_path}: {e}")
        raise Exception(f"Failed to find HIPS key: {e}")

def load_hips_keypair(keystore_path: str) -> Keypair:
    """Loads the HIPS keypair from the keystore by finding the key file and reading the seed phrase.

    Args:
        keystore_path (str): Path to the keystore directory.

    Returns:
        Keypair: The keypair generated from the HIPS key seed phrase.

    Raises:
        Exception: If the key file cannot be found, read, or parsed into a keypair.
    """
    try:
        # Find the HIPS key file
        key_file = find_hips_key(keystore_path)
        key_path = os.path.join(keystore_path, key_file)

        # Read the seed phrase from the file
        with open(key_path, 'r') as f:
            raw = f.read()
        logger.debug(f"Raw content from {key_path}: {raw}")  # Debug the raw content
        # Trim surrounding quotes if present (as in Rust)
        seed_phrase = raw.strip().strip('"')
        logger.debug(f"Processed seed phrase: {seed_phrase}")  # Debug the processed phrase

        # Try to create keypair from seed (assuming hex or mnemonic)
        try:
            # If hex fails, try as a mnemonic phrase
            keypair = Keypair.create_from_mnemonic(seed_phrase)
        except ValueError as e:
            logger.warning(f"Failed to create keypair from seed: {e}")
            # First, try as a hex seed
            keypair = Keypair.create_from_seed(seed_phrase)
        
        logger.info(f"Loaded keypair for account: {keypair.ss58_address}")
        return keypair

    except Exception as e:
        logger.error(f"Failed to load HIPS keypair: {e}")
        raise Exception(f"Failed to load HIPS keypair: {e}")

async def call_update_pin_and_storage_requests(requests: List[Dict[str, Any]]) -> bool:
    """Calls the update_pin_and_storage_requests extrinsic on the Substrate node using the HIPS key for signing.

    Args:
        requests (List[Dict[str, Any]]): List of storage request updates.

    Returns:
        bool: True if the extrinsic was successfully submitted and finalized, False otherwise.
    """
    try:
        # Initialize Substrate interface
        substrate = SubstrateInterface(
            url=config.NODE_URL,
            type_registry=config.TYPE_REGISTRY if 'TYPE_REGISTRY' in dir(config) else None,
            use_remote_preset=True
        )
        logger.info(f"Connected to Substrate node at {config.NODE_URL}")

        # Check if IpfsPallet exists in metadata
        metadata = substrate.get_metadata()
        if 'IpfsPallet' not in [p.name for p in metadata.pallets]:
            logger.error("IpfsPallet not found in chain metadata!")
            return False

        # Load the HIPS keypair for signing
        keypair = load_hips_keypair(config.KEYSTORE_PATH)
        logger.info(f"Using account {keypair.ss58_address} for signing")

        # Format the requests to match the StorageRequestUpdate structure
        formatted_requests = []
        for req in requests:
            formatted_req = {
                "miner_pin_requests": [
                    {
                        "miner_node_id": string_to_bounded_vec(item["miner_node_id"]),
                        "cid": string_to_bounded_vec(item["cid"]),
                        "files_count": item["files_count"]
                    }
                    for item in req["miner_pin_requests"]
                ],
                "storage_request_owner": req["storage_request_owner"],
                "storage_request_file_hash": string_to_bounded_vec(req["storage_request_file_hash"]),
                "file_size": req["file_size"],
                "user_profile_cid": string_to_bounded_vec(req["user_profile_cid"])
            }
            formatted_requests.append(formatted_req)

        logger.debug(f"Formatted {len(formatted_requests)} request(s)")

        # Compose the call
        call = substrate.compose_call(
            call_module='IpfsPallet',
            call_function='update_pin_and_storage_requests',
            call_params={
                'requests': formatted_requests
            }
        )

        # Create and sign the extrinsic
        extrinsic = substrate.create_signed_extrinsic(call, keypair)
        logger.debug(f"Created extrinsic: {extrinsic}")

        # Submit the extrinsic and wait for finalization
        receipt = substrate.submit_extrinsic(
            extrinsic,
            wait_for_inclusion=True,
            wait_for_finalization=True
        )

        if receipt.is_success:
            logger.info(f"Extrinsic successful in block {receipt.block_hash}")
            return True
        else:
            logger.error(f"Extrinsic failed with error: {receipt.error_message}")
            return False

    except SubstrateRequestException as e:
        logger.error(f"Substrate request error: {e}")
        return False
    except Exception as e:
        logger.error(f"Unexpected error during extrinsic submission: {e}", exc_info=True)
        return False
    finally:
        if 'substrate' in locals() and substrate:
            substrate.close()

async def call_update_unpin_and_storage_requests(requests: List[Dict[str, Any]]) -> bool:
    """Calls the update_unpin_and_storage_requests extrinsic on the Substrate node using the HIPS key for signing.

    Args:
        requests (List[Dict[str, Any]]): List of storage unpin requests.

    Returns:
        bool: True if the extrinsic was successfully submitted and included in a block, False otherwise.
    """
    try:
        # Initialize Substrate interface
        substrate = SubstrateInterface(
            url=config.NODE_URL,
            type_registry=config.TYPE_REGISTRY if 'TYPE_REGISTRY' in dir(config) else None,
            use_remote_preset=True
        )
        logger.info(f"Connected to Substrate node at {config.NODE_URL}")

        # Verify the pallet exists
        metadata = substrate.get_metadata()
        if 'IpfsPallet' not in [p.name for p in metadata.pallets]:
            logger.error("IpfsPallet not found in chain metadata!")
            return False

        # Load the HIPS keypair for signing
        keypair = load_hips_keypair(config.KEYSTORE_PATH)
        logger.info(f"Using account {keypair.ss58_address} for signing")

        # Format the requests to match the StorageUnpinUpdateRequest structure
        formatted_requests = []
        for req in requests:
            formatted_req = {
                "miner_pin_requests": [
                    {
                        "miner_node_id": string_to_bounded_vec(item["miner_node_id"]),
                        "cid": string_to_bounded_vec(item["cid"]),
                        "files_count": item["files_count"]
                    }
                    for item in req["miner_pin_requests"]
                ],
                "storage_request_owner": req["storage_request_owner"],
                "storage_request_file_hash": string_to_bounded_vec(req["storage_request_file_hash"]),
                "file_size": req["file_size"],
                "user_profile_cid": string_to_bounded_vec(req["user_profile_cid"])
            }
            formatted_requests.append(formatted_req)

        logger.debug(f"Formatted {len(formatted_requests)} request(s)")

        # Prepare the call
        call = substrate.compose_call(
            call_module='IpfsPallet',
            call_function='update_unpin_and_storage_requests',
            call_params={
                'requests': formatted_requests
            }
        )

        # Create and sign the extrinsic
        extrinsic = substrate.create_signed_extrinsic(call, keypair)
        logger.debug(f"Created extrinsic: {extrinsic}")

        # Submit the extrinsic and wait for finalization
        receipt = substrate.submit_extrinsic(
            extrinsic,
            wait_for_inclusion=True,
            wait_for_finalization=True
        )

        if receipt.is_success:
            logger.info(f"Extrinsic successful in block {receipt.block_hash}")
            return True
        else:
            logger.error(f"Extrinsic failed with error: {receipt.error_message}")
            return False

    except SubstrateRequestException as e:
        logger.error(f"Substrate request error: {e}")
        return False
    except Exception as e:
        logger.error(f"Unexpected error during extrinsic submission: {e}", exc_info=True)
        return False
    finally:
        if 'substrate' in locals() and substrate:
            substrate.close()

async def call_remove_bad_storage_request(file_hash: str) -> bool:
    """Calls the remove_bad_storage_request extrinsic on the Substrate node using the HIPS key for signing.

    Args:
        file_hash (str): The file hash (e.g., an IPFS CID) to remove, as a string.

    Returns:
        bool: True if the extrinsic was successfully submitted and included in a block, False otherwise.
    """
    try:
        # Initialize Substrate interface
        substrate = SubstrateInterface(
            url=config.NODE_URL,
            type_registry=config.TYPE_REGISTRY if hasattr(config, 'TYPE_REGISTRY') else None,
            use_remote_preset=True
        )
        logger.info(f"Connected to Substrate node at {config.NODE_URL}")

        # Load the HIPS keypair for signing
        keypair = load_hips_keypair(config.KEYSTORE_PATH)
        logger.info(f"Using account {keypair.ss58_address} for signing")

        # Format the file_hash as a BoundedVec<u8, ...>
        formatted_file_hash = string_to_bounded_vec(file_hash)

        # Compose the extrinsic call
        call = substrate.compose_call(
            call_module='IpfsPallet',
            call_function='remove_bad_storage_request',
            call_params={
                'file_hash': formatted_file_hash
            }
        )

        # Create and sign the extrinsic
        extrinsic = substrate.create_signed_extrinsic(call, keypair)

        # Submit the extrinsic and wait for finalization
        receipt = substrate.submit_extrinsic(
            extrinsic,
            wait_for_inclusion=True,
            wait_for_finalization=True
        )

        # Check the result
        if receipt.is_success:
            logger.info(f"Extrinsic successful in block {receipt.block_hash}")
            return True
        else:
            logger.error(f"Extrinsic failed: {receipt.error_message}")
            return False

    except SubstrateRequestException as e:
        logger.error(f"Substrate request error: {e}")
        return False
    except Exception as e:
        logger.error(f"Unexpected error during extrinsic submission: {e}", exc_info=True)
        return False
    finally:
        if 'substrate' in locals() and substrate:
            substrate.close()

async def call_remove_bad_unpin_request(file_hash: str) -> bool:
    """Calls the remove_bad_unpin_request extrinsic on the Substrate node using the HIPS key for signing.

    Args:
        file_hash (str): The file hash (e.g., an IPFS CID) to remove from unpin requests, as a string.

    Returns:
        bool: True if the extrinsic was successfully submitted and included in a block, False otherwise.
    """
    try:
        # Initialize Substrate interface
        substrate = SubstrateInterface(
            url=config.NODE_URL,
            type_registry=config.TYPE_REGISTRY if 'TYPE_REGISTRY' in dir(config) else None,
            use_remote_preset=True
        )
        logger.info(f"Connected to Substrate node at {config.NODE_URL}")

        # Load the HIPS keypair for signing
        keypair = load_hips_keypair(config.KEYSTORE_PATH)
        logger.info(f"Using account {keypair.ss58_address} for signing")

        # Format the file_hash as a BoundedVec<u8, ...>
        formatted_file_hash = string_to_bounded_vec(file_hash)

        # Prepare the call parameters
        call = substrate.compose_call(
            call_module='IpfsPallet',
            call_function='remove_bad_unpin_request',
            call_params={
                'file_hash': formatted_file_hash
            }
        )

        # Create and sign the extrinsic
        extrinsic = substrate.create_signed_extrinsic(call, keypair)

        # Submit the extrinsic
        receipt = substrate.submit_extrinsic(
            extrinsic,
            wait_for_inclusion=True,
            wait_for_finalization=True  # Changed to True for better verification
        )

        # Check the result
        if receipt.is_success:
            logger.info(f"Extrinsic submitted successfully. Block hash: {receipt.block_hash}")
            return True
        else:
            logger.error(f"Extrinsic failed with error: {receipt.error_message}")
            return False

    except SubstrateRequestException as e:
        logger.error(f"Substrate request error: {e}")
        return False
    except Exception as e:
        logger.error(f"Unexpected error during extrinsic submission: {e}", exc_info=True)
        return False
    finally:
        if 'substrate' in locals() and substrate:
            substrate.close()

async def call_update_pin_check_metrics(miners_metrics: List[Dict[str, Any]]) -> bool:
    """Calls the update_pin_check_metrics extrinsic with enhanced debugging"""
    try:
        substrate = SubstrateInterface(
            url=config.NODE_URL,
            type_registry=config.TYPE_REGISTRY if 'TYPE_REGISTRY' in dir(config) else None,
            use_remote_preset=True
        )
        logger.info(f"Connected to Substrate node at {config.NODE_URL}")

        keypair = load_hips_keypair(config.KEYSTORE_PATH)
        logger.info(f"Using account {keypair.ss58_address} for signing")

        # Debug: Print metadata to verify pallet exists
        metadata = substrate.get_metadata()
        if 'ExecutionUnit' not in [p.name for p in metadata.pallets]:
            logger.error("ExecutionUnit pallet not found in chain metadata!")
            return False

        formatted_metrics = []
        for metric in miners_metrics:
            print(string_to_bounded_vec(metric["node_id"]), "submiting")
            formatted_metric = {
                "node_id": string_to_bounded_vec(metric["node_id"]),
                "total_pin_checks": metric["total_pin_checks"],
                "successful_pin_checks": metric["successful_pin_checks"]
            }
            formatted_metrics.append(formatted_metric)

        call = substrate.compose_call(
            call_module='ExecutionUnit',
            call_function='update_pin_check_metrics',
            call_params={'miners_metrics': formatted_metrics}
        )

        extrinsic = substrate.create_signed_extrinsic(call, keypair)
        
        logger.info("Submitting extrinsic...")
        receipt = substrate.submit_extrinsic(
            extrinsic,
            wait_for_inclusion=True,
            wait_for_finalization=True  # Wait for finalization
        )

        if receipt.is_success:
            logger.info(f"Extrinsic successful in block {receipt.block_hash}")            
            # Additional check - query some related storage
            
            return True
        else:
            logger.error(f"Extrinsic failed: {receipt.error_message}")
            return False

    except Exception as e:
        logger.error(f"Error: {e}", exc_info=True)
        return False
    finally:
        if 'substrate' in locals():
            substrate.close()

# async def main():
    # # Example usage for update_pin_and_storage_requests
    # pin_requests = [
    #     {
    #         "miner_pin_requests": [
    #             {
    #                 "miner_node_id": "0x1234567890abcdef",
    #                 "cid": "QmXoypizjW3WknFiJnKLwHCnL72vedxjQkDDP1mXWo6ucX",
    #                 "files_count": 5
    #             },
    #             {
    #                 "miner_node_id": "0xabcdef1234567890",
    #                 "cid": "QmY9jG2dWq6PfQ7dX8sYQ5b3r8sXWFqiW8w2e5j6hX2X2X",
    #                 "files_count": 3
    #             }
    #         ],
    #         "storage_request_owner": "5GrwvaEF5zXb26Fz9rcQpDWS57CtERHpNehXCPcNoHGKutQY",
    #         "storage_request_file_hash": "QmZ1a2b3c4d5e6f7g8h9i0j1k2l3m4n5o6p7q8r9s0t1u2",
    #         "file_size": 1024,
    #         "user_profile_cid": "QmA1b2c3d4e5f6g7h8i9j0k1l2m3n4o5p6q7r8s9t0u1v2"
    #     }
    # ]
    # print("Submitting update_pin_and_storage_requests...")
    # pin_success = await call_update_pin_and_storage_requests(pin_requests)
    # print(f"update_pin_and_storage_requests {'succeeded' if pin_success else 'failed'}")

    # # Example usage for update_unpin_and_storage_requests
    # unpin_requests = [
    #     {
    #         "miner_pin_requests": [
    #             {
    #                 "miner_node_id": "0x9876543210fedcba",
    #                 "cid": "QmW1x2y3z4a5b6c7d8e9f0g1h2i3j4k5l6m7n8o9p0q1r2",
    #                 "files_count": 2
    #             }
    #         ],
    #         "storage_request_owner": "5FHneW46xGXgs5mUiveU4sbTyGBzmstUspZC92UhjJM694ty",
    #         "storage_request_file_hash": "QmU1v2w3x4y5z6a7b8c9d0e1f2g3h4i5j6k7l8m9n0o1p2",
    #         "file_size": 512,
    #         "user_profile_cid": "QmT1s2r3q4p5o6n7m8l9k0j1i2h3g4f5e6d7c8b9a0z1y2"
    #     }
    # ]
    # print("Submitting update_unpin_and_storage_requests...")
    # unpin_success = await call_update_unpin_and_storage_requests(unpin_requests)
    # print(f"update_unpin_and_storage_requests {'succeeded' if unpin_success else 'failed'}")

    # # Example usage for remove_bad_storage_request
    # file_hash_storage = "QmR1s2t3u4v5w6x7y8z9a0b1c2d3e4f5g6h7i8j9k0l1m2"
    # print("Submitting remove_bad_storage_request...")
    # remove_storage_success = await call_remove_bad_storage_request(file_hash_storage)
    # print(f"remove_bad_storage_request {'succeeded' if remove_storage_success else 'failed'}")

    # # Example usage for remove_bad_unpin_request
    # file_hash_unpin = "QmS1t2u3v4w5x6y7z8a9b0c1d2e3f4g5h6i7j8k9l0m1n2"
    # print("Submitting remove_bad_unpin_request...")
    # remove_unpin_success = await call_remove_bad_unpin_request(file_hash_unpin)
    # print(f"remove_bad_unpin_request {'succeeded' if remove_unpin_success else 'failed'}")

    # Example usage for update_pin_check_metrics
    # metrics = [
    #     {
    #         "node_id": "12D3KooWLGwcL7uJSv4rdhCeADULDuehrzcdXKfeMjaJ2sCh2pBJ",
    #         "total_pin_checks": 100,
    #         "successful_pin_checks": 95
    #     },
    # ]
    # print("Submitting update_pin_check_metrics...")
    # metrics_success = await call_update_pin_check_metrics(metrics)
    # print(f"update_pin_check_metrics {'succeeded' if metrics_success else 'failed'}")

# if __name__ == "__main__":
#     asyncio.run(main())