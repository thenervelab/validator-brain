
from db_manager import delete_completed_storage_request
from ipfs_api import upload_json_to_ipfs
from utils import get_ipfs_node_url, logger


async def upload_profiles_to_ipfs(user_profiles, miner_profiles):
    """
    Upload user and miner profiles to IPFS and return CIDs.
    
    Args:
        user_profiles: Dictionary of user profiles (owner -> list of entries)
        miner_profiles: Dictionary of miner profiles (miner_id -> list of entries)
        
    Returns:
        Dictionary mapping owner/miner IDs to their profile CIDs
    """
    ipfs_node_url = get_ipfs_node_url()
    profile_cids = {'users': {}, 'miners': {}}

    try:
        # Upload user profiles
        for owner, entries in user_profiles.items():
            if not entries:
                continue

            # We need to encode the file_hash bytes for blockchain submission
            encoded_entries = []
            for entry in entries:
                file_hash = entry.get('file_hash', '')
                file_hash_encoded = None

                if file_hash:
                    file_hash_encoded = list(bytes.fromhex(file_hash.encode('utf-8').hex()))

                encoded_entry = entry.copy()
                encoded_entry['file_hash'] = file_hash_encoded

                # Similarly encode main_req_hash if present
                main_req_hash = entry.get('main_req_hash', '')
                if main_req_hash:
                    main_req_hash_encoded = list(bytes.fromhex(main_req_hash.encode('utf-8').hex()))
                    encoded_entry['main_req_hash'] = main_req_hash_encoded

                encoded_entries.append(encoded_entry)

            # Upload to IPFS
            result = await upload_json_to_ipfs(data=encoded_entries, api_url=ipfs_node_url)
            if result['success']:
                profile_cids['users'][owner] = result['cid']
                logger.info(f"Uploaded user profile for {owner} to IPFS: {result['cid']}")
            else:
                logger.error(f"Failed to upload user profile for {owner}: {result['error']}")

        # Upload miner profiles
        for miner_id, entries in miner_profiles.items():
            if not entries:
                continue

            # We need to encode the file_hash bytes for blockchain submission
            encoded_entries = []
            for entry in entries:
                file_hash = entry.get('file_hash', '')
                file_hash_encoded = None

                if file_hash:
                    file_hash_encoded = list(bytes.fromhex(file_hash.encode('utf-8').hex()))

                encoded_entry = entry.copy()
                encoded_entry['file_hash'] = file_hash_encoded
                encoded_entries.append(encoded_entry)

            # Upload to IPFS
            result = await upload_json_to_ipfs(data=encoded_entries, api_url=ipfs_node_url)
            if result['success']:
                profile_cids['miners'][miner_id] = result['cid']
                logger.info(f"Uploaded miner profile for {miner_id} to IPFS: {result['cid']}")
            else:
                logger.error(f"Failed to upload miner profile for {miner_id}: {result['error']}")

    except Exception as e:
        logger.error(f"Error uploading profiles to IPFS: {e}")

    return profile_cids


async def process_completed_storage_requests(conn, user_profiles):
    """
    Process 'processed' requests, update profiles, and submit to chain.
    
    Args:
        conn: Database connection
        user_profiles: Dictionary of user profiles
        
    Returns:
        List of pin requests for blockchain submission
    """
    try:
        # Fetch all processed requests from pending_pool
        processed_requests = await conn.fetch("""
            SELECT owner, file_hash, main_req_hash, selected_miners
            FROM pending_pool
            WHERE status = $1
            """, "processed")

        if not processed_requests:
            logger.info("No processed requests found to update pin and storage.")
            return []

        logger.info(f"Found {len(processed_requests)} processed requests to update")

        # Process each request to prepare for blockchain submission
        pin_requests = []

        for request in processed_requests:
            owner = request['owner']
            file_hash = request['file_hash']
            main_req_hash = request['main_req_hash']

            # Parse selected_miners from database format
            db_selected_miners = request['selected_miners']
            if isinstance(db_selected_miners, str):
                # Strip curly braces and split by comma
                parsed_miners = db_selected_miners.strip('{}').split(',')
                # Filter out empty strings
                selected_miners = [m for m in parsed_miners if m]
            elif isinstance(db_selected_miners, list):
                selected_miners = db_selected_miners  # Already a list
            else:
                selected_miners = []  # Default to empty list if None or other type

            # Upload the updated user profile to IPFS
            if owner in user_profiles and user_profiles[owner]:
                encoded_entries = []
                total_file_size = 0
                total_files_pinned = 0

                for entry in user_profiles[owner]:
                    # Calculate total file size and count
                    file_size = entry.get('file_size_in_bytes', 0)
                    total_file_size += file_size if file_size else 0
                    total_files_pinned += 1

                    # Encode file_hash for blockchain submission
                    entry_file_hash = entry.get('file_hash', '')
                    file_hash_encoded = None

                    if entry_file_hash:
                        file_hash_encoded = list(
                            bytes.fromhex(entry_file_hash.encode('utf-8').hex()))

                    encoded_entry = entry.copy()
                    encoded_entry['file_hash'] = file_hash_encoded

                    # Encode main_req_hash if present
                    entry_main_req_hash = entry.get('main_req_hash', '')
                    if entry_main_req_hash:
                        main_req_hash_encoded = list(
                            bytes.fromhex(entry_main_req_hash.encode('utf-8').hex()))
                        encoded_entry['main_req_hash'] = main_req_hash_encoded

                    encoded_entries.append(encoded_entry)

                # Upload to IPFS
                result = await upload_json_to_ipfs(data=encoded_entries)
                if result['success']:
                    user_profile_cid = result['cid']
                    logger.info(f"Uploaded user profile for {owner} to IPFS: {user_profile_cid}")

                    # Encode main_req_hash for blockchain
                    encoded_main_req_hash = None
                    if main_req_hash:
                        encoded_main_req_hash = list(
                            bytes.fromhex(main_req_hash.encode('utf-8').hex()))

                    # Add to pin_requests for blockchain submission
                    pin_request = {"storage_request_owner": owner,
                        "storage_request_file_hash": encoded_main_req_hash,
                        "file_size": total_file_size, "user_profile_cid": user_profile_cid,
                        "total_files_pinned": total_files_pinned}
                    pin_requests.append(pin_request)

                    # Delete the processed request after successful upload
                    await delete_completed_storage_request(conn, owner, file_hash, main_req_hash)
                else:
                    logger.error(f"Failed to upload user profile for {owner}: {result['error']}")
            else:
                logger.warning(f"No user profile found for owner {owner}")

        logger.info(f"Prepared {len(pin_requests)} pin requests for blockchain submission")
        return pin_requests

    except Exception as e:
        logger.error(f"Error processing completed storage requests: {e}")
        return []


async def prepare_miner_profile_updates(miner_profiles):
    """
    Prepare miner profile updates for blockchain submission.
    
    Args:
        miner_profiles: Dictionary of miner profiles
        
    Returns:
        List of miner profile updates for blockchain submission
    """
    try:
        miner_updates = []

        for miner_id, entries in miner_profiles.items():
            if not entries:
                continue

            # Calculate total files and size
            total_files = len(entries)
            total_size = sum(entry.get('file_size_in_bytes', 0) for entry in entries)

            # Encode entries for blockchain
            encoded_entries = []
            for entry in entries:
                file_hash = entry.get('file_hash', '')
                file_hash_encoded = None

                if file_hash:
                    file_hash_encoded = list(bytes.fromhex(file_hash.encode('utf-8').hex()))

                encoded_entry = entry.copy()
                encoded_entry['file_hash'] = file_hash_encoded
                encoded_entries.append(encoded_entry)

            # Upload to IPFS
            result = await upload_json_to_ipfs(data=encoded_entries)
            if result['success']:
                cid = result['cid']
                logger.info(f"Uploaded miner profile for {miner_id} to IPFS: {cid}")

                miner_update = {"miner_node_id": miner_id, "cid": cid, "files_count": total_files,
                    "files_size": total_size}
                miner_updates.append(miner_update)
            else:
                logger.error(f"Failed to upload miner profile for {miner_id}: {result['error']}")

        logger.info(
            f"Prepared {len(miner_updates)} miner profile updates for blockchain submission")
        return miner_updates

    except Exception as e:
        logger.error(f"Error preparing miner profile updates: {e}")
        return []


async def prepare_pin_check_metrics(health_results):
    """
    Prepare pin check metrics for blockchain submission.
    
    Args:
        health_results: List of miner health check results
        
    Returns:
        List of pin check metrics for blockchain submission
    """
    try:
        metrics = []

        for result in health_results:
            node_id = result['node_id']

            # Extract content verification stats
            content_verification = result.get('content_verification', {})
            total_cids = content_verification.get('total_cids', 0)
            successful_cids = content_verification.get('successful_cids', 0)

            if total_cids > 0:
                metrics.append({"node_id": node_id, "total_pin_checks": total_cids,
                    "successful_pin_checks": successful_cids})

        logger.info(f"Prepared pin check metrics for {len(metrics)} miners")
        return metrics

    except Exception as e:
        logger.error(f"Error preparing pin check metrics: {e}")
        return []
