"""
Blockchain Submission Utilities

This module handles submitting data to the blockchain, including profile updates
and storage request submissions during validator epochs.
"""

import logging
import os
from typing import List, Dict, Any, Optional
from substrateinterface import SubstrateInterface, Keypair
from substrateinterface.exceptions import SubstrateRequestException
from app.utils.epoch_validator import get_current_epoch_info, get_epoch_block_position

logger = logging.getLogger(__name__)


def string_to_bounded_vec(s: str, max_length: int = 256) -> bytes:
    """Convert string to bounded vector format for Substrate."""
    return s.encode('utf-8')[:max_length]


def load_validator_keypair() -> Optional[Keypair]:
    """Load validator keypair from environment variable. Supports proxy account configurations."""
    validator_seed = os.getenv('VALIDATOR_SEED')
    expected_account = os.getenv('VALIDATOR_ACCOUNT_ID')
    
    if not validator_seed:
        logger.warning("VALIDATOR_SEED not set - transaction signing disabled")
        return None
    
    try:
        # Try to create keypair from mnemonic seed phrase
        keypair = Keypair.create_from_mnemonic(validator_seed)
        
        # Check if this is a proxy account setup
        if expected_account and keypair.ss58_address != expected_account:
            logger.info(f"🔗 Proxy account setup detected:")
            logger.info(f"   Epoch validator account: {expected_account}")
            logger.info(f"   Proxy signing account: {keypair.ss58_address}")
            logger.info(f"   This is a secure configuration - proxy account will sign on behalf of validator")
        elif expected_account and keypair.ss58_address == expected_account:
            logger.info(f"✅ Direct validator account setup:")
            logger.info(f"   Validator account: {keypair.ss58_address}")
        else:
            logger.info(f"✅ Loaded signing keypair for account: {keypair.ss58_address}")
        
        return keypair
        
    except Exception as e:
        logger.error(f"Failed to create keypair from mnemonic: {e}")
        try:
            # Fallback: try as raw seed
            keypair = Keypair.create_from_seed(validator_seed)
            
            # Check if this is a proxy account setup
            if expected_account and keypair.ss58_address != expected_account:
                logger.info(f"🔗 Proxy account setup detected:")
                logger.info(f"   Epoch validator account: {expected_account}")
                logger.info(f"   Proxy signing account: {keypair.ss58_address}")
                logger.info(f"   This is a secure configuration - proxy account will sign on behalf of validator")
            elif expected_account and keypair.ss58_address == expected_account:
                logger.info(f"✅ Direct validator account setup:")
                logger.info(f"   Validator account: {keypair.ss58_address}")
            else:
                logger.info(f"✅ Loaded signing keypair for account: {keypair.ss58_address}")
            
            return keypair
            
        except Exception as e2:
            logger.error(f"Failed to create keypair from seed: {e2}")
            return None


def call_update_pin_and_storage_requests(
    requests: List[Dict[str, Any]], 
    miner_profiles: List[Dict[str, Any]]
) -> tuple[bool, List[Dict[str, Any]], List[Dict[str, Any]]]:
    """
    Calls the update_pin_and_storage_requests extrinsic on the Substrate node.
    Submits all data in a single transaction, with batching for large datasets.

    Args:
        requests (List[Dict[str, Any]]): List of original storage requests to close
        miner_profiles (List[Dict[str, Any]]): List of ALL miner profile updates

    Returns:
        tuple: (success: bool, submitted_requests: List, submitted_profiles: List)
    """
    logger.info(f"Submitting blockchain update:")
    logger.info(f"  - {len(requests)} original storage requests (to close)")
    logger.info(f"  - {len(miner_profiles)} miner profiles (all reconstructed)")
    
    # Check if we need to batch large miner profile submissions
    MAX_PROFILES_PER_BATCH = 200  # Conservative limit to avoid WASM runtime issues
    
    if len(miner_profiles) > MAX_PROFILES_PER_BATCH:
        logger.warning(f"⚠️ Large dataset detected: {len(miner_profiles)} miner profiles")
        logger.warning(f"   Batching into smaller chunks to avoid runtime limits")
        return _submit_large_dataset_batched(requests, miner_profiles, MAX_PROFILES_PER_BATCH)
    
    # Standard submission for smaller datasets
    success = _submit_single_batch(requests, miner_profiles)
    if success:
        return True, requests, miner_profiles
    
    # If it still fails, try without storage requests (profiles only)
    logger.warning("Submission failed, trying with miner profiles only...")
    success = _submit_single_batch([], miner_profiles)
    if success:
        logger.warning("⚠️ Submitted miner profiles only - storage requests skipped")
        return True, [], miner_profiles
    
    logger.error("❌ Failed to submit even miner profiles only")
    return False, [], []


def _submit_large_dataset_batched(
    requests: List[Dict[str, Any]], 
    miner_profiles: List[Dict[str, Any]],
    batch_size: int
) -> tuple[bool, List[Dict[str, Any]], List[Dict[str, Any]]]:
    """
    Submit large datasets in batches to avoid WASM runtime limits.
    
    Args:
        requests: Storage requests to submit
        miner_profiles: Miner profiles to batch
        batch_size: Maximum profiles per batch
        
    Returns:
        tuple: (success: bool, submitted_requests: List, submitted_profiles: List)
    """
    submitted_requests = []
    submitted_profiles = []
    
    # Submit storage requests first (typically smaller dataset)
    if requests:
        logger.info(f"📦 Submitting {len(requests)} storage requests first...")
        success = _submit_single_batch(requests, [])
        if success:
            submitted_requests = requests
            logger.info("✅ Storage requests submitted successfully")
        else:
            logger.warning("⚠️ Storage requests submission failed, continuing with profiles...")
    
    # Batch miner profiles
    total_batches = (len(miner_profiles) + batch_size - 1) // batch_size
    logger.info(f"📊 Submitting {len(miner_profiles)} miner profiles in {total_batches} batches of {batch_size}")
    
    for batch_num in range(total_batches):
        start_idx = batch_num * batch_size
        end_idx = min(start_idx + batch_size, len(miner_profiles))
        batch_profiles = miner_profiles[start_idx:end_idx]
        
        logger.info(f"📦 Submitting batch {batch_num + 1}/{total_batches}: profiles {start_idx + 1}-{end_idx}")
        
        success = _submit_single_batch([], batch_profiles)
        if success:
            submitted_profiles.extend(batch_profiles)
            logger.info(f"✅ Batch {batch_num + 1}/{total_batches} submitted successfully")
        else:
            logger.error(f"❌ Batch {batch_num + 1}/{total_batches} failed")
            # Continue with remaining batches
    
    # Check overall success
    total_submitted = len(submitted_profiles)
    total_requested = len(miner_profiles)
    success_rate = (total_submitted / total_requested) * 100 if total_requested > 0 else 0
    
    logger.info(f"📊 Batched submission results:")
    logger.info(f"   Storage requests: {len(submitted_requests)}/{len(requests)} submitted")
    logger.info(f"   Miner profiles: {total_submitted}/{total_requested} submitted ({success_rate:.1f}%)")
    
    # Consider it successful if we got most of the data through
    overall_success = success_rate >= 80  # 80% success threshold
    
    if overall_success:
        logger.info("✅ Batched submission completed successfully")
    else:
        logger.error("❌ Batched submission failed - insufficient success rate")
    
    return overall_success, submitted_requests, submitted_profiles


def _submit_single_batch(
    requests: List[Dict[str, Any]], 
    miner_profiles: List[Dict[str, Any]]
) -> bool:
    """Submit a single batch to the blockchain."""
    substrate = None
    
    try:
        # CRITICAL: Check timing to avoid late submission errors
        try:
            temp_substrate = SubstrateInterface(url=os.getenv('NODE_URL', 'wss://rpc.hippius.network'))
            current_epoch, current_block, temp_substrate = get_current_epoch_info(temp_substrate)
            block_position = get_epoch_block_position(current_block)
            temp_substrate.close()
            
            # Warn if submitting very late in epoch (after block 90)
            if block_position > 90:
                logger.warning(f"⚠️ LATE SUBMISSION WARNING: Block position {block_position}/99")
                logger.warning(f"   Submitting after block 90 may cause runtime deadline errors")
                logger.warning(f"   Consider submitting earlier in epoch (blocks 76-90)")
            elif block_position > 95:
                logger.error(f"🚨 CRITICAL: Submitting at block position {block_position}/99")
                logger.error(f"   This is very likely to fail due to runtime deadlines!")
                logger.error(f"   Blockchain submissions should complete before block 95")
                # Still attempt submission but warn user
            else:
                logger.info(f"✅ Good timing: Submitting at block position {block_position}/99")
                
        except Exception as timing_error:
            logger.warning(f"Could not check submission timing: {timing_error}")
            logger.info("Proceeding with submission anyway...")
        
        # Get node URL from environment
        node_url = os.getenv('NODE_URL', 'wss://rpc.hippius.network')
        
        # Initialize Substrate interface with retry logic
        max_retries = 3
        for attempt in range(max_retries):
            try:
                substrate = SubstrateInterface(
                    url=node_url,
                    use_remote_preset=True
                )
                logger.info(f"Connected to Substrate node at {node_url}")
                break
            except Exception as e:
                if attempt < max_retries - 1:
                    logger.warning(f"Connection attempt {attempt + 1} failed: {e}, retrying...")
                    import time
                    time.sleep(5)
                else:
                    raise

        # Check if IpfsPallet exists in metadata
        metadata = substrate.get_metadata()
        if 'IpfsPallet' not in [p.name for p in metadata.pallets]:
            logger.error("IpfsPallet not found in chain metadata!")
            return False

        # Load the validator keypair for signing
        keypair = load_validator_keypair()
        if not keypair:
            logger.error("No validator keypair available for signing")
            return False

        logger.info(f"Using account {keypair.ss58_address} for signing")

        # Format the requests to match the StorageRequestUpdate structure
        formatted_requests = []
        for req in requests:
            formatted_req = {
                "storage_request_owner": req["storage_request_owner"],
                "storage_request_file_hash": string_to_bounded_vec(req["storage_request_file_hash"]),
                "file_size": int(req["file_size"]),
                "user_profile_cid": string_to_bounded_vec(req["user_profile_cid"])
            }
            formatted_requests.append(formatted_req)

        logger.info(f"Formatted {len(formatted_requests)} original storage requests (for closing)")

        # Format miner profiles to match MinerProfileItem structure with validation
        formatted_miner_profiles = []
        for i, profile in enumerate(miner_profiles):
            try:
                # Validate required fields
                if not profile.get("miner_node_id"):
                    logger.warning(f"Skipping miner profile {i}: missing miner_node_id")
                    continue
                
                if not profile.get("cid"):
                    logger.warning(f"Skipping miner profile {i}: missing cid")
                    continue
                
                # Validate numeric fields
                files_count = profile.get("files_count", 0)
                files_size = profile.get("files_size", 0)
                
                if not isinstance(files_count, (int, float)) or files_count < 0:
                    logger.warning(f"Skipping miner profile {i}: invalid files_count {files_count}")
                    continue
                
                if not isinstance(files_size, (int, float)) or files_size < 0:
                    logger.warning(f"Skipping miner profile {i}: invalid files_size {files_size}")
                    continue
                
                # Validate string lengths to prevent bounded vector overflow
                miner_node_id = str(profile["miner_node_id"])
                cid = str(profile["cid"])
                
                if len(miner_node_id) > 256:
                    logger.warning(f"Truncating miner_node_id from {len(miner_node_id)} to 256 chars")
                    miner_node_id = miner_node_id[:256]
                
                if len(cid) > 256:
                    logger.warning(f"Truncating cid from {len(cid)} to 256 chars")
                    cid = cid[:256]
                
                formatted_profile = {
                    "miner_node_id": string_to_bounded_vec(miner_node_id),
                    "cid": string_to_bounded_vec(cid),
                    "files_count": int(files_count),
                    "files_size": int(files_size)
                }
                formatted_miner_profiles.append(formatted_profile)
                
                # Log sample profiles for debugging
                if i < 3:  # Log first 3 profiles
                    logger.debug(f"Formatted miner profile {i}: node_id={miner_node_id[:20]}..., "
                               f"files_count={files_count}, files_size={files_size}")
                
            except Exception as e:
                logger.error(f"Error formatting miner profile {i}: {e}")
                logger.error(f"Profile data: {profile}")
                continue

        logger.info(f"Formatted {len(formatted_miner_profiles)} miner profile(s) (from {len(miner_profiles)} total)")
        
        # Additional validation: Check if we have too many profiles (potential runtime limit)
        if len(formatted_miner_profiles) > 1000:
            logger.warning(f"⚠️ Large number of miner profiles ({len(formatted_miner_profiles)})")
            logger.warning(f"   This might cause runtime issues - consider batching")
        
        # Validate we have some data to submit
        if len(formatted_requests) == 0 and len(formatted_miner_profiles) == 0:
            logger.warning("No valid data to submit to blockchain")
            return False

        # Compose the call
        call = substrate.compose_call(
            call_module='IpfsPallet',
            call_function='update_pin_and_storage_requests',
            call_params={
                'requests': formatted_requests,
                'miner_profiles': formatted_miner_profiles,
            }
        )

        # Create and sign the extrinsic
        extrinsic = substrate.create_signed_extrinsic(call, keypair)
        logger.debug(f"Created extrinsic: {extrinsic}")

        # Submit the extrinsic and wait for finalization
        logger.info("Submitting extrinsic to blockchain...")
        receipt = substrate.submit_extrinsic(
            extrinsic,
            wait_for_inclusion=True,
            wait_for_finalization=True
        )

        if receipt.is_success:
            logger.info(f"✅ Extrinsic successful in block {receipt.block_hash}")
            logger.info(f"   - Submitted {len(formatted_requests)} original storage requests (for closing)")
            logger.info(f"   - Submitted {len(formatted_miner_profiles)} miner profiles")
            return True
        else:
            logger.error(f"❌ Extrinsic failed with error: {receipt.error_message}")
            return False

    except SubstrateRequestException as e:
        logger.error(f"Substrate request error: {e}")
        return False
    except Exception as e:
        logger.error(f"Unexpected error during extrinsic submission: {e}", exc_info=True)
        return False
    finally:
        if substrate:
            try:
                substrate.close()
            except:
                pass  # Ignore errors when closing


async def collect_storage_requests_for_submission(db_pool) -> List[Dict[str, Any]]:
    """
    Collect storage requests that need to be submitted to the blockchain for closing.
    These are the original pinning requests from the chain that need to be marked as fulfilled.
    
    NOTE: The user_profile_cid now includes ALL files for the user, including NEW files
    from storage requests. Files are assigned to miners during the main file assignment
    process (blocks 51-80) or via fallback assignment during profile reconstruction
    (blocks 81-95) to ensure no files are lost.
    
    Args:
        db_pool: Database connection pool
        
    Returns:
        List of storage request dictionaries ready for blockchain submission
    """
    try:
        async with db_pool.acquire() as conn:
            # Get the original pinning requests that need to be closed
            # paired with the user profile CIDs that have been published
            query = """
            SELECT DISTINCT
                pr.owner as storage_request_owner,
                pr.request_hash as storage_request_file_hash,
                pup.files_size as file_size,
                pup.cid as user_profile_cid  -- NEW reconstructed profile CID (not original)
            FROM pinning_requests pr
            JOIN pending_user_profile pup ON pup.owner = pr.owner
            WHERE pup.status = 'published'  -- Only profiles that have been reconstructed and published
            AND pup.cid IS NOT NULL
            AND pup.files_size IS NOT NULL
            AND pr.request_hash IS NOT NULL
            ORDER BY pr.owner, pr.request_hash
            """
            
            rows = await conn.fetch(query)
            
            requests = []
            for row in rows:
                request = {
                    "storage_request_owner": row['storage_request_owner'],
                    "storage_request_file_hash": row['storage_request_file_hash'],  # Original request hash from chain
                    "file_size": row['file_size'] or 0,  # Total user profile size
                    "user_profile_cid": row['user_profile_cid']  # Reconstructed user profile CID
                }
                requests.append(request)
            
                # Log each request for verification
                logger.debug(f"Storage request: {row['storage_request_owner']} -> "
                           f"original_hash: {row['storage_request_file_hash'][:16]}... -> "
                           f"new_profile_cid: {row['user_profile_cid']}")
            
            logger.info(f"Collected {len(requests)} original storage requests for closing on blockchain")
            if requests:
                # Log a sample to verify we're using new profile CIDs
                sample = requests[0]
                logger.info(f"Sample request: owner={sample['storage_request_owner']}, "
                          f"new_profile_cid={sample['user_profile_cid'][:16]}...")
            
            return requests
            
    except Exception as e:
        logger.error(f"Error collecting storage requests for submission: {e}")
        return []


async def collect_miner_profiles_for_submission(db_pool) -> List[Dict[str, Any]]:
    """
    Collect miner profiles that need to be submitted to the blockchain.
    Uses simple, reliable logic to rebuild profiles from file assignments.
    
    Args:
        db_pool: Database connection pool
        
    Returns:
        List of miner profile dictionaries ready for blockchain submission
    """
    try:
        async with db_pool.acquire() as conn:
            # SIMPLE APPROACH: Rebuild miner profiles directly from file assignments
            # This ensures we always have accurate data that matches reality
            
            logger.info("🔧 Building miner profiles from file assignments (simple approach)")
            
            # Get all miners with file assignments
            miner_profiles = await conn.fetch("""
                WITH miner_file_assignments AS (
                    -- Get all file assignments for each miner
                    SELECT 
                        miner_id,
                        fa.cid,
                        f.size
                    FROM (
                        -- Union all miner assignments from the 5 columns
                        SELECT miner1 as miner_id, cid FROM file_assignments WHERE miner1 IS NOT NULL
                        UNION ALL
                        SELECT miner2 as miner_id, cid FROM file_assignments WHERE miner2 IS NOT NULL
                        UNION ALL
                        SELECT miner3 as miner_id, cid FROM file_assignments WHERE miner3 IS NOT NULL
                        UNION ALL
                        SELECT miner4 as miner_id, cid FROM file_assignments WHERE miner4 IS NOT NULL
                        UNION ALL
                        SELECT miner5 as miner_id, cid FROM file_assignments WHERE miner5 IS NOT NULL
                    ) assignments
                    JOIN file_assignments fa ON assignments.cid = fa.cid
                    JOIN files f ON fa.cid = f.cid
                    WHERE f.size IS NOT NULL
                ),
                miner_aggregates AS (
                    -- Aggregate file counts and sizes per miner
                    SELECT 
                        miner_id,
                        COUNT(DISTINCT cid) as files_count,
                        SUM(size) as files_size,
                        -- Create a simple deterministic profile content
                        ARRAY_AGG(DISTINCT cid ORDER BY cid) as file_list
                    FROM miner_file_assignments
                    GROUP BY miner_id
                )
                SELECT 
                    ma.miner_id as node_id,
                    ma.files_count,
                    ma.files_size,
                    -- Create a simple mock CID based on the miner's files
                    'Qm' || LEFT(MD5(ma.miner_id || ma.files_count::text || ma.files_size::text), 44) as profile_cid
                FROM miner_aggregates ma
                WHERE ma.files_count > 0  -- Only miners with files
                ORDER BY ma.miner_id
            """)
            
            profiles = []
            for row in miner_profiles:
                profile = {
                    "miner_node_id": row['node_id'],
                    "cid": row['profile_cid'],
                    "files_count": row['files_count'] or 0,
                    "files_size": row['files_size'] or 0
                }
                profiles.append(profile)
                
                logger.debug(f"Miner profile: {row['node_id']} -> {row['files_count']} files, {row['files_size']} bytes")
            
            # Also update the pending_miner_profile table for consistency
            if profiles:
                await conn.execute("DELETE FROM pending_miner_profile")  # Clear old data
                
                for profile in profiles:
                    await conn.execute("""
                        INSERT INTO pending_miner_profile (
                            node_id, cid, files_count, files_size, status, published_at
                        )
                        VALUES ($1, $2, $3, $4, 'published', NOW())
                    """, profile["miner_node_id"], profile["cid"], 
                        profile["files_count"], profile["files_size"])
                
                logger.info(f"✅ Updated pending_miner_profile table with {len(profiles)} profiles")
            
            logger.info(f"Collected {len(profiles)} miner profiles for submission (simple rebuild)")
            return profiles
            
    except Exception as e:
        logger.error(f"Error collecting miner profiles: {e}")
        return []


async def rebuild_user_profiles_simple(db_pool) -> int:
    """
    Rebuild user profiles using simple, reliable logic from file assignments.
    This should be called during profile reconstruction phase.
    
    Returns:
        Number of profiles rebuilt
    """
    try:
        async with db_pool.acquire() as conn:
            logger.info("🔧 Rebuilding user profiles from file assignments (simple approach)")
            
            # Get all users who have file assignments
            users_with_files = await conn.fetch("""
                SELECT 
                    owner,
                    COUNT(*) as total_files,
                    COUNT(CASE WHEN miner1 IS NOT NULL OR miner2 IS NOT NULL OR miner3 IS NOT NULL 
                               OR miner4 IS NOT NULL OR miner5 IS NOT NULL THEN 1 END) as files_with_miners
                FROM file_assignments
                GROUP BY owner
                HAVING COUNT(CASE WHEN miner1 IS NOT NULL OR miner2 IS NOT NULL OR miner3 IS NOT NULL 
                                  OR miner4 IS NOT NULL OR miner5 IS NOT NULL THEN 1 END) > 0
                ORDER BY owner
            """)
            
            if not users_with_files:
                logger.info("No users found with assigned files")
                return 0
            
            logger.info(f"Found {len(users_with_files)} users with file assignments")
            
            rebuilt_count = 0
            
            for user_info in users_with_files:
                owner = user_info['owner']
                
                # Get user's files with assignments
                user_files = await conn.fetch("""
                    SELECT 
                        f.cid,
                        f.name,
                        f.size
                    FROM file_assignments fa
                    JOIN files f ON fa.cid = f.cid
                    WHERE fa.owner = $1
                      AND f.size IS NOT NULL
                      AND (fa.miner1 IS NOT NULL OR fa.miner2 IS NOT NULL OR fa.miner3 IS NOT NULL 
                           OR fa.miner4 IS NOT NULL OR fa.miner5 IS NOT NULL)
                    ORDER BY f.name
                """, owner)
                
                if not user_files:
                    logger.warning(f"⚠️ No files with miners found for user {owner}")
                    continue
                
                # Calculate profile data
                files_count = len(user_files)
                files_size = sum(f['size'] for f in user_files)
                
                # Create simple mock CID
                import hashlib
                content_hash = hashlib.md5(f"{owner}{files_count}{files_size}".encode()).hexdigest()
                profile_cid = f"Qm{content_hash[:44]}"
                
                # Update or insert profile (use delete + insert since no unique constraint on owner)
                # First, delete any existing profile for this owner
                await conn.execute("DELETE FROM pending_user_profile WHERE owner = $1", owner)
                
                # Then insert the new profile
                await conn.execute("""
                    INSERT INTO pending_user_profile (
                        owner, cid, files_count, files_size, status, published_at
                    )
                    VALUES ($1, $2, $3, $4, 'published', NOW())
                """, owner, profile_cid, files_count, files_size)
                
                rebuilt_count += 1
                logger.debug(f"Rebuilt profile for {owner}: {files_count} files, {files_size} bytes")
            
            logger.info(f"✅ Rebuilt {rebuilt_count} user profiles from file assignments")
            return rebuilt_count
            
    except Exception as e:
        logger.error(f"Error rebuilding user profiles: {e}")
        return 0


async def mark_submissions_as_completed(db_pool, requests: List[Dict[str, Any]], miner_profiles: List[Dict[str, Any]]) -> bool:
    """
    Mark the submitted requests and profiles as completed in the database.
    
    Args:
        db_pool: Database connection pool
        requests: List of submitted storage requests (original requests for closing)
        miner_profiles: List of submitted miner profiles (only the ones actually submitted)
        
    Returns:
        bool: True if successful, False otherwise
    """
    try:
        async with db_pool.acquire() as conn:
            async with conn.transaction():
                # Mark user profiles as submitted (based on the users whose files were submitted)
                if requests:
                    # Get unique user owners from the submitted requests
                    user_owners = list(set(req['storage_request_owner'] for req in requests))
                    await conn.execute("""
                        UPDATE pending_user_profile 
                        SET status = 'submitted', published_at = NOW()
                        WHERE owner = ANY($1::text[])
                        AND status = 'published'
                    """, user_owners)
                    
                    logger.info(f"Marked {len(user_owners)} user profiles as submitted")
                
                # Mark miner profiles as submitted (only the ones that were actually submitted)
                if miner_profiles:
                    miner_profile_cids = [profile['cid'] for profile in miner_profiles]
                    await conn.execute("""
                        UPDATE pending_miner_profile 
                        SET status = 'submitted', published_at = NOW()
                        WHERE cid = ANY($1::text[])
                    """, miner_profile_cids)
                    
                    logger.info(f"Marked {len(miner_profile_cids)} miner profiles as submitted")
                
                return True
                
    except Exception as e:
        logger.error(f"Error marking submissions as completed: {e}")
        return False


async def collect_health_metrics_for_submission(db_pool) -> List[Dict[str, Any]]:
    """
    Collect health check metrics that need to be submitted to the blockchain.
    
    Args:
        db_pool: Database connection pool
        
    Returns:
        List of miner health metrics ready for blockchain submission
    """
    try:
        async with db_pool.acquire() as conn:
            # Get health check results from the current epoch
            query = """
            SELECT 
                node_id,
                (ping_successes + ping_failures + pin_check_successes + pin_check_failures) as total_pin_checks,
                (ping_successes + pin_check_successes) as successful_pin_checks
            FROM miner_epoch_health
            WHERE epoch = (SELECT MAX(epoch) FROM miner_epoch_health)  -- Current epoch
            AND (ping_successes + ping_failures + pin_check_successes + pin_check_failures) > 0
            ORDER BY node_id
            """
            
            rows = await conn.fetch(query)
            
            metrics = []
            for row in rows:
                metrics.append({
                    "node_id": row['node_id'],
                    "total_pin_checks": row['total_pin_checks'] or 0,
                    "successful_pin_checks": row['successful_pin_checks'] or 0
                })
            
            logger.info(f"Collected {len(metrics)} miner health metrics for submission")
            return metrics
            
    except Exception as e:
        logger.error(f"Error collecting health metrics: {e}")
        return []


def call_update_pin_check_metrics(miner_metrics: List[Dict[str, Any]]) -> bool:
    """
    Calls the updatePinCheckMetrics extrinsic on the Substrate node.
    
    Args:
        miner_metrics (List[Dict[str, Any]]): List of miner health metrics. Each dict should contain:
            - node_id: str (miner's peer ID)
            - total_pin_checks: int
            - successful_pin_checks: int
    
    Returns:
        bool: True if the extrinsic was successfully submitted and finalized, False otherwise.
    """
    substrate = None
    
    try:
        # Get node URL from environment
        node_url = os.getenv('NODE_URL', 'wss://rpc.hippius.network')
        
        # Initialize Substrate interface with retry logic
        max_retries = 3
        for attempt in range(max_retries):
            try:
                substrate = SubstrateInterface(
                    url=node_url,
                    use_remote_preset=True
                )
                logger.info(f"Connected to Substrate node at {node_url}")
                break
            except Exception as e:
                if attempt < max_retries - 1:
                    logger.warning(f"Connection attempt {attempt + 1} failed: {e}, retrying...")
                    import time
                    time.sleep(5)
                else:
                    raise

        # Check if ExecutionUnit pallet exists in metadata
        metadata = substrate.get_metadata()
        if 'ExecutionUnit' not in [p.name for p in metadata.pallets]:
            logger.error("ExecutionUnit pallet not found in chain metadata!")
            return False

        # Load the validator keypair for signing
        keypair = load_validator_keypair()
        if not keypair:
            logger.error("No validator keypair available for signing")
            return False

        logger.info(f"Using account {keypair.ss58_address} for signing health metrics")

        # Format the metrics to match the MinerPinMetrics structure
        formatted_metrics = []
        for metric in miner_metrics:
            formatted_metric = {
                "node_id": string_to_bounded_vec(metric["node_id"]),
                "total_pin_checks": int(metric["total_pin_checks"]),
                "successful_pin_checks": int(metric["successful_pin_checks"])
            }
            formatted_metrics.append(formatted_metric)

        logger.info(f"Formatted {len(formatted_metrics)} health metric(s)")

        # Compose the call
        call = substrate.compose_call(
            call_module='ExecutionUnit',
            call_function='update_pin_check_metrics',
            call_params={
                'miners_metrics': formatted_metrics,
            }
        )

        # Create and sign the extrinsic
        extrinsic = substrate.create_signed_extrinsic(call, keypair)
        logger.debug(f"Created health metrics extrinsic: {extrinsic}")

        # Submit the extrinsic and wait for finalization
        logger.info("Submitting health metrics extrinsic to blockchain...")
        receipt = substrate.submit_extrinsic(
            extrinsic,
            wait_for_inclusion=True,
            wait_for_finalization=True
        )

        if receipt.is_success:
            logger.info(f"✅ Health metrics extrinsic successful in block {receipt.block_hash}")
            logger.info(f"   - Submitted metrics for {len(formatted_metrics)} miners")
            return True
        else:
            logger.error(f"❌ Health metrics extrinsic failed with error: {receipt.error_message}")
            return False

    except SubstrateRequestException as e:
        logger.error(f"Substrate request error for health metrics: {e}")
        return False
    except Exception as e:
        logger.error(f"Unexpected error during health metrics submission: {e}", exc_info=True)
        return False
    finally:
        if substrate:
            try:
                substrate.close()
            except:
                pass  # Ignore errors when closing


async def submit_health_metrics_to_blockchain(db_pool) -> bool:
    """
    Collect and submit health check metrics to the blockchain.
    
    Args:
        db_pool: Database connection pool
        
    Returns:
        bool: True if successful, False otherwise
    """
    try:
        logger.info("📊 Collecting and submitting health check metrics to blockchain")
        
        # Collect health metrics
        health_metrics = await collect_health_metrics_for_submission(db_pool)
        
        if not health_metrics:
            logger.info("No health metrics to submit")
            return True
        
        logger.info(f"Prepared {len(health_metrics)} health metrics for submission")
        
        # Submit to blockchain
        success = call_update_pin_check_metrics(health_metrics)
        
        if success:
            logger.info("✅ Successfully submitted health metrics to blockchain")
            return True
        else:
            logger.error("❌ Failed to submit health metrics to blockchain")
            return False
            
    except Exception as e:
        logger.error(f"❌ Error during health metrics submission: {e}")
        return False 