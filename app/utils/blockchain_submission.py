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
    Submits all data in a single transaction.

    Args:
        requests (List[Dict[str, Any]]): List of original storage requests to close
        miner_profiles (List[Dict[str, Any]]): List of ALL miner profile updates

    Returns:
        tuple: (success: bool, submitted_requests: List, submitted_profiles: List)
    """
    logger.info(f"🚀 STARTING BLOCKCHAIN SUBMISSION:")
    logger.info(f"📋 Data to submit:")
    logger.info(f"   - {len(requests)} original storage requests (to close)")
    logger.info(f"   - {len(miner_profiles)} miner profiles (all reconstructed)")
    
    # Debug: Show sample data if available
    if requests:
        sample_request = requests[0]
        logger.info(f"📄 Sample storage request: {sample_request['storage_request_owner']} -> {sample_request['user_profile_cid'][:16]}...")
    
    if miner_profiles:
        sample_profile = miner_profiles[0]
        logger.info(f"⛏️ Sample miner profile: {sample_profile['miner_node_id'][:20]}... -> {sample_profile['cid'][:16]}...")
    
    if not requests and not miner_profiles:
        logger.error("🚨 CRITICAL: No data to submit! Both requests and miner_profiles are empty")
        logger.error("   This suggests profile reconstruction did not complete successfully")
        return False, [], []
    
    # Submit everything in a single transaction (as it was working before)
    logger.info("📤 Attempting single transaction submission...")
    success = _submit_single_batch(requests, miner_profiles)
    if success:
        logger.info("✅ Single transaction submission successful!")
        return True, requests, miner_profiles
    
    # If it fails, try without storage requests (profiles only)
    logger.warning("⚠️ Single transaction failed, trying with miner profiles only...")
    success = _submit_single_batch([], miner_profiles)
    if success:
        logger.warning("⚠️ Submitted miner profiles only - storage requests skipped")
        return True, [], miner_profiles
    
    logger.error("❌ Failed to submit even miner profiles only")
    return False, [], []


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
            logger.error("🚨 CRITICAL: No validator keypair available for signing!")
            logger.error("❌ Environment check:")
            validator_seed = os.getenv('VALIDATOR_SEED')
            validator_account = os.getenv('VALIDATOR_ACCOUNT_ID')
            logger.error(f"   - VALIDATOR_SEED: {'✅ SET' if validator_seed else '❌ MISSING'}")
            logger.error(f"   - VALIDATOR_ACCOUNT_ID: {'✅ SET' if validator_account else '❌ MISSING'}")
            if not validator_seed:
                logger.error("🔥 VALIDATOR_SEED environment variable is required for transaction signing!")
                logger.error("🔥 No blockchain transactions can be sent without a valid signing key!")
            return False

        logger.info(f"✅ Validator keypair loaded successfully")
        logger.info(f"🔑 Using account {keypair.ss58_address} for signing")
        
        # Log expected vs actual account for transparency
        expected_account = os.getenv('VALIDATOR_ACCOUNT_ID')
        if expected_account and expected_account != keypair.ss58_address:
            logger.info(f"🔗 Proxy configuration: epoch validator {expected_account}, signing with {keypair.ss58_address}")
        elif expected_account:
            logger.info(f"✅ Direct signing: using validator account {keypair.ss58_address}")
        else:
            logger.info(f"⚠️ No VALIDATOR_ACCOUNT_ID set - signing with {keypair.ss58_address}")

        # Format the requests to match the StorageRequestUpdate structure
        formatted_requests = []
        for req in requests:
            logger.info(f"{req=}")
            formatted_req = {
                "storage_request_owner": req["storage_request_owner"],
                "storage_request_file_hash": string_to_bounded_vec(req["storage_request_file_hash"]),
                "file_size": int(req["file_size"]),
                "user_profile_cid": string_to_bounded_vec(req["user_profile_cid"])
            }
            formatted_requests.append(formatted_req)

        logger.info(f"Formatted {len(formatted_requests)} original storage requests (for closing)")

        # Format miner profiles to match MinerProfileItem structure with enhanced validation
        formatted_miner_profiles = []
        for i, profile in enumerate(miner_profiles):
            try:
                # Validate required fields exist and are not None
                if not profile.get("miner_node_id"):
                    logger.warning(f"Skipping miner profile {i}: missing or empty miner_node_id")
                    continue
                
                if not profile.get("cid"):
                    logger.warning(f"Skipping miner profile {i}: missing or empty cid")
                    continue
                
                # Validate and sanitize numeric fields
                files_count = profile.get("files_count", 0)
                files_size = profile.get("files_size", 0)
                
                # Check for valid numeric types and handle potential NaN/infinity
                if not isinstance(files_count, (int, float)) or files_count < 0 or not str(files_count).replace('.', '').isdigit():
                    logger.warning(f"Skipping miner profile {i}: invalid files_count {files_count} (type: {type(files_count)})")
                    continue
                
                if not isinstance(files_size, (int, float)) or files_size < 0 or not str(files_size).replace('.', '').isdigit():
                    logger.warning(f"Skipping miner profile {i}: invalid files_size {files_size} (type: {type(files_size)})")
                    continue
                
                # Convert to safe integers
                try:
                    files_count = int(float(files_count))
                    files_size = int(float(files_size))
                except (ValueError, OverflowError):
                    logger.warning(f"Skipping miner profile {i}: cannot convert to integer - files_count={files_count}, files_size={files_size}")
                    continue
                
                # Validate string fields and sanitize
                miner_node_id = str(profile["miner_node_id"]).strip()
                cid = str(profile["cid"]).strip()
                
                # Check for empty strings after stripping
                if not miner_node_id or not cid:
                    logger.warning(f"Skipping miner profile {i}: empty miner_node_id or cid after sanitization")
                    continue
                
                # Validate string lengths to prevent bounded vector overflow
                if len(miner_node_id) > 256:
                    logger.warning(f"Truncating miner_node_id from {len(miner_node_id)} to 256 chars")
                    miner_node_id = miner_node_id[:256]
                
                if len(cid) > 256:
                    logger.warning(f"Truncating cid from {len(cid)} to 256 chars")
                    cid = cid[:256]
                
                # Validate reasonable bounds (prevent obviously wrong values)
                if files_count > 1000000:  # 1M files seems unreasonable
                    logger.warning(f"Suspiciously high files_count {files_count} for miner {miner_node_id[:20]}..., capping at 1000000")
                    files_count = 1000000
                
                if files_size > 1000000000000:  # 1TB seems like a reasonable upper bound
                    logger.warning(f"Suspiciously high files_size {files_size} for miner {miner_node_id[:20]}..., capping at 1TB")
                    files_size = 1000000000000
                
                formatted_profile = {
                    "miner_node_id": string_to_bounded_vec(miner_node_id),
                    "cid": string_to_bounded_vec(cid),
                    "files_count": files_count,
                    "files_size": files_size
                }
                formatted_miner_profiles.append(formatted_profile)

                # Log sample profiles for debugging (first 3)
                if i < 3:
                    logger.debug(f"Formatted miner profile {i}: node_id={miner_node_id[:20]}..., "
                               f"files_count={files_count}, files_size={files_size}")
                
            except Exception as e:
                logger.error(f"Error formatting miner profile {i}: {e}")
                logger.error(f"Profile data: {profile}")
                continue

        logger.info(f"Formatted {len(formatted_miner_profiles)} valid miner profiles (from {len(miner_profiles)} total)")
        
        if len(formatted_miner_profiles) == 0:
            logger.error("🚨 CRITICAL: No valid miner profiles after formatting!")
            logger.error("   This suggests data corruption in miner profile generation")
            logger.error("   Check the file assignment and profile reconstruction logic")
            return False
        
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
        logger.info(f"✅ Created and signed extrinsic")
        logger.info(f"📋 Transaction details:")
        logger.info(f"   - Signing account: {keypair.ss58_address}")
        logger.info(f"   - Call module: IpfsPallet")
        logger.info(f"   - Call function: update_pin_and_storage_requests")
        logger.info(f"   - Storage requests: {len(formatted_requests)}")
        logger.info(f"   - Miner profiles: {len(formatted_miner_profiles)}")
        
        # Get the extrinsic hash before submission
        extrinsic_hash = extrinsic.extrinsic_hash
        
        # Convert binary hash to hex string for logging
        if isinstance(extrinsic_hash, bytes):
            extrinsic_hash_hex = "0x" + extrinsic_hash.hex()
        else:
            extrinsic_hash_hex = str(extrinsic_hash)
        
        logger.info(f"🔗 TRANSACTION HASH: {extrinsic_hash_hex}")
        logger.info(f"🚀 Submitting transaction {extrinsic_hash_hex} to blockchain...")

        # Submit the extrinsic and wait for finalization
        receipt = substrate.submit_extrinsic(
            extrinsic,
            wait_for_inclusion=True,
            wait_for_finalization=True
        )

        if receipt.is_success:
            # Convert block hash to hex string for logging
            block_hash_hex = "0x" + receipt.block_hash.hex() if isinstance(receipt.block_hash, bytes) else str(receipt.block_hash)
            
            logger.info(f"✅ ✨ TRANSACTION SUCCESSFUL! ✨")
            logger.info(f"🔗 Transaction Hash: {extrinsic_hash_hex}")
            logger.info(f"📦 Block Hash: {block_hash_hex}")
            logger.info(f"📊 Submitted Data:")
            logger.info(f"   - {len(formatted_requests)} original storage requests (for closing)")
            logger.info(f"   - {len(formatted_miner_profiles)} miner profiles")
            logger.info(f"🎯 IpfsPallet::UpdatePinAndStorageRequests transaction completed successfully!")
            return True
        else:
            logger.error(f"❌ TRANSACTION FAILED!")
            logger.error(f"🔗 Transaction Hash: {extrinsic_hash_hex}")
            logger.error(f"❌ Error: {receipt.error_message}")
            logger.error(f"📋 Failed transaction details:")
            logger.error(f"   - Storage requests: {len(formatted_requests)}")
            logger.error(f"   - Miner profiles: {len(formatted_miner_profiles)}")
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

    Args:
        db_pool: Database connection pool
        
    Returns:
        List of storage request dictionaries ready for blockchain submission
    """
    try:
        async with db_pool.acquire() as conn:
            query = """
            SELECT DISTINCT
                pr.owner as storage_request_owner,
                pr.request_hash as storage_request_file_hash,
                COALESCE(f.size, 0) as file_size,
                pup.cid as user_profile_cid
            FROM pinning_requests pr
            JOIN processed_pinning_requests ppr ON pr.request_hash = ppr.request_hash
            LEFT JOIN pending_user_profile pup ON pr.owner = pup.owner AND pup.status = 'published'
            LEFT JOIN files f ON pr.file_hash = f.cid
            WHERE pr.request_hash IS NOT NULL
            ORDER BY pr.owner
            """
            
            rows = await conn.fetch(query)

            requests = []
            for row in rows:
                logger.info(f">>>>request>>>>{row}")
                request = {
                    "storage_request_owner": row['storage_request_owner'],
                    "storage_request_file_hash": row['storage_request_file_hash'],
                    "file_size": row['file_size'] or 0,
                    "user_profile_cid": row['user_profile_cid']
                }
                requests.append(request)
            
            logger.info(f"✅ Collected {len(requests)} processed storage requests to close on blockchain")
            
            return requests
            
    except Exception as e:
        logger.error(f"Error collecting storage requests for submission: {e}")
        return []


async def collect_miner_profiles_for_submission(db_pool) -> List[Dict[str, Any]]:
    """
    Collect miner profiles that need to be submitted to the blockchain.
    
    Args:
        db_pool: Database connection pool
        
    Returns:
        List of miner profile dictionaries ready for blockchain submission
    """
    try:
        async with db_pool.acquire() as conn:
            # Get pending miner profiles that are published
            query = """
            SELECT 
                node_id as miner_node_id,
                cid,
                files_count,
                files_size
            FROM pending_miner_profile
            WHERE status = 'published'
            AND cid IS NOT NULL
            ORDER BY node_id
            """
            
            rows = await conn.fetch(query)
            
            profiles = []
            for row in rows:
                profiles.append({
                    "miner_node_id": row['miner_node_id'],
                    "cid": row['cid'],
                    "files_count": row['files_count'] or 0,
                    "files_size": row['files_size'] or 0
                })
            
            logger.info(f"Collected {len(profiles)} miner profiles for submission")
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