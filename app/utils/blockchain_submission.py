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

logger = logging.getLogger(__name__)


def string_to_bounded_vec(s: str, max_length: int = 256) -> bytes:
    """Convert string to bounded vector format for Substrate."""
    return s.encode('utf-8')[:max_length]


def load_validator_keypair() -> Optional[Keypair]:
    """Load validator keypair from environment variable."""
    validator_seed = os.getenv('VALIDATOR_SEED')
    if not validator_seed:
        logger.warning("VALIDATOR_SEED not set - transaction signing disabled")
        return None
    
    try:
        # Try to create keypair from mnemonic seed phrase
        keypair = Keypair.create_from_mnemonic(validator_seed)
        logger.info(f"Loaded validator keypair for account: {keypair.ss58_address}")
        return keypair
    except Exception as e:
        logger.error(f"Failed to create keypair from mnemonic: {e}")
        try:
            # Fallback: try as raw seed
            keypair = Keypair.create_from_seed(validator_seed)
            logger.info(f"Loaded validator keypair for account: {keypair.ss58_address}")
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
    logger.info(f"Submitting blockchain update:")
    logger.info(f"  - {len(requests)} original storage requests (to close)")
    logger.info(f"  - {len(miner_profiles)} miner profiles (all reconstructed)")
    
    # With the corrected data collection, we should be able to submit everything
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


def _submit_single_batch(
    requests: List[Dict[str, Any]], 
    miner_profiles: List[Dict[str, Any]]
) -> bool:
    """Submit a single batch to the blockchain."""
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

        # Format miner profiles to match MinerProfileItem structure
        formatted_miner_profiles = []
        for profile in miner_profiles:
            formatted_profile = {
                "miner_node_id": string_to_bounded_vec(profile["miner_node_id"]),
                "cid": string_to_bounded_vec(profile["cid"]),
                "files_count": int(profile["files_count"]),
                "files_size": int(profile["files_size"])
            }
            formatted_miner_profiles.append(formatted_profile)

        logger.info(f"Formatted {len(formatted_miner_profiles)} miner profile(s)")

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
                pup.cid as user_profile_cid
            FROM pinning_requests pr
            JOIN pending_user_profile pup ON pup.owner = pr.owner
            WHERE pup.status = 'published'
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
            
            logger.info(f"Collected {len(requests)} original storage requests for closing on blockchain")
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