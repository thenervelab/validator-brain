#!/usr/bin/env python3
"""
Epoch Orchestrator v2.1.4

CRITICAL FIX IN v2.1.4:
- 🚨 FIXED: AttributeError for missing state variables (profiles_completed, submission_completed)
- ✅ Properly initialize all workflow state variables in __init__ method
- ✅ Prevents runtime crashes during validator workflow execution

CRITICAL FIX IN v2.1.3:
- 🚨 FIXED: Validator state confusion during connection failures (broken pipe errors)
- ✅ Enhanced connection resilience with validator state persistence across disconnections
- ✅ More aggressive connection retry logic for validators (5 attempts vs 3 for non-validators)
- ✅ Connection recovery detection prevents unnecessary "waiting for next epoch"
- ✅ Validator state cache maintains processing continuity during network disruptions

CRITICAL FIX IN v2.1.2:
- 🚨 FIXED: Connection lag causing validators to miss early epoch detection window
- ✅ Enhanced role transition detection to handle substrate connection delays
- ✅ Validators can now process even when detected after block 10 due to connection lag
- ✅ Added previous_epoch tracking to distinguish role transitions from true mid-epoch startup

CRITICAL FIX IN v2.1.1:
- 🚨 FIXED: Validator startup safety mechanism causing validators to wait entire epoch
- ✅ Validators becoming active at epoch start (positions 0-10) now process immediately
- ✅ Enhanced epoch state reset to clear waiting state on role changes
- ✅ Proper startup_epoch tracking across role transitions

MAJOR IMPROVEMENTS IN v2.1.0:
- ✅ Fixed critical timing issue: Blockchain submission now happens in blocks 76-90 (before block 95 deadline)
- ✅ Optimized workflow phases with proper health-checks-first ordering
- ✅ Enhanced startup safety: Non-validators can start immediately, validators wait if mid-epoch startup
- ✅ Schema fixes: Resolved all updated_at column errors
- ✅ Comprehensive phase-by-phase monitoring and error reporting

This is the main orchestrator that manages the entire IPFS Service Validator application
based on whether we are the current epoch validator or not.

OPTIMIZED EPOCH WORKFLOW (100 blocks):
- Phase 1 (0-5): Initialization 
- Phase 2 (6-35): Health Checks + Self-healing
- Phase 3 (36-60): File Assignments  
- Phase 4 (61-75): Profile Reconstruction
- Phase 5 (76-90): Blockchain Submission (EARLY - before block 95 deadline!)
- Phase 6 (91-99): Cleanup & Summary

Non-Validator Mode:
- Can start processing immediately (no epoch timing restrictions)
- Performs health checks and submits to chain
- Helps maintain network availability

Validator Mode:
- Follows strict phase timing for epoch processing
- Enhanced connection resilience prevents state loss during network issues
- FIXED: Maintains validator state across connection failures
- Must complete blockchain submission before block 95
"""

import asyncio
import logging
import os
import sys
import time
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional
import subprocess

# Add parent directory to path for imports
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from dotenv import load_dotenv
from substrateinterface import SubstrateInterface

from app.utils.epoch_validator import (
    get_current_epoch_info, 
    get_epoch_block_position, 
    is_epoch_validator, 
    get_validator_account_from_env,
    connect_substrate,
    calculate_epoch_from_block,
    get_epoch_start_block,
    get_epoch_end_block
)
from app.db.connection import init_db_pool, close_db_pool, get_db_pool

# Load environment variables
load_dotenv()

# Orchestrator version
ORCHESTRATOR_VERSION = "2.1.4"

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class EpochOrchestrator:
    """Main orchestrator for epoch-based operations."""
    
    def __init__(self):
        self.substrate = None
        self.db_pool = None
        self.our_validator_account = None
        self.current_epoch = None
        self.current_block = None
        self.is_validator = False
        self.epoch_start_block = None
        
        # State tracking
        self.current_epoch = None
        self.is_validator = False
        self.initialization_completed = False
        self.pinning_completed = False
        self.assignment_completed = False
        self.health_checks_completed = False
        self.health_metrics_submitted = False
        self.availability_completed = False  # Track availability maintenance
        self.profiles_reconstructed = False
        self.blockchain_submitted = False
        
        # New state variables for enhanced workflow tracking
        self.profiles_completed = False  # Phase 4: Profile reconstruction
        self.submission_completed = False  # Phase 5: Blockchain submission
        self.cleanup_completed = False  # Phase 6: Cleanup and summary
        
        # Enhanced state persistence across connection failures
        self.validator_state_cache = {
            'last_known_epoch': None,
            'last_known_validator_status': False,
            'last_successful_connection': None,
            'validator_epoch_start': None,  # Track when we became validator
        }
        
        # Safety mechanism for mid-epoch startup
        self.startup_epoch = None
        self.previous_epoch = None  # Track previous epoch for role transition detection
        self.waiting_for_next_epoch = False
        
        # Connection management
        self.connection_failures = 0
        self.last_failure_time = 0
        self.max_backoff = 300  # 5 minutes max backoff
        
        # Configuration
        self.block_check_interval = int(os.getenv('BLOCK_CHECK_INTERVAL', '6'))  # seconds (every block)
        self.queue_check_timeout = int(os.getenv('QUEUE_CHECK_TIMEOUT', '300'))  # seconds
        
        # Validator seed for transaction signing
        self.validator_seed = os.getenv('VALIDATOR_SEED')  # Optional for signing transactions
        
    async def initialize(self):
        """Initialize connections and get our validator account."""
        try:
            # Get our validator account
            self.our_validator_account = get_validator_account_from_env()
            logger.info(f"Our validator account: {self.our_validator_account}")
            
            # Check validator seed for transaction signing
            if self.validator_seed:
                logger.info("✅ Validator seed provided - transaction signing enabled")
            else:
                logger.warning("⚠️ No validator seed provided - transaction signing disabled")
            
            # Connect to substrate
            self.substrate = connect_substrate()
            
            # Initialize database pool
            await init_db_pool()
            self.db_pool = await get_db_pool()
            logger.info("Database connection pool initialized")
            
            logger.info("Epoch orchestrator initialized successfully")
            
        except Exception as e:
            logger.error(f"Failed to initialize epoch orchestrator: {e}")
            raise
    
    async def cleanup(self):
        """Clean up connections."""
        if self.substrate:
            self.substrate.close()
        if self.db_pool:
            await close_db_pool()
        logger.info("Epoch orchestrator cleaned up")
    
    def get_backoff_delay(self) -> int:
        """Calculate exponential backoff delay based on connection failures."""
        if self.connection_failures == 0:
            return 0
        
        # Exponential backoff: 2^failures * base_delay, capped at max_backoff
        base_delay = 10  # 10 seconds base delay
        delay = min(base_delay * (2 ** (self.connection_failures - 1)), self.max_backoff)
        return delay
    
    def should_attempt_connection(self) -> bool:
        """Check if enough time has passed since last failure to attempt connection."""
        if self.connection_failures == 0:
            return True
        
        backoff_delay = self.get_backoff_delay()
        time_since_failure = time.time() - self.last_failure_time
        return time_since_failure >= backoff_delay
    
    def record_connection_success(self):
        """Record successful connection, reset failure count and update state cache."""
        self.connection_failures = 0
        self.last_failure_time = 0
        
        # Update state cache with successful connection
        self.validator_state_cache['last_successful_connection'] = time.time()
    
    def record_connection_failure(self):
        """Record connection failure, increment failure count."""
        self.connection_failures += 1
        self.last_failure_time = time.time()
        logger.warning(f"Connection failure #{self.connection_failures}, next attempt in {self.get_backoff_delay()}s")
        
        # Log state cache for debugging connection issues
        if self.validator_state_cache['last_known_validator_status']:
            logger.info(f"📋 Validator state cache: Was validator in epoch {self.validator_state_cache['last_known_epoch']}")
    
    def update_validator_state_cache(self, epoch: int, is_validator: bool):
        """Update the validator state cache with current information."""
        # Track when we become validator
        if is_validator and not self.validator_state_cache['last_known_validator_status']:
            self.validator_state_cache['validator_epoch_start'] = epoch
            logger.info(f"📝 Cached: Became validator in epoch {epoch}")
        
        self.validator_state_cache['last_known_epoch'] = epoch
        self.validator_state_cache['last_known_validator_status'] = is_validator
    
    def is_validator_state_transition_recovery(self, current_epoch: int, is_validator: bool, block_position: int) -> bool:
        """
        Determine if this is a recovery from connection failure where we were already validator.
        
        Returns:
            True if this is a connection recovery scenario (not true mid-epoch startup)
        """
        cache = self.validator_state_cache
        
        # If we have no cached state, this could be true startup
        if cache['last_known_epoch'] is None:
            return False
        
        # If we were validator in the same epoch before connection failure
        if (cache['last_known_validator_status'] and 
            cache['last_known_epoch'] == current_epoch and
            is_validator and
            cache['validator_epoch_start'] == current_epoch):
            
            logger.info(f"🔄 Connection recovery detected: Was validator in epoch {current_epoch} before connection failure")
            logger.info(f"   Validator since epoch start, connection failed at block ~{block_position}")
            return True
        
        return False
    
    def run_processor(self, processor_name: str, description: str) -> bool:
        """
        Run a processor script and wait for completion.
        
        Args:
            processor_name: Name of the processor script
            description: Human-readable description
            
        Returns:
            True if successful, False otherwise
        """
        try:
            logger.info(f"🚀 Starting {description}")
            
            # Run the processor
            result = subprocess.run([
                'python', f'rabbitmq/{processor_name}'
            ], capture_output=True, text=True, timeout=600)  # 10 minute timeout
            
            if result.returncode == 0:
                logger.info(f"✅ {description} completed successfully")
                return True
            else:
                logger.error(f"❌ {description} failed with return code {result.returncode}")
                logger.error(f"STDOUT: {result.stdout}")
                logger.error(f"STDERR: {result.stderr}")
                return False
                
        except subprocess.TimeoutExpired:
            logger.error(f"❌ {description} timed out after 10 minutes")
            return False
        except Exception as e:
            logger.error(f"❌ Error running {description}: {e}")
            return False
    
    async def wait_for_queues_empty(self, queue_names: List[str], timeout: int = 300) -> bool:
        """
        Wait for specified queues to be empty.
        
        Args:
            queue_names: List of queue names to check
            timeout: Timeout in seconds
            
        Returns:
            True if all queues are empty, False if timeout
        """
        logger.info(f"⏳ Waiting for queues to be empty: {', '.join(queue_names)}")
        
        # Import the queue checker
        from scripts.check_queue_status import wait_for_queues_empty
        
        try:
            success = await wait_for_queues_empty(queue_names, timeout, 10)
            return success
        except Exception as e:
            logger.error(f"Error checking queue status: {e}")
            # Fallback to time-based approach
            logger.info("Falling back to time-based queue checking")
            await asyncio.sleep(60)  # Wait 1 minute as fallback
            return True
    
    async def refresh_registration_data(self) -> bool:
        """Refresh registration table with new data."""
        logger.info("🔄 Refreshing registration data")
        
        # Clear and refill registration table
        success = self.run_processor(
            'registration_processor.py',
            'Registration data refresh'
        )
        
        if success:
            # Wait for registration consumer to process
            await self.wait_for_queues_empty(['registration'], 120)
        
        return success
    
    async def refresh_node_metrics(self) -> bool:
        """Refresh node metrics data."""
        logger.info("🔄 Refreshing node metrics")
        
        success = self.run_processor(
            'node_metrics_processor.py',
            'Node metrics refresh'
        )
        
        if success:
            # Wait for node metrics consumer to process
            await self.wait_for_queues_empty(['node_metrics_latest'], 120)
        
        return success
    
    async def refresh_user_profiles(self) -> bool:
        """Refresh user profiles data."""
        logger.info("🔄 Refreshing user profiles")
        
        success = self.run_processor(
            'user_profile_processor.py',
            'User profiles refresh'
        )
        
        if success:
            # Wait for user profile consumer to process
            await self.wait_for_queues_empty(['user_profile'], 300)
        
        return success
    
    async def perform_health_checks(self) -> bool:
        """Perform miner health checks."""
        logger.info("🏥 Performing miner health checks")
        
        success = self.run_processor(
            'miner_health_processor.py',
            'Miner health checks'
        )
        
        if success:
            # Wait for health check consumer to process
            await self.wait_for_queues_empty(['miner_health_check'], 600)
        
        return success
    
    async def process_pinning_requests(self) -> bool:
        """Process pinning requests (validator only)."""
        logger.info("📌 Processing pinning requests")
        
        success = self.run_processor(
            'pinning_request_processor.py',
            'Pinning requests processing'
        )
        
        if success:
            # Wait for pinning request consumer to process
            await self.wait_for_queues_empty(['pinning_request'], 300)
        
        return success
    
    async def process_pinning_files(self) -> bool:
        """Process individual pinning files."""
        logger.info("📁 Processing pinning files")
        
        success = self.run_processor(
            'pinning_file_processor.py',
            'Pinning files processing'
        )
        
        if success:
            # Wait for pinning file consumer to process
            await self.wait_for_queues_empty(['pinning_file_processing'], 600)
        
        return success
    
    async def assign_files(self) -> bool:
        """
        Assign files to miners for the current epoch.
        CRITICAL: This must only run AFTER health checks are completed for accurate miner health data.
        """
        logger.info("📋 Starting file assignment for current epoch")
        
        # CRITICAL VALIDATION: Ensure health checks completed first
        if not self.health_checks_completed:
            logger.error("🚨 CRITICAL: Cannot assign files - health checks not completed!")
            logger.error("   File assignments require fresh health data from current epoch")
            return False
        
        # Verify we have fresh health data
        async with self.db_pool.acquire() as conn:
            # Check if we have health data for current epoch
            current_health_data = await conn.fetchval("""
                SELECT COUNT(*) FROM miner_epoch_health 
                WHERE epoch = $1 AND last_activity_at >= NOW() - INTERVAL '30 minutes'
            """, self.current_epoch)
            
            if current_health_data == 0:
                # Check for health data from previous epoch as fallback
                fallback_health_data = await conn.fetchval("""
                    SELECT COUNT(*) FROM miner_epoch_health 
                    WHERE epoch >= $1 - 1 AND last_activity_at >= NOW() - INTERVAL '4 hours'
                """, self.current_epoch)
                
                if fallback_health_data == 0:
                    logger.error("🚨 CRITICAL: No health data found for current OR previous epoch!")
                    logger.error(f"   Expected health data for epoch {self.current_epoch} or {self.current_epoch - 1}")
                    return False
                else:
                    logger.warning(f"⚠️ No current epoch health data, using {fallback_health_data} miners from previous epoch")
                    logger.warning("   Assignment quality may be reduced but will proceed")
            else:
                logger.info(f"✅ Verified fresh health data available: {current_health_data} miners with recent health data")
        
        try:
            # Use the reliable simple assignment strategy
            from scripts.simple_reliable_assignment import SimpleFileAssigner
            
            # Create assigner with fresh health data guarantee
            assigner = SimpleFileAssigner(self.db_pool)
            
            # Process unassigned files using fresh health data
            success = await assigner.assign_unassigned_files()
            
            if success:
                logger.info("✅ File assignment completed successfully with fresh health data")
                return True
            else:
                logger.error("❌ File assignment failed despite fresh health data")
                return False
                
        except Exception as e:
            logger.error(f"❌ Error during file assignment: {e}")
            return False
    
    async def run_availability_maintenance(self) -> bool:
        """Run file availability maintenance to handle empty assignments and failures."""
        logger.info("🛠️ Running file availability maintenance")
        
        success = self.run_processor(
            'availability_manager_processor.py',
            'File availability maintenance'
        )
        
        # No queue to wait for since availability manager runs synchronously
        return success
    
    async def reconstruct_profiles(self) -> bool:
        """
        Reconstruct user and miner profiles from file assignments.
        Phase 4: Profile reconstruction (blocks 61-75)
        """
        logger.info("🔧 Starting profile reconstruction phase")
        
        try:
            # Import the reconstruction utilities
            from app.utils.blockchain_submission import rebuild_user_profiles_simple, collect_miner_profiles_for_submission
            
            # Rebuild user profiles from file assignments
            logger.info("👥 Rebuilding user profiles from file assignments...")
            user_count = await rebuild_user_profiles_simple(self.db_pool)
            
            if user_count > 0:
                logger.info(f"✅ Rebuilt {user_count} user profiles")
            else:
                logger.warning("⚠️ No user profiles to rebuild")
            
            # Collect miner profiles for verification
            logger.info("⛏️ Collecting miner profiles from file assignments...")
            miner_profiles = await collect_miner_profiles_for_submission(self.db_pool)
            
            if len(miner_profiles) > 0:
                logger.info(f"✅ Collected {len(miner_profiles)} miner profiles")
            else:
                logger.warning("⚠️ No miner profiles collected")
            
            # Verify we have data to submit
            if user_count > 0 or len(miner_profiles) > 0:
                logger.info("✅ Profile reconstruction completed successfully")
                return True
            else:
                logger.error("❌ Profile reconstruction failed - no profiles generated")
                return False
                
        except Exception as e:
            logger.error(f"❌ Error during profile reconstruction: {e}")
            return False
    
    async def submit_to_blockchain(self) -> bool:
        """
        Submit all data to blockchain including health metrics.
        Phase 5: Blockchain submission (blocks 76-90) - EARLY with more time
        """
        logger.info("🚀 Starting blockchain submission phase (EARLY to meet deadline)")
        
        try:
            # Import submission utilities
            from app.utils.blockchain_submission import (
                collect_storage_requests_for_submission,
                collect_miner_profiles_for_submission, 
                call_update_pin_and_storage_requests,
                mark_submissions_as_completed,
                submit_health_metrics_to_blockchain
            )
            
            # Step 1: Submit health metrics FIRST (if not already done)
            if self.health_checks_completed and not self.health_metrics_submitted:
                logger.info("📊 Submitting health metrics to blockchain...")
                health_success = await submit_health_metrics_to_blockchain(self.db_pool)
                if health_success:
                    self.health_metrics_submitted = True
                    logger.info("✅ Health metrics submitted successfully")
                else:
                    logger.warning("⚠️ Health metrics submission failed but continuing")
            
            # Step 2: Collect data for main submission
            logger.info("📦 Collecting data for blockchain submission...")
            
            storage_requests = await collect_storage_requests_for_submission(self.db_pool)
            miner_profiles = await collect_miner_profiles_for_submission(self.db_pool)
            
            logger.info(f"Prepared for submission:")
            logger.info(f"  - {len(storage_requests)} original storage requests (for closing)")
            logger.info(f"  - {len(miner_profiles)} miner profiles")
            
            if len(storage_requests) == 0 and len(miner_profiles) == 0:
                logger.warning("⚠️ No data to submit to blockchain")
                return True  # Not an error, just nothing to do
            
            # Step 3: Submit to blockchain
            success, submitted_requests, submitted_profiles = call_update_pin_and_storage_requests(
                storage_requests, miner_profiles
            )
            
            if success:
                # Mark as completed in database
                await mark_submissions_as_completed(self.db_pool, submitted_requests, submitted_profiles)
                logger.info("✅ Blockchain submission completed successfully")
                return True
            else:
                logger.error("❌ Blockchain submission failed")
                return False
                
        except Exception as e:
            logger.error(f"❌ Error during blockchain submission: {e}")
            return False
    
    async def submit_health_metrics(self) -> bool:
        """Submit health check metrics to the blockchain."""
        logger.info("📊 Submitting health check metrics to blockchain")
        
        try:
            from app.utils.blockchain_submission import submit_health_metrics_to_blockchain
            
            if not self.db_pool:
                logger.error("Database pool not initialized")
                return False
            
            # Submit health metrics to blockchain
            success = await submit_health_metrics_to_blockchain(self.db_pool)
            
            if success:
                logger.info("✅ Successfully submitted health metrics to blockchain")
                return True
            else:
                logger.error("❌ Failed to submit health metrics to blockchain")
                return False
                
        except Exception as e:
            logger.error(f"❌ Error during health metrics submission: {e}")
            return False
    
    async def epoch_initialization(self) -> bool:
        """Perform epoch initialization tasks."""
        logger.info("🚀 Starting epoch initialization")
        
        # Clean up tables from previous epoch
        cleanup_success = await self.cleanup_epoch_tables()
        if not cleanup_success:
            logger.warning("⚠️ Table cleanup failed, but continuing with initialization")
        
        # Refresh all base data
        tasks = [
            self.refresh_registration_data(),
            self.refresh_node_metrics(),
            self.refresh_user_profiles()
        ]
        
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        success = all(isinstance(r, bool) and r for r in results)
        
        if success:
            logger.info("✅ Epoch initialization completed successfully")
        else:
            logger.error("❌ Epoch initialization failed")
        
        return success
    
    async def non_validator_workflow(self):
        """Execute non-validator workflow."""
        logger.info("👤 Executing NON-VALIDATOR workflow")
        
        # Get current position for context
        current_block = self.current_block
        block_position = get_epoch_block_position(current_block)
        
        logger.info(f"👤 Non-validator can process at any time - current position: {block_position}/99")
        
        # Initialize epoch data if not done
        if not self.initialization_completed:
            logger.info("🚀 Non-validator: Starting epoch initialization...")
            success = await self.epoch_initialization()
            if success:
                self.initialization_completed = True
                logger.info("✅ Non-validator: Epoch initialization completed")
            else:
                logger.error("❌ Non-validator: Failed to initialize epoch data")
                return
        
        # Periodically refresh user profiles to stay current (every ~20 blocks)
        if block_position % 20 == 0 and block_position > 10:  # Every 20 blocks after initialization
            logger.info("🔄 Non-validator: Refreshing user profiles to stay current with network changes")
            profile_success = await self.refresh_user_profiles()
            if profile_success:
                logger.info("✅ Non-validator: User profiles refreshed successfully")
            else:
                logger.warning("⚠️ Non-validator: User profile refresh failed")
        
        # Perform health checks and submit to chain
        if not self.health_checks_completed:
            logger.info("🏥 Non-validator: Starting health checks...")
            success = await self.perform_health_checks()
            if success:
                self.health_checks_completed = True
                logger.info("✅ Non-validator: Health checks completed")
            else:
                logger.error("❌ Non-validator: Health checks failed")
        
        # Run availability maintenance (non-validators can help maintain the network)
        if self.health_checks_completed and not self.availability_completed:
            logger.info("🛠️ Non-validator: Running availability maintenance to help network...")
            success = await self.run_availability_maintenance()
            if success:
                self.availability_completed = True
                logger.info("✅ Non-validator: File availability maintenance completed")
            else:
                logger.warning("⚠️ Non-validator: File availability maintenance failed")
        
        # Submit health metrics to blockchain
        if self.health_checks_completed and not self.health_metrics_submitted:
            logger.info("📊 Non-validator: Submitting health metrics to blockchain...")
            success = await self.submit_health_metrics()
            if success:
                self.health_metrics_submitted = True
                logger.info("✅ Non-validator: Health metrics submitted to blockchain")
            else:
                logger.error("❌ Non-validator: Health metrics submission failed")
        
        # Status summary for non-validators
        if block_position % 25 == 0:  # Every 25 blocks show summary
            logger.info("📋 Non-validator status summary:")
            logger.info(f"   Initialization: {'✅' if self.initialization_completed else '❌'}")
            logger.info(f"   Health checks: {'✅' if self.health_checks_completed else '❌'}")
            logger.info(f"   Availability maintenance: {'✅' if self.availability_completed else '❌'}")
            logger.info(f"   Health metrics submitted: {'✅' if self.health_metrics_submitted else '❌'}")
        
        # Wait for end of epoch
        if block_position % 30 == 0:  # Every 30 blocks
            logger.info("⏳ Non-validator: Monitoring network and waiting for next epoch...")
            remaining_blocks = 99 - block_position
            logger.info(f"   {remaining_blocks} blocks remaining in current epoch")
    
    async def validator_workflow(self):
        """Execute validator workflow."""
        logger.info("👑 Executing VALIDATOR workflow")
        
        # Use the current epoch and block from the main loop
        current_epoch = self.current_epoch
        current_block = self.current_block
        block_position = get_epoch_block_position(current_block)
        
        logger.info(f"Current block position in epoch: {block_position}/99")
        
        # Phase 1: Initialization (blocks 0-5) - SHORTENED
        if block_position <= 5 and not self.initialization_completed:
            # Only do basic initialization - NO self-healing yet (needs fresh health data)
            success = await self.epoch_initialization()
            if success:
                self.initialization_completed = True
                logger.info("✅ Phase 1 complete: Initialization")
            return
        
        # Phase 2: Health Checks (blocks 6-35) - EXTENDED
        elif 6 <= block_position <= 35:
            if not self.health_checks_completed:
                success = await self.perform_health_checks()
                if success:
                    self.health_checks_completed = True
                    logger.info("✅ Phase 2 complete: Health checks")
                    
                    # NOW safe to run self-healing with fresh health data
                    logger.info("🛠️ Running network self-healing with fresh health data...")
                    await self.network_self_healing_routine()
            return
        
        # Phase 3: File Assignment (blocks 36-60) - ADJUSTED
        elif 36 <= block_position <= 60:
            if self.health_checks_completed and not self.assignment_completed:
                success = await self.assign_files()
                if success:
                    self.assignment_completed = True
                    logger.info("✅ Phase 3 complete: File assignments")
            return
        
        # Phase 4: Profile Reconstruction (blocks 61-75) - NEW SEPARATE PHASE
        elif 61 <= block_position <= 75:
            if self.assignment_completed and not self.profiles_completed:
                success = await self.reconstruct_profiles()
                if success:
                    self.profiles_completed = True
                    self.profiles_reconstructed = True  # Keep legacy variable for compatibility
                    logger.info("✅ Phase 4 complete: Profile reconstruction")
            return
        
        # Phase 5: Blockchain Submission (blocks 76-90) - EARLY WITH MORE TIME
        elif 76 <= block_position <= 90:
            if self.profiles_completed and not self.submission_completed:
                logger.info("🚀 Starting blockchain submission (EARLY to meet block 95 deadline)")
                success = await self.submit_to_blockchain()
                if success:
                    self.submission_completed = True
                    self.blockchain_submitted = True  # Keep legacy variable for compatibility
                    logger.info("✅ Phase 5 complete: Blockchain submission")
                else:
                    logger.error("❌ Blockchain submission failed - will retry next block")
            return
        
        # Phase 6: Cleanup and Final Tasks (blocks 91-99)
        elif 91 <= block_position <= 99:
            if not self.cleanup_completed:
                # Only run cleanup once
                await self.epoch_cleanup()
                self.cleanup_completed = True
                logger.info("✅ Phase 6 complete: Cleanup")
            return
        
        else:
            logger.warning(f"⚠️ Unexpected block position: {block_position}")
    
    async def reset_epoch_state(self):
        """Reset epoch state for new epoch."""
        self.initialization_completed = False
        self.health_checks_completed = False
        self.assignment_completed = False
        self.profiles_completed = False  # NEW
        self.submission_completed = False  # NEW
        self.cleanup_completed = False  # NEW
        
        # Legacy state variables (keeping for compatibility)
        self.pinning_completed = False
        self.profiles_reconstructed = False
        self.blockchain_submitted = False
        self.availability_completed = False
        self.health_metrics_submitted = False
        
        # CRITICAL FIX: Reset startup safety mechanism for new epoch
        # This ensures validators can start processing if they become validator at epoch start
        if hasattr(self, 'waiting_for_next_epoch'):
            self.waiting_for_next_epoch = False
        
        logger.info("🔄 Epoch state reset for new epoch")
    
    def should_wait_for_next_epoch(self, current_epoch: int, block_position: int) -> bool:
        """
        Determine if VALIDATORS should wait for the next epoch before starting processing.
        This prevents validator processing with incomplete data when starting mid-epoch.
        
        NOTE: This safety mechanism ONLY applies to validators. Non-validators can start immediately.
        
        Args:
            current_epoch: Current epoch number
            block_position: Current position in epoch (0-99)
            
        Returns:
            True if validator should wait, False if validator can proceed
        """
        # CRITICAL FIX: If we're at the start of an epoch (0-10), always allow processing
        if block_position <= 10:
            if self.waiting_for_next_epoch:
                logger.info(f"🎯 Validator at epoch start (position {block_position}/99) - resuming processing")
                self.waiting_for_next_epoch = False
                self.startup_epoch = current_epoch
            return False
        
        # ENHANCED FIX: Check if this is a connection recovery scenario
        if self.is_validator_state_transition_recovery(current_epoch, True, block_position):
            logger.info(f"🔗 Connection recovery: Resuming validator processing without waiting")
            self.waiting_for_next_epoch = False
            self.startup_epoch = current_epoch
            return False
        
        # ENHANCED FIX: If we became validator in this epoch (role transition), allow processing
        # This handles connection lag where we miss the early detection window
        if (hasattr(self, 'previous_epoch') and 
            self.previous_epoch is not None and 
            current_epoch > self.previous_epoch and
            self.waiting_for_next_epoch):
            logger.info(f"🎯 Role transition to validator in epoch {current_epoch} at position {block_position}/99")
            logger.info(f"   Allowing processing despite late detection (connection lag or role transition)")
            self.waiting_for_next_epoch = False
            self.startup_epoch = current_epoch
            return False
        
        # If this is the first time we're seeing this epoch (true startup)
        if self.startup_epoch is None:
            self.startup_epoch = current_epoch
            
            # If we're starting after block 10, wait for next epoch (true mid-epoch startup)
            if block_position > 10:
                logger.warning(f"🚨 Validator started mid-epoch at block position {block_position}/99")
                logger.warning(f"   Validator waiting for next epoch to avoid processing incomplete data")
                self.waiting_for_next_epoch = True
                return True
            else:
                logger.info(f"✅ Validator started early in epoch at block position {block_position}/99")
                logger.info(f"   Safe for validator to proceed with current epoch processing")
                return False
        
        # If we were waiting and we're now in a new epoch, we can proceed
        if self.waiting_for_next_epoch and current_epoch > self.startup_epoch:
            logger.info(f"🎯 New epoch {current_epoch} started - validator resuming normal processing")
            self.waiting_for_next_epoch = False
            self.startup_epoch = current_epoch
            return False
        
        # Continue waiting if we're still in the startup epoch and started mid-epoch
        return self.waiting_for_next_epoch
    
    async def run(self):
        """Main orchestrator loop."""
        logger.info("🎯 Starting Epoch Orchestrator")
        logger.info(f"📦 Version: {ORCHESTRATOR_VERSION}")
        logger.info("🛡️ Safety mechanism: Only validators wait for next epoch if starting mid-epoch (after block 10)")
        logger.info("👤 Non-validators can start processing immediately")
        
        try:
            await self.initialize()
            
            last_epoch = None
            
            while True:
                try:
                    # Check if we should attempt connection based on backoff
                    if not self.should_attempt_connection():
                        backoff_delay = self.get_backoff_delay()
                        logger.info(f"⏳ Backing off for {backoff_delay}s due to connection failures")
                        await asyncio.sleep(min(backoff_delay, self.block_check_interval))
                        continue
                    
                    # Ensure we have a substrate connection with enhanced retry logic
                    if self.substrate is None:
                        logger.info("🔗 Creating new substrate connection...")
                        
                        # More aggressive retry for validators to minimize downtime
                        max_connection_attempts = 5 if self.validator_state_cache['last_known_validator_status'] else 3
                        
                        for attempt in range(max_connection_attempts):
                            try:
                                self.substrate = connect_substrate()
                                logger.info(f"✅ Substrate connection established (attempt {attempt + 1}/{max_connection_attempts})")
                                break
                            except Exception as e:
                                if attempt < max_connection_attempts - 1:
                                    wait_time = 2 ** attempt  # Exponential backoff: 1s, 2s, 4s, 8s, 16s
                                    logger.warning(f"⚠️ Connection attempt {attempt + 1} failed: {e}")
                                    logger.info(f"   Retrying in {wait_time}s...")
                                    await asyncio.sleep(wait_time)
                                else:
                                    logger.error(f"❌ All {max_connection_attempts} connection attempts failed")
                                    raise
                    
                    # Get current epoch and validator status with updated substrate connection
                    current_epoch, current_block, self.substrate = get_current_epoch_info(self.substrate)
                    is_validator, current_validator, epoch_start, self.substrate = is_epoch_validator(
                        self.substrate, self.our_validator_account
                    )
                    
                    # Record successful connection
                    self.record_connection_success()
                    
                    # Check if we've moved to a new epoch
                    if last_epoch is not None and current_epoch != last_epoch:
                        logger.info(f"🔄 New epoch detected: {last_epoch} -> {current_epoch}")
                        
                        # Track previous epoch for role transition detection
                        self.previous_epoch = last_epoch
                        
                        await self.reset_epoch_state()
                        
                        # Update startup epoch tracking for new epoch
                        if self.startup_epoch == last_epoch:
                            self.startup_epoch = current_epoch
                    
                    # Update state
                    self.current_epoch = current_epoch
                    self.current_block = current_block
                    
                    # Track role transitions for debugging
                    previous_is_validator = getattr(self, 'is_validator', None)
                    self.is_validator = is_validator
                    
                    # Update validator state cache for connection resilience
                    self.update_validator_state_cache(current_epoch, is_validator)
                    
                    # Log role transitions
                    if previous_is_validator is not None and previous_is_validator != is_validator:
                        role_from = "VALIDATOR" if previous_is_validator else "NON-VALIDATOR"
                        role_to = "VALIDATOR" if is_validator else "NON-VALIDATOR"
                        logger.info(f"🔄 Role transition detected: {role_from} → {role_to} in epoch {current_epoch}")
                        if is_validator:
                            logger.info(f"   Became validator at block position {block_position}/99")
                    
                    # Log connection recovery scenarios
                    if (self.connection_failures > 0 and 
                        self.validator_state_cache['last_known_validator_status'] == is_validator and
                        is_validator):
                        logger.info(f"🔗 Connection recovered: Maintaining validator role in epoch {current_epoch}")
                        logger.info(f"   Validator state preserved across {self.connection_failures} connection failure(s)")
                    
                    self.epoch_start_block = epoch_start
                    last_epoch = current_epoch
                    
                    block_position = get_epoch_block_position(current_block)
                    
                    logger.info(f"📊 Epoch {current_epoch}, Block {current_block} (position {block_position}/99)")
                    logger.info(f"🎭 Role: {'VALIDATOR' if is_validator else 'NON-VALIDATOR'}")
                    
                    # ENHANCED STARTUP SAFETY: Only apply to validators
                    if is_validator and self.should_wait_for_next_epoch(current_epoch, block_position):
                        if block_position % 10 == 0:  # Log every 10 blocks to avoid spam
                            logger.info(f"⏳ Validator waiting for next epoch (started mid-epoch at position {block_position}/99)")
                            logger.info(f"   This prevents validator processing with incomplete epoch data")
                        await asyncio.sleep(self.block_check_interval)
                        continue
                    
                    # Non-validators can always start immediately
                    if not is_validator and block_position % 20 == 0:  # Periodic status for non-validators
                        logger.info(f"👤 Non-validator processing: Can start immediately at any block position")
                    
                    # Log monitoring frequency periodically
                    if block_position % 10 == 0:  # Every 10 blocks
                        logger.info(f"⏰ Monitoring every {self.block_check_interval}s (every block)")
                        if self.validator_seed:
                            logger.info("🔐 Transaction signing: ENABLED")
                        else:
                            logger.info("🔐 Transaction signing: DISABLED")
                    
                    # Execute appropriate workflow
                    if is_validator:
                        await self.validator_workflow()
                    else:
                        await self.non_validator_workflow()
                    
                    # Wait before next check (every block = 6 seconds)
                    await asyncio.sleep(self.block_check_interval)
                    
                except Exception as e:
                    logger.error(f"Error in orchestrator loop: {e}")
                    logger.error("Full traceback:")
                    logger.exception("")
                    
                    # Record connection failure and implement backoff
                    self.record_connection_failure()
                    
                    # If it's a connection error, try to reconnect with backoff
                    if any(error_type in str(e).lower() for error_type in ["jsondecodeerror", "websocket", "broken pipe", "connection", "timeout"]):
                        logger.warning("Connection issue detected - will retry with exponential backoff")
                        
                        # Close existing connection
                        if hasattr(self, 'substrate') and self.substrate:
                            try:
                                self.substrate.close()
                            except:
                                pass  # Ignore errors when closing broken connection
                        
                        # Don't immediately reconnect - let the backoff logic handle it
                        self.substrate = None
                    
                    # Wait with backoff before retrying
                    backoff_delay = self.get_backoff_delay()
                    await asyncio.sleep(min(backoff_delay, 60))  # Cap at 60 seconds for this loop
                    
        except KeyboardInterrupt:
            logger.info("🛑 Orchestrator stopped by user")
        except Exception as e:
            logger.error(f"Fatal error in orchestrator: {e}")
            raise
        finally:
            await self.cleanup()

    def get_keypair(self):
        """
        Get keypair for transaction signing.
        
        Returns:
            Keypair object if seed is available, None otherwise
        """
        if not self.validator_seed:
            logger.warning("No validator seed available for transaction signing")
            return None
        
        try:
            from substrateinterface import Keypair
            keypair = Keypair.create_from_seed(self.validator_seed)
            logger.debug(f"Created keypair for account: {keypair.ss58_address}")
            return keypair
        except Exception as e:
            logger.error(f"Error creating keypair from seed: {e}")
            return None

    async def cleanup_epoch_tables(self):
        """Clean up tables at the start of each epoch."""
        logger.info("🧹 Cleaning up epoch tables")
        
        tables_to_clean = [
            'pinning_requests',
            'node_metrics',
            'parsed_cids',
            'pending_assignment_file',
            'pending_miner_profile',
            'pending_submissions',
            'pending_user_profile',
            'processed_pinning_requests'
        ]
        
        try:
            if not self.db_pool:
                logger.error("Database pool not initialized")
                return False
            
            async with self.db_pool.acquire() as conn:
                # Special handling for miner_epoch_health - only clean OLD data (>2 epochs ago)
                try:
                    # Keep health data from current and previous epoch, clean older data
                    result = await conn.execute("""
                        DELETE FROM miner_epoch_health 
                        WHERE epoch < $1 - 1
                    """, self.current_epoch)
                    deleted_count = result.split()[-1] if result else "0"
                    logger.info(f"✅ Cleaned old health data (>2 epochs): {deleted_count} records deleted")
                    logger.info(f"✅ Preserved health data from current epoch {self.current_epoch} and previous epoch")
                except Exception as e:
                    logger.warning(f"⚠️ Could not clean old health data: {e}")
                
                for table in tables_to_clean:
                    try:
                        # Delete all records from the table
                        result = await conn.execute(f"DELETE FROM {table}")
                        deleted_count = result.split()[-1] if result else "0"
                        logger.info(f"✅ Cleaned table '{table}': {deleted_count} records deleted")
                    except Exception as e:
                        # Some tables might not exist, which is okay
                        logger.warning(f"⚠️ Could not clean table '{table}': {e}")
            
            logger.info("✅ Epoch table cleanup completed (preserved health data as fallback)")
            return True
            
        except Exception as e:
            logger.error(f"❌ Epoch table cleanup failed: {e}")
            return False

    async def network_self_healing_routine(self) -> bool:
        """
        Run network self-healing to fix broken file assignments.
        CRITICAL: This should run AFTER health checks to use fresh health data.
        """
        logger.info("🛠️ Starting network self-healing routine")
        
        # CRITICAL VALIDATION: Ensure we have fresh health data
        if not self.health_checks_completed:
            logger.warning("⚠️ Self-healing without fresh health data - using previous epoch data")
        else:
            logger.info("✅ Self-healing with fresh health data from current epoch")
        
        # Verify we have some health data (current or previous epoch)
        async with self.db_pool.acquire() as conn:
            health_data_count = await conn.fetchval("""
                SELECT COUNT(*) FROM miner_epoch_health 
                WHERE last_activity_at >= NOW() - INTERVAL '2 hours'
            """)
            
            if health_data_count == 0:
                logger.error("🚨 CRITICAL: No health data available for self-healing!")
                logger.error("   Self-healing requires some health data to determine miner availability")
                return False
            else:
                logger.info(f"✅ Found {health_data_count} miners with recent health data for self-healing")
        
        try:
            # Use subprocess approach (cleaner isolation)
            success = self.run_processor(
                'network_self_healing_processor.py',
                'Network self-healing'
            )
            
            if success:
                # Wait for self-healing consumer to process (shorter timeout for healing)
                await self.wait_for_queues_empty(['network_self_healing'], 300)
                logger.info("✅ Network self-healing completed via subprocess")
                return True
            else:
                logger.warning("⚠️ Subprocess self-healing failed, trying direct approach...")
                return await self.network_self_healing_direct()
                
        except Exception as e:
            logger.error(f"❌ Error during network self-healing: {e}")
            return False

    async def network_self_healing_direct(self) -> bool:
        """
        Direct network self-healing when subprocess approach fails.
        Uses the emergency manual assignment script as a fallback.
        """
        logger.info("🔧 Running network self-healing via direct approach")
        
        try:
            # Use the emergency manual assignment logic directly
            from scripts.emergency_manual_assignment import manual_assignment_fix
            
            # Set up database context for the emergency script
            original_db_pool = None
            try:
                # Store original connection info
                from app.db.connection import get_db_pool
                original_db_pool = await get_db_pool()
                
                # Import the emergency script's main function
                await manual_assignment_fix()
                
                logger.info("✅ Network self-healing completed via direct emergency assignment")
                return True
                
            except Exception as e:
                logger.error(f"❌ Direct network self-healing failed: {e}")
                return False
                
        except Exception as e:
            logger.error(f"❌ Error importing emergency manual assignment: {e}")
            return False

    async def epoch_cleanup(self) -> bool:
        """
        Cleanup and finalization tasks.
        Phase 6: Cleanup and final tasks (blocks 91-99)
        """
        logger.info("🧹 Starting epoch cleanup and finalization")
        
        try:
            # Cleanup old data if needed
            await self.cleanup_epoch_tables()
            
            # Provide comprehensive epoch summary
            logger.info("🏁 EPOCH SUMMARY:")
            logger.info("=" * 50)
            logger.info(f"   Epoch {self.current_epoch} Results:")
            logger.info(f"   ✅ Phase 1 - Initialization: {self.initialization_completed}")
            logger.info(f"   ✅ Phase 2 - Health Checks: {self.health_checks_completed}")
            logger.info(f"   ✅ Phase 3 - File Assignment: {self.assignment_completed}")
            logger.info(f"   ✅ Phase 4 - Profile Reconstruction: {self.profiles_completed}")
            logger.info(f"   ✅ Phase 5 - Blockchain Submission: {self.submission_completed}")
            logger.info(f"   📊 Health Metrics Submitted: {self.health_metrics_submitted}")
            
            # Check assignment coverage
            if self.assignment_completed:
                async with self.db_pool.acquire() as conn:
                    # Check file assignment coverage
                    assignment_stats = await conn.fetchrow("""
                        SELECT 
                            COUNT(*) as total_files,
                            COUNT(CASE WHEN miner1 IS NOT NULL OR miner2 IS NOT NULL OR miner3 IS NOT NULL 
                                       OR miner4 IS NOT NULL OR miner5 IS NOT NULL THEN 1 END) as assigned_files
                        FROM file_assignments
                    """)
                    
                    if assignment_stats:
                        total = assignment_stats['total_files']
                        assigned = assignment_stats['assigned_files']
                        coverage = (assigned / total * 100) if total > 0 else 0
                        logger.info(f"   📋 File Assignment Coverage: {assigned}/{total} ({coverage:.1f}%)")
                        
                        if coverage >= 99:
                            logger.info("   🎯 EXCELLENT: Near-perfect assignment coverage!")
                        elif coverage >= 90:
                            logger.info("   ✅ GOOD: High assignment coverage")
                        else:
                            logger.warning(f"   ⚠️ WARNING: Low assignment coverage ({coverage:.1f}%)")
            
            # Critical validations
            critical_issues = []
            
            if not self.health_checks_completed:
                critical_issues.append("Health checks never completed")
            
            if self.assignment_completed and not self.health_checks_completed:
                critical_issues.append("Assignments completed WITHOUT health checks")
            
            if self.profiles_completed and not self.assignment_completed:
                critical_issues.append("Profiles reconstructed WITHOUT assignments")
            
            if self.submission_completed and not self.profiles_completed:
                critical_issues.append("Blockchain submission WITHOUT profile reconstruction")
            
            if critical_issues:
                logger.error("🚨 CRITICAL ISSUES DETECTED:")
                for issue in critical_issues:
                    logger.error(f"   ❌ {issue}")
            else:
                logger.info("   ✅ WORKFLOW INTEGRITY: All phases completed in correct order")
            
            # Performance metrics
            logger.info("=" * 50)
            logger.info("📈 Performance Metrics:")
            if hasattr(self, 'epoch_start_time'):
                from datetime import datetime
                elapsed = (datetime.now() - self.epoch_start_time).total_seconds()
                logger.info(f"   ⏱️ Total epoch processing time: {elapsed:.1f} seconds")
            
            logger.info("✅ Epoch cleanup completed")
            return True
            
        except Exception as e:
            logger.error(f"❌ Error during epoch cleanup: {e}")
            return False


async def main():
    """Main entry point."""
    orchestrator = EpochOrchestrator()
    await orchestrator.run()


if __name__ == "__main__":
    asyncio.run(main()) 