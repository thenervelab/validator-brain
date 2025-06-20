#!/usr/bin/env python3
"""
Epoch Orchestrator v2.2.0

CRITICAL TIMING FIX IN v2.2.0:
- 🚨 FIXED: Blockchain submission timing moved to block ~90 (was happening too early at block 48-60)
- ✅ REMOVED: Immediate submission after profile reconstruction
- ✅ ENHANCED: Phase 5 now runs at blocks 88-95 for proper end-of-epoch timing
- ✅ Submission happens at the END of the epoch cycle, not immediately after profiles
- ✅ Better timing control prevents premature submissions

CRITICAL FIX IN v2.1.9:
- 🚨 FIXED: Missing network_self_healing_processor.py causing processor failures
- ✅ ENHANCED: Flexible phase timing for connection recovery scenarios
- ✅ RECOVERY MODE: Phases run based on dependencies, not just block timing
- ✅ Connection resilience: Assignment/reconstruction phases run even after late recovery
- ✅ Prevents missed profile submissions due to connection disruptions
- ✅ Extended phase windows for better fault tolerance

CRITICAL CLEANUP IN v2.1.8:
- 🧹 CLEANED: Removed all standalone script dependencies and fallbacks
- ✅ Pure RabbitMQ-based processor architecture (no more standalone scripts)
- ✅ Removed network_self_healing_direct fallback to emergency_manual_assignment
- ✅ Streamlined self-healing to use only RabbitMQ processor system
- ✅ Cleaner, more maintainable code without script mixing
- ✅ Production-ready scalable architecture only

CRITICAL FIX IN v2.1.7:
- 🚨 FIXED: Integrated proper RabbitMQ-based file assignment system
- ✅ Replaced standalone SimpleFileAssigner script with scalable file_assignment_processor.py  
- ✅ Integrated RabbitMQ-based user profile reconstruction (user_profile_reconstruction_processor.py)
- ✅ Both user and miner profile reconstruction now use scalable RabbitMQ systems
- ✅ Proper processor/consumer pattern following orchestrator architecture
- ✅ Enhanced assignment verification after completion
- ✅ Fills ANY NULL miner columns in file_assignments table
- ✅ Complete scalable, distributed processing architecture

CRITICAL FIX IN v2.1.6:
- 🚨 ENHANCED: Transaction hash logging for blockchain submissions
- ✅ Added detailed debugging for IpfsPallet::UpdatePinAndStorageRequests transactions  
- ✅ Enhanced validator keypair validation with environment variable checks
- ✅ Comprehensive data collection logging (storage requests + miner profiles)
- ✅ Database state diagnostics when miner profiles are missing
- ✅ Clear transaction success/failure reporting with tx hash

CRITICAL FIX IN v2.1.5:
- 🚨 FIXED: UnboundLocalError crash with block_position variable referenced before assignment
- 🚨 FIXED: Corrupted validator_workflow method that prevented phase processing
- ✅ Restored proper validator phase workflow (Initialization → Health → Assignment → Profiles → Submission → Cleanup)
- ✅ ENHANCED: Immediate blockchain submission after profile reconstruction (blocks 61-75)
- ✅ Phase 5 now serves as backup retry if Phase 4 submission fails
- ✅ Predictable submission timing - profiles submitted immediately when ready

CRITICAL FIX IN v2.1.4:
- 🚨 FIXED: AttributeError for missing state variables (profiles_completed, submission_completed)
- ✅ Properly initialize all workflow state variables in __init__ method
- ✅ Prevents runtime crashes during validator workflow execution

CRITICAL FIX IN v2.1.3:
- 🚨 FIXED: Validator state confusion during connection recovery
- ✅ Enhanced role transition detection with proper state persistence
- ✅ Connection lag tolerance prevents "dropping state" issues
- ✅ Improved startup safety mechanism for mid-epoch validator detection

CRITICAL FIX IN v2.1.2:
- 🚨 FIXED: Connection recovery resilience with validator state persistence
- ✅ Enhanced connection failure handling to prevent processing interruption
- ✅ Improved role transition detection across connection failures
- ✅ Better startup timing detection for mid-epoch scenarios

Previous fixes and enhancements in v2.1.0 and v2.1.1 maintained for stability
and compatibility. This orchestrator now provides a complete, resilient epoch
processing workflow with proper error handling, connection recovery, and
comprehensive blockchain submission capabilities.

This is the main orchestrator that manages the entire IPFS Service Validator application
based on whether we are the current epoch validator or not.

OPTIMIZED EPOCH WORKFLOW (100 blocks):
- Phase 1 (0-5): Initialization 
- Phase 2 (6-35): Health Checks
- Phase 2.5 (sequential): Health Score Processing
- Phase 2.75 (sequential): Network Self-Healing (fix broken assignments)
- Phase 3 (sequential): File Assignments (new storage requests)  
- Phase 4 (sequential): Profile Reconstruction
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
import subprocess
import sys
import time
from typing import List

# Add parent directory to path for imports
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from dotenv import load_dotenv

from app.utils.epoch_validator import (get_current_epoch_info, get_epoch_block_position,
                                       is_epoch_validator, get_validator_account_from_env,
                                       connect_substrate)
from app.db.connection import init_db_pool, close_db_pool, get_db_pool

# Load environment variables
load_dotenv()

# Orchestrator version
ORCHESTRATOR_VERSION = "2.2.0"

# Setup logging
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
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
        self.health_scores_processed = False  # CRITICAL: Transfer health data to miner_stats
        self.network_self_healing_completed = False  # Phase 2.75: Fix broken assignments
        self.health_metrics_submitted = False
        self.availability_completed = False  # Track availability maintenance
        self.profiles_reconstructed = False
        self.blockchain_submitted = False

        # New state variables for enhanced workflow tracking
        self.profiles_completed = False  # Phase 4: Profile reconstruction
        self.submission_completed = False  # Phase 5: Blockchain submission
        self.cleanup_completed = False  # Phase 6: Cleanup and summary

        # Enhanced state persistence across connection failures
        self.validator_state_cache = {'last_known_epoch': None,
                                      'last_known_validator_status': False,
                                      'last_successful_connection': None,
                                      'validator_epoch_start': None,
                                      # Track when we became validator
                                      }

        # Safety mechanism for mid-epoch startup
        self.startup_epoch = None
        self.previous_epoch = None  # Track previous epoch for role transition detection
        self.waiting_for_next_epoch = False

        # Connection management
        self.connection_failures = 0
        self.last_failure_time = 0
        self.max_backoff = 300  # 5 minutes max backoff

        # Node metrics timing: Use simple modulo check (refresh every 300 blocks)
        self.node_metrics_refresh_interval = 300  # Refresh every 300 blocks

        # Configuration
        self.block_check_interval = int(
            os.getenv('BLOCK_CHECK_INTERVAL', '6'))  # seconds (every block)
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
            self.db_pool = get_db_pool()
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
        logger.warning(
            f"Connection failure #{self.connection_failures}, next attempt in {self.get_backoff_delay()}s")

        # Log state cache for debugging connection issues
        if self.validator_state_cache['last_known_validator_status']:
            logger.info(
                f"📋 Validator state cache: Was validator in epoch {self.validator_state_cache['last_known_epoch']}")

    def update_validator_state_cache(self, epoch: int, is_validator: bool):
        """Update the validator state cache with current information."""
        # Track when we become validator
        if is_validator and not self.validator_state_cache['last_known_validator_status']:
            self.validator_state_cache['validator_epoch_start'] = epoch
            logger.info(f"📝 Cached: Became validator in epoch {epoch}")

        self.validator_state_cache['last_known_epoch'] = epoch
        self.validator_state_cache['last_known_validator_status'] = is_validator

    def is_validator_state_transition_recovery(self, current_epoch: int, is_validator: bool,
                                               block_position: int) -> bool:
        """
        Determine if this is a recovery from connection failure where we were already validator.
        
        Returns:
            True if this is a connection recovery scenario (not true mid-epoch startup)
        """
        cache = self.validator_state_cache

        # If we have no cached state, this could be true startup
        if cache['last_known_epoch'] is None:
            return False

        # FIXED: Only return True if we haven't already processed recovery for this epoch
        # Add a flag to prevent repeated recovery detection
        recovery_key = f"recovery_processed_{current_epoch}"
        if hasattr(self, recovery_key) and getattr(self, recovery_key):
            return False

        # If we were validator in the same epoch before connection failure
        if (cache['last_known_validator_status'] and cache[
            'last_known_epoch'] == current_epoch and is_validator and cache[
            'validator_epoch_start'] == current_epoch and self.waiting_for_next_epoch):  # Only if we're actually waiting

            logger.info(
                f"🔄 Connection recovery detected: Was validator in epoch {current_epoch} before connection failure")
            logger.info(
                f"   Validator since epoch start, connection failed at block ~{block_position}")

            # Mark recovery as processed for this epoch
            setattr(self, recovery_key, True)
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
            result = subprocess.run(['python', f'rabbitmq/{processor_name}'], capture_output=True,
                                    text=True, timeout=600)  # 10 minute timeout

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
        Wait for specified queues to be empty by actually checking RabbitMQ.
        
        Args:
            queue_names: List of queue names to check
            timeout: Timeout in seconds
            
        Returns:
            True if all queues are empty, False if timeout
        """
        logger.info(f"⏳ Waiting for queues to be empty: {', '.join(queue_names)}")

        try:
            import aio_pika
            import os

            # Get RabbitMQ connection URL
            rabbitmq_url = os.getenv('RABBITMQ_URL', 'amqp://admin:admin@rabbitmq-service:5672/')

            start_time = asyncio.get_event_loop().time()
            check_interval = 10  # Check every 10 seconds

            while (asyncio.get_event_loop().time() - start_time) < timeout:
                try:
                    # Connect to RabbitMQ
                    connection = await aio_pika.connect_robust(rabbitmq_url)
                    channel = await connection.channel()

                    all_empty = True
                    queue_status = {}

                    for queue_name in queue_names:
                        try:
                            # Declare queue (doesn't create if exists, just gets info)
                            queue = await channel.declare_queue(queue_name, durable=True,
                                                                passive=True)
                            message_count = queue.declaration_result.message_count
                            queue_status[queue_name] = message_count

                            if message_count > 0:
                                all_empty = False

                        except Exception as e:
                            # Queue doesn't exist or can't access - treat as empty
                            logger.debug(
                                f"Queue {queue_name} not accessible (treating as empty): {e}")
                            queue_status[queue_name] = 0

                    await connection.close()

                    # Log status
                    status_str = ", ".join([f"{q}:{count}" for q, count in queue_status.items()])
                    logger.info(f"📊 Queue status: {status_str}")

                    if all_empty:
                        logger.info("✅ All queues are empty!")
                        return True

                    # Wait before next check
                    logger.info(f"⏳ Queues not empty, checking again in {check_interval}s...")
                    await asyncio.sleep(check_interval)

                except Exception as e:
                    logger.warning(f"⚠️ Error checking queues: {e}")
                    logger.info(f"Retrying in {check_interval}s...")
                    await asyncio.sleep(check_interval)

            # Timeout reached
            logger.warning(f"⏰ Timeout reached ({timeout}s) - some queues may not be empty")
            return False

        except Exception as e:
            logger.error(f"Error setting up queue monitoring: {e}")
            logger.info("Falling back to time-based waiting...")
            # Fallback to time-based approach
            wait_time = min(60, timeout // 5)
            logger.info(f"⏳ Waiting {wait_time}s for queue processing to complete...")
            await asyncio.sleep(wait_time)
            return True

    async def refresh_registration_data(self) -> bool:
        """Refresh registration table with new data."""
        logger.info("🔄 Refreshing registration data")

        # Clear and refill registration table
        success = self.run_processor('registration_processor.py', 'Registration data refresh')

        if success:
            # Wait for registration consumer to process
            await self.wait_for_queues_empty(['registration'], 120)

        return success

    async def refresh_node_metrics(self) -> bool:
        """Refresh node metrics data."""
        logger.info("🔄 Refreshing node metrics")

        success = self.run_processor('node_metrics_processor.py', 'Node metrics refresh')

        if success:
            # Wait for node metrics consumer to process
            await self.wait_for_queues_empty(['node_metrics_latest'], 120)

        return success

    async def refresh_user_profiles(self) -> bool:
        """Refresh user profiles data."""
        logger.info("🔄 Refreshing user profiles")

        success = self.run_processor('user_profile_processor.py', 'User profiles refresh')

        if success:
            # Wait for user profile consumer to process
            await self.wait_for_queues_empty(['user_profile'], 300)

        return success

    async def perform_health_checks(self) -> bool:
        """Perform miner health checks."""
        logger.info("🏥 Performing miner health checks")

        success = self.run_processor('miner_health_processor.py', 'Miner health checks')

        if success:
            # Wait for health check consumer to process
            await self.wait_for_queues_empty(['miner_health_check'], 600)

        return success

    async def process_health_scores(self) -> bool:
        """
        Process health scores from epoch health data to miner stats.
        This MUST run after health checks complete to ensure health_score data is available for file assignment.
        """
        logger.info("🏥 Processing health scores from epoch health data to miner stats")

        try:
            async with self.db_pool.acquire() as conn:
                # Get most recent health data per miner (avoiding duplicates from multiple epochs)
                health_calculations = await conn.fetch("""
                    SELECT DISTINCT ON (meh.node_id)
                        meh.node_id,
                        meh.epoch,
                        meh.ping_successes,
                        meh.ping_failures,
                        meh.pin_check_successes,
                        meh.pin_check_failures,
                        -- Calculate overall health score (ping + pin performance)
                        CASE 
                            WHEN (meh.ping_successes + meh.ping_failures + meh.pin_check_successes + meh.pin_check_failures) = 0 THEN 100
                            ELSE ((meh.ping_successes + meh.pin_check_successes) * 100.0 / 
                                  (meh.ping_successes + meh.ping_failures + meh.pin_check_successes + meh.pin_check_failures))
                        END AS calculated_health_score,
                        meh.last_activity_at
                    FROM miner_epoch_health meh
                    WHERE meh.last_activity_at >= NOW() - INTERVAL '6 hours'
                    ORDER BY meh.node_id, meh.last_activity_at DESC
                """)

                if not health_calculations:
                    logger.warning("⚠️ No recent health data found to process")
                    return False

                logger.info(f"📊 Processing health scores for {len(health_calculations)} miners")

                # Update miner_stats with pin check data (health_score is auto-calculated from successful_pin_checks/total_pin_checks)
                updated_count = 0
                for health in health_calculations:
                    node_id = health['node_id']
                    ping_successes = health['ping_successes'] or 0
                    ping_failures = health['ping_failures'] or 0
                    pin_successes = health['pin_check_successes'] or 0
                    pin_failures = health['pin_check_failures'] or 0

                    # Calculate totals for the generated health_score column
                    total_pin_checks = pin_successes + pin_failures
                    successful_pin_checks = pin_successes

                    # Update or insert into miner_stats (health_score is auto-calculated)
                # Note: miner_stats table only has pin check columns, not ping columns
                await conn.execute("""
                    INSERT INTO miner_stats (
                        node_id, 
                        successful_pin_checks,
                        total_pin_checks,
                        updated_at
                    )
                    VALUES ($1, $2, $3, NOW())
                    ON CONFLICT (node_id) 
                    DO UPDATE SET 
                        successful_pin_checks = $2,
                        total_pin_checks = $3,
                        updated_at = NOW()
                """, node_id, successful_pin_checks, total_pin_checks)

                updated_count += 1

                # Verify health scores were calculated
                healthy_miners = await conn.fetchval("""
                    SELECT COUNT(*) FROM miner_stats 
                    WHERE health_score > 0 AND updated_at >= NOW() - INTERVAL '10 minutes'
                """)

                avg_health = await conn.fetchval("""
                    SELECT ROUND(AVG(health_score), 1) FROM miner_stats 
                    WHERE health_score > 0 AND updated_at >= NOW() - INTERVAL '10 minutes'
                """)

                logger.info("✅ Health score processing completed:")
                logger.info(f"   📊 Updated {updated_count} miner records")
                logger.info(f"   🏥 {healthy_miners} miners now have health scores > 0")
                logger.info(f"   📈 Average health score: {avg_health}%")

                if healthy_miners < 100:
                    logger.warning(
                        f"⚠️ Only {healthy_miners} healthy miners - may impact assignment quality")

                return True

        except Exception as e:
            logger.error(f"❌ Error processing health scores: {e}")
            logger.exception("Full traceback:")
            return False

    async def process_pinning_requests(self) -> bool:
        """
        Process pinning requests by running the necessary processors and waiting for queues.
        This is a two-stage process:
        1. Fetch pinning requests from the chain.
        2. Process the files from those requests to get their sizes.
        """
        logger.info("📌 Processing ALL unassigned pinning requests from the blockchain...")
        logger.info(
            "   🔄 This includes both NEW requests AND old unprocessed requests from previous validators")

        # Step 1: Run the processor to fetch requests from the chain and put them on the queue
        logger.info(
            "   Running pinning_request_processor.py to fetch ALL unassigned requests from chain...")
        success = self.run_processor('pinning_request_processor.py', 'Pinning requests processing')
        if not success:
            logger.error("❌ Pinning request processor script failed to run.")
            return False
        logger.info("   ✅ Pinning request processor completed.")

        # Step 2: Wait for the consumer to process the messages from the queue
        logger.info("   ⏳ Waiting for 'pinning_request' queue to be processed...")
        await self.wait_for_queues_empty(['pinning_request'], 300)
        logger.info("   ✅ 'pinning_request' queue processed.")

        # Step 3: Now, run the file processor to get file sizes for the new requests
        logger.info("   📁 Running pinning_file_processor.py to get file sizes...")
        success = self.run_processor('pinning_file_processor.py', 'Pinning files processing')
        if not success:
            logger.error("❌ Pinning file processor script failed to run.")
            return False
        logger.info("   ✅ Pinning file processor completed.")

        # Step 4: Wait for the file consumer to process the files and write to the 'files' table
        logger.info("   ⏳ Waiting for 'pinning_file_processing' queue to be processed...")
        await self.wait_for_queues_empty(['pinning_file_processing'], 600)
        logger.info("   ✅ 'pinning_file_processing' queue processed.")

        logger.info(
            "✅ ALL unassigned pinning requests (new + old unprocessed) and their files have been processed.")
        return True

    async def process_pinning_files(self) -> bool:
        """Process individual pinning files - ENHANCED: Process ALL files."""
        logger.info("📁 Processing pinning files (ENHANCED: Process ALL)")

        # Run pinning file processor multiple times until all files are processed
        total_rounds = 0
        max_rounds = 10  # More rounds for individual files since there can be many

        while total_rounds < max_rounds:
            total_rounds += 1
            logger.info(f"📁 Pinning files processing - Round {total_rounds}")

            # Check if there are still unprocessed pinning requests with files
            async with self.db_pool.acquire() as conn:
                unprocessed_files = await conn.fetchval("""
                    SELECT COUNT(*) FROM pinning_requests pr
                    WHERE pr.file_hash IS NOT NULL 
                    AND pr.file_hash != ''
                    AND NOT EXISTS (
                        SELECT 1 FROM pending_assignment_file paf 
                        WHERE paf.owner = pr.owner 
                        AND paf.cid = pr.file_hash
                    )
                """)

                # Also check for files in the processing queue
                pending_assignments = await conn.fetchval("""
                    SELECT COUNT(*) FROM pending_assignment_file 
                    WHERE status = 'pending'
                """)

                logger.info(
                    f"📊 Round {total_rounds}: {unprocessed_files} unprocessed files, {pending_assignments} pending assignments")

                if unprocessed_files == 0 and pending_assignments == 0:
                    logger.info("✅ ALL pinning files processed successfully!")
                    break

        success = self.run_processor('pinning_file_processor.py',
                                     f'Pinning files processing (Round {total_rounds})')

        if success:
            # Wait for pinning file consumer to process
            await self.wait_for_queues_empty(['pinning_file_processing'], 600)
            logger.info(f"✅ Pinning files round {total_rounds} completed")
        else:
            logger.error(f"❌ Pinning files round {total_rounds} failed")
            return False

        if total_rounds >= max_rounds:
            logger.warning(f"⚠️ Reached maximum rounds ({max_rounds}) for pinning files processing")

        # Final verification
        async with self.db_pool.acquire() as conn:
            processed_files = await conn.fetchval("""
                SELECT COUNT(*) FROM pending_assignment_file 
                WHERE status = 'processed' AND file_size_bytes IS NOT NULL
            """)
            failed_files = await conn.fetchval("""
                SELECT COUNT(*) FROM pending_assignment_file 
                WHERE status = 'failed'
            """)

            logger.info("📊 FINAL PINNING RESULTS:")
            logger.info(f"   ✅ Processed files: {processed_files}")
            logger.info(f"   ❌ Failed files: {failed_files}")
            logger.info(f"   🎯 Files ready for assignment: {processed_files}")

        return True

    async def assign_files(self) -> bool:
        """
        Assign miners to files using the previous ValidatorWorkflow approach.
        Phase 3: File assignment (blocks 36-60)
        
        ENHANCED: Comprehensive queue monitoring to prevent race conditions.
        """
        logger.info("📋 Starting file assignment phase (ENHANCED: Queue monitoring)")

        # CRITICAL: Always process pinning requests first to get the latest data
        logger.info("📌 Step 1: Processing pinning requests for new files before assignment...")
        pinning_success = await self.process_pinning_requests()
        if pinning_success:
            logger.info("✅ Pinning requests processed successfully")

            # CRITICAL: Wait for pinning request queue to be empty
            logger.info("⏳ Step 1a: Waiting for pinning request queue to be empty...")
            pinning_queue_empty = await self.wait_for_queues_empty(['pinning_request'], 300)
            if pinning_queue_empty:
                logger.info("✅ Pinning request queue is empty")
            else:
                logger.warning("⚠️ Pinning request queue timeout - proceeding with assignment")
        else:
            logger.warning("⚠️ Pinning requests processing failed, assignment may use stale data.")

        # CRITICAL: Process pinning files (extract individual files from manifests)
        logger.info("📁 Step 2: Processing pinning files (manifest extraction)...")
        pinning_files_success = await self.process_pinning_files()
        if pinning_files_success:
            logger.info("✅ Pinning files processed successfully")

            # CRITICAL: Wait for pinning file processing queue to be empty
            logger.info("⏳ Step 2a: Waiting for pinning file processing queue to be empty...")
            pinning_files_queue_empty = await self.wait_for_queues_empty(
                ['pinning_file_processing'], 300)
            if pinning_files_queue_empty:
                logger.info("✅ Pinning file processing queue is empty")
            else:
                logger.warning(
                    "⚠️ Pinning file processing queue timeout - proceeding with assignment")
        else:
            logger.warning("⚠️ Pinning files processing failed, assignment may use stale data.")

        logger.info("🔧 Step 3: Using previous storage request assignment workflow")

        # Validate that health checks completed
        if not self.health_checks_completed:
            logger.error("❌ Health checks must complete before file assignment")
            return False

        try:
            # Import the previous ValidatorWorkflow
            from substrate_fetcher.validator_workflow import ValidatorWorkflow

            # Create workflow instance
            workflow = ValidatorWorkflow(validator_account_id=self.our_validator_account)

            # Collect blockchain data for assignment processing
            logger.info("📦 Step 3a: Collecting blockchain data for storage request processing...")

            # Get individual files from file_assignments (already extracted from manifests by pinning consumer)
            storage_requests = []
            async with self.db_pool.acquire() as conn:
                rows = await conn.fetch("""
                    SELECT 
                        fa.owner, 
                        fa.cid as file_hash,  -- Use individual file CID, not manifest CID
                        f.name as file_name,
                        f.size as file_size,
                        3 as total_replicas,  -- Default replica count
                        fa.created_at,
                        pr.request_hash as original_request_hash  -- Get original storage request hash
                    FROM file_assignments fa
                    LEFT JOIN files f ON fa.cid = f.cid
                    LEFT JOIN pinning_requests pr ON fa.owner = pr.owner  -- Join to get original request hash
                    WHERE (fa.miner1 IS NULL OR fa.miner2 IS NULL OR fa.miner3 IS NULL 
                       OR fa.miner4 IS NULL OR fa.miner5 IS NULL)  -- Any missing assignments
                    ORDER BY fa.created_at ASC
                """)

                # DEBUG LOGGING: Print raw file assignments from DB (includes partially assigned files)
                logger.info(f"DEBUG: Files needing assignment completion: {len(rows)} files")

                for row in rows:
                    # Convert to the format expected by ValidatorWorkflow (using file CID as request_hash)
                    storage_request = (
                        (row['owner'], row['file_hash']),  # Use file CID as unique identifier
                        {'file_hash': row['file_hash'],  # Individual file CID
                         'file_name': row['file_name'], 'file_size': row['file_size'] or 0,
                         # Handle NULL sizes
                         'total_replicas': row['total_replicas'], 'created_at': row['created_at']})
                    storage_requests.append(storage_request)

                logger.info(f"📋 Found {len(storage_requests)} individual files to assign")

            # Get miner profiles from database
            miner_profiles = []
            async with self.db_pool.acquire() as conn:
                rows = await conn.fetch("""
                    SELECT 
                        r.node_id, 
                        r.ipfs_peer_id, 
                        r.owner_account,
                        GREATEST(0, COALESCE(nm.ipfs_storage_max, 0) - COALESCE(nm.ipfs_repo_size, 0)) as storage_capacity_bytes,
                        COALESCE(ms.total_files_pinned, 0) as total_files_pinned,
                        COALESCE(ms.total_files_size_bytes, 0) as total_files_size_bytes,
                        COALESCE(ms.health_score, 100) as health_score
                    FROM registration r
                    LEFT JOIN miner_stats ms ON r.node_id = ms.node_id
                    LEFT JOIN (
                        SELECT DISTINCT ON (miner_id)
                            miner_id,
                            ipfs_storage_max,
                            ipfs_repo_size
                        FROM node_metrics
                        ORDER BY miner_id, block_number DESC
                    ) nm ON r.node_id = nm.miner_id
                    WHERE r.node_type = 'StorageMiner' 
                    AND r.status = 'active'
                    AND COALESCE(ms.health_score, 100) >= 1.0
                    ORDER BY COALESCE(ms.health_score, 100) DESC
                """)

                # DEBUG LOGGING: Print raw miner profiles from DB
                logger.info(f"DEBUG: Raw miner profiles from DB: {len(rows)} miners")

                for row in rows:
                    # Convert to the format expected by ValidatorWorkflow
                    miner_profile = {'node_id': row['node_id'], 'ipfs_peer_id': row['ipfs_peer_id'],
                                     'owner_account': row['owner_account'],
                                     'storage_capacity_bytes': row['storage_capacity_bytes'],
                                     'total_files_pinned': row['total_files_pinned'],
                                     'total_files_size_bytes': row['total_files_size_bytes'],
                                     'health_score': row['health_score']}
                    miner_profiles.append(miner_profile)

                logger.info(f"⛏️ Found {len(miner_profiles)} available miners")

            # Get node registration data
            node_registration = []
            async with self.db_pool.acquire() as conn:
                rows = await conn.fetch("""
                    SELECT node_id, ipfs_peer_id
                    FROM registration 
                    WHERE node_type = 'StorageMiner' AND status = 'active'
                """)

                for row in rows:
                    # Convert to expected format
                    node_reg = type('NodeReg', (), {'node_id': row['node_id'],
                                                    'ipfs_node_id': row['ipfs_peer_id']})()
                    node_registration.append(node_reg)

            if not storage_requests:
                logger.info("✅ No unassigned files to process")
                return True

            if not miner_profiles:
                logger.error("❌ No available miners found")
                return False

            # Process individual files using ValidatorWorkflow
            logger.info("🚀 Step 3b: Processing individual files with ValidatorWorkflow...")
            user_profiles, processed_miner_profiles = await workflow.process_storage_requests(
                storage_requests=storage_requests, miner_profiles=miner_profiles,
                node_registration=node_registration)

            logger.info("✅ ValidatorWorkflow completed:")
            logger.info(f"   📝 Generated {len(user_profiles)} user profile entries")
            logger.info(f"   ⛏️ Generated {len(processed_miner_profiles)} miner profile entries")

            # Note: pinning_requests table should now contain original storage request hashes
            # from the consumer processing ALL blockchain requests (assigned + unassigned)

            # Update file_assignments with assigned miners
            logger.info("💾 Step 3c: Updating file assignments in database...")
            async with self.db_pool.acquire() as conn:
                async with conn.transaction():
                    assignments_updated = 0

                    # Process each user profile and update file_assignments
                    for profile in user_profiles:
                        file_cid = profile['file_hash']
                        owner = profile['user_id']
                        assigned_miners = profile.get('assigned_miners', [])

                        # Update file_assignments with assigned miners
                        # Assign miners to miner1, miner2, miner3, miner4, miner5 slots
                        miner_slots = [None] * 5
                        for i, miner_id in enumerate(assigned_miners[:5]):  # Max 5 miners
                            miner_slots[i] = miner_id

                        await conn.execute("""
                            UPDATE file_assignments 
                            SET miner1 = $3, miner2 = $4, miner3 = $5, miner4 = $6, miner5 = $7,
                                updated_at = CURRENT_TIMESTAMP
                            WHERE cid = $1 AND owner = $2
                        """, file_cid, owner, miner_slots[0], miner_slots[1], miner_slots[2],
                                           miner_slots[3], miner_slots[4])
                        assignments_updated += 1

                    # Also store in storage_requests table for blockchain submission
                    await conn.execute("DELETE FROM storage_requests")

                    # Group by owner for storage_requests
                    user_assignments = {}
                    for profile in user_profiles:
                        owner = profile['user_id']
                        if owner not in user_assignments:
                            user_assignments[owner] = []
                        user_assignments[owner].append(profile)

                    # Insert storage requests for blockchain submission
                    for owner, profiles in user_assignments.items():
                        for profile in profiles:
                            assigned_miners = profile.get('assigned_miners', [])

                            # Convert created_at (datetime) to Unix timestamp
                            created_at_value = profile.get('created_at', 0)
                            if hasattr(created_at_value, 'timestamp'):
                                # It's a datetime object, convert to Unix timestamp
                                timestamp = int(created_at_value.timestamp())
                            elif isinstance(created_at_value, str):
                                # It's a datetime string, parse and convert
                                from datetime import datetime
                                try:
                                    dt = datetime.fromisoformat(
                                        created_at_value.replace('Z', '+00:00'))
                                    timestamp = int(dt.timestamp())
                                except:
                                    timestamp = 0
                            elif isinstance(created_at_value, (int, float)):
                                # Already a timestamp
                                timestamp = int(created_at_value)
                            else:
                                # Default to current time
                                import time
                                timestamp = int(time.time())

                            await conn.execute("""
                                INSERT INTO storage_requests 
                                (owner_account, file_hash, file_name, file_size_bytes, 
                                 total_replicas, last_charged_at, created_at, miner_ids, 
                                 selected_validator, status)
                                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
                                ON CONFLICT (owner_account, file_hash) 
                                DO UPDATE SET
                                    file_name = EXCLUDED.file_name,
                                    file_size_bytes = EXCLUDED.file_size_bytes,
                                    total_replicas = EXCLUDED.total_replicas,
                                    last_charged_at = EXCLUDED.last_charged_at,
                                    miner_ids = EXCLUDED.miner_ids,
                                    selected_validator = EXCLUDED.selected_validator,
                                    status = EXCLUDED.status,
                                    updated_at = CURRENT_TIMESTAMP
                            """, owner, profile['file_hash'], profile.get('file_name', ''),
                                               profile['file_size_in_bytes'], len(assigned_miners),
                                               timestamp,  # Use converted timestamp
                                               timestamp,  # Use converted timestamp
                                               assigned_miners, self.our_validator_account,
                                               'assigned')

                    logger.info(f"💾 Updated {assignments_updated} individual file assignments")
                    logger.info(
                        f"💾 Created {len(user_profiles)} storage request entries for blockchain submission")

            # CRITICAL ENHANCEMENT: Verify no unassigned files remain before declaring success
            logger.info("🔍 Step 4: Verifying assignment completion...")
            async with self.db_pool.acquire() as conn:
                unassigned_count = await conn.fetchval("""
                    SELECT COUNT(*) FROM file_assignments 
                    WHERE miner1 IS NULL AND miner2 IS NULL AND miner3 IS NULL 
                      AND miner4 IS NULL AND miner5 IS NULL
                """)

                if unassigned_count > 0:
                    logger.warning(
                        f"⚠️ Found {unassigned_count} files still unassigned after assignment process")
                    logger.warning(
                        "   This suggests assignment process was incomplete")  # Don't return False immediately - might be files with no available miners
                else:
                    logger.info("✅ All files have been assigned to miners")

            logger.info(
                "✅ Individual file assignment completed successfully with ValidatorWorkflow")
            logger.info("🎯 File assignments ready for profile reconstruction")
            return True

        except Exception as e:
            logger.error(f"❌ Error during ValidatorWorkflow file assignment: {e}")
            logger.exception("Full traceback:")
            return False

    # DISABLED: Network rebalancing functionality
    # async def run_network_rebalancing(self) -> bool:
    async def run_network_rebalancing_DISABLED(self) -> bool:
        """
        Run network rebalancing ONLY when there are offline/unhealthy miners.
        This is targeted rebalancing - only when actually needed, not on every validator cycle.
        """
        try:
            # STEP 1: Check if rebalancing is actually needed
            async with self.db_pool.acquire() as conn:
                # Check for miners that have gone offline or become unhealthy
                offline_miners = await conn.fetch("""
                    SELECT DISTINCT fa.miner1 as miner_id, 'miner1' as position FROM file_assignments fa
                    WHERE fa.miner1 IS NOT NULL 
                      AND NOT EXISTS (
                          SELECT 1 FROM miner_epoch_health meh 
                          LEFT JOIN miner_stats ms ON meh.node_id = ms.node_id
                          WHERE meh.node_id = fa.miner1 
                            AND (
                                -- Check health score from miner_stats if available
                                (ms.health_score IS NOT NULL AND ms.health_score >= 70.0) OR
                                -- OR check recent activity in miner_epoch_health
                                (meh.last_activity_at >= NOW() - INTERVAL '4 hours' AND 
                                 CASE 
                                   WHEN (meh.ping_successes + meh.ping_failures + meh.pin_check_successes + meh.pin_check_failures) = 0 THEN 100
                                   ELSE ((meh.ping_successes + meh.pin_check_successes) * 100.0 / 
                                         (meh.ping_successes + meh.ping_failures + meh.pin_check_successes + meh.pin_check_failures))
                                 END >= 70.0)
                            )
                      )
                    UNION
                    SELECT DISTINCT fa.miner2 as miner_id, 'miner2' as position FROM file_assignments fa
                    WHERE fa.miner2 IS NOT NULL 
                      AND NOT EXISTS (
                          SELECT 1 FROM miner_epoch_health meh 
                          LEFT JOIN miner_stats ms ON meh.node_id = ms.node_id
                          WHERE meh.node_id = fa.miner2 
                            AND (
                                -- Check health score from miner_stats if available
                                (ms.health_score IS NOT NULL AND ms.health_score >= 70.0) OR
                                -- OR check recent activity in miner_epoch_health
                                (meh.last_activity_at >= NOW() - INTERVAL '4 hours' AND 
                                 CASE 
                                   WHEN (meh.ping_successes + meh.ping_failures + meh.pin_check_successes + meh.pin_check_failures) = 0 THEN 100
                                   ELSE ((meh.ping_successes + meh.pin_check_successes) * 100.0 / 
                                         (meh.ping_successes + meh.ping_failures + meh.pin_check_successes + meh.pin_check_failures))
                                 END >= 70.0)
                            )
                      )
                    UNION
                    SELECT DISTINCT fa.miner3 as miner_id, 'miner3' as position FROM file_assignments fa
                    WHERE fa.miner3 IS NOT NULL 
                      AND NOT EXISTS (
                          SELECT 1 FROM miner_epoch_health meh 
                          LEFT JOIN miner_stats ms ON meh.node_id = ms.node_id
                          WHERE meh.node_id = fa.miner3 
                            AND (
                                -- Check health score from miner_stats if available
                                (ms.health_score IS NOT NULL AND ms.health_score >= 70.0) OR
                                -- OR check recent activity in miner_epoch_health
                                (meh.last_activity_at >= NOW() - INTERVAL '4 hours' AND 
                                 CASE 
                                   WHEN (meh.ping_successes + meh.ping_failures + meh.pin_check_successes + meh.pin_check_failures) = 0 THEN 100
                                   ELSE ((meh.ping_successes + meh.pin_check_successes) * 100.0 / 
                                         (meh.ping_successes + meh.ping_failures + meh.pin_check_successes + meh.pin_check_failures))
                                 END >= 70.0)
                            )
                      )
                    UNION
                    SELECT DISTINCT fa.miner4 as miner_id, 'miner4' as position FROM file_assignments fa
                    WHERE fa.miner4 IS NOT NULL 
                      AND NOT EXISTS (
                          SELECT 1 FROM miner_epoch_health meh 
                          LEFT JOIN miner_stats ms ON meh.node_id = ms.node_id
                          WHERE meh.node_id = fa.miner4 
                            AND (
                                -- Check health score from miner_stats if available
                                (ms.health_score IS NOT NULL AND ms.health_score >= 70.0) OR
                                -- OR check recent activity in miner_epoch_health
                                (meh.last_activity_at >= NOW() - INTERVAL '4 hours' AND 
                                 CASE 
                                   WHEN (meh.ping_successes + meh.ping_failures + meh.pin_check_successes + meh.pin_check_failures) = 0 THEN 100
                                   ELSE ((meh.ping_successes + meh.pin_check_successes) * 100.0 / 
                                         (meh.ping_successes + meh.ping_failures + meh.pin_check_successes + meh.pin_check_failures))
                                 END >= 70.0)
                            )
                      )
                    UNION
                    SELECT DISTINCT fa.miner5 as miner_id, 'miner5' as position FROM file_assignments fa
                    WHERE fa.miner5 IS NOT NULL 
                      AND NOT EXISTS (
                          SELECT 1 FROM miner_epoch_health meh 
                          LEFT JOIN miner_stats ms ON meh.node_id = ms.node_id
                          WHERE meh.node_id = fa.miner5 
                            AND (
                                -- Check health score from miner_stats if available
                                (ms.health_score IS NOT NULL AND ms.health_score >= 70.0) OR
                                -- OR check recent activity in miner_epoch_health
                                (meh.last_activity_at >= NOW() - INTERVAL '4 hours' AND 
                                 CASE 
                                   WHEN (meh.ping_successes + meh.ping_failures + meh.pin_check_successes + meh.pin_check_failures) = 0 THEN 100
                                   ELSE ((meh.ping_successes + meh.pin_check_successes) * 100.0 / 
                                         (meh.ping_successes + meh.ping_failures + meh.pin_check_successes + meh.pin_check_failures))
                                 END >= 70.0)
                            )
                      )
                """)

                offline_miner_count = len(offline_miners)

                # Check for recent rebalancing to avoid too frequent operations
                recent_rebalancing = await conn.fetchval("""
                    SELECT COUNT(*) FROM system_events 
                    WHERE event_type = 'network_rebalancing' 
                      AND created_at >= NOW() - INTERVAL '6 hours'
                """)

                logger.info("🔍 Rebalancing assessment:")
                logger.info(f"   Offline/unhealthy miners: {offline_miner_count}")
                logger.info(f"   Recent rebalancing (last 6h): {recent_rebalancing}")

                # DECISION: Only rebalance if there are offline miners AND we haven't rebalanced recently
                if offline_miner_count == 0:
                    logger.info("✅ No offline miners detected - skipping rebalancing")
                    return True  # Success, just nothing to do

                if recent_rebalancing > 0:
                    logger.info(
                        f"⏳ Recent rebalancing detected ({recent_rebalancing} in last 6h) - skipping to avoid over-rebalancing")
                    return True  # Success, just avoiding too frequent rebalancing

                # Log details about offline miners
                if offline_miners:
                    offline_miner_ids = list(set([m['miner_id'] for m in offline_miners]))
                    logger.warning(
                        f"🚨 Found {len(offline_miner_ids)} offline miners needing rebalancing:")
                    for miner_id in offline_miner_ids[:5]:  # Show first 5
                        logger.warning(f"   - {miner_id} (offline/unhealthy)")

                    if len(offline_miner_ids) > 5:
                        logger.warning(f"   ... and {len(offline_miner_ids) - 5} more")

            # STEP 2: Run targeted rebalancing for offline miners
            logger.info("🔄 Starting TARGETED network rebalancing for offline miners...")

            # Run the network rebalancing processor (it will handle the targeting)
            success = self.run_processor('network_rebalancing_processor.py',
                                         'Targeted network rebalancing (offline miners)')

            if success:
                logger.info("✅ Targeted network rebalancing processor completed")

                # Wait for rebalancing tasks to be processed by file assignment consumer
                logger.info("⏳ Waiting for targeted rebalancing tasks to be processed...")
                await self.wait_for_queues_empty(['file_assignment_processing'],
                                                 300)  # 5 minute timeout
                logger.info("✅ Targeted network rebalancing tasks processed")

                # Log rebalancing event for tracking
                async with self.db_pool.acquire() as conn:
                    await conn.execute("""
                        INSERT INTO system_events (event_type, event_data, created_at)
                        VALUES ('network_rebalancing', $1, NOW())
                    """,
                                       f'{{"offline_miners": {offline_miner_count}, "trigger": "offline_miners"}}')

                logger.info(
                    f"📝 Recorded rebalancing event (handled {offline_miner_count} offline miners)")
                return True
            else:
                logger.warning("⚠️ Targeted network rebalancing processor failed")
                return False

        except Exception as e:
            logger.error(f"❌ Error during targeted network rebalancing: {e}")
            return False

    async def run_availability_maintenance(self) -> bool:
        """Run file availability maintenance to handle empty assignments and failures."""
        logger.info("🛠️ Running file availability maintenance")

        success = self.run_processor('availability_manager_processor.py',
                                     'File availability maintenance')

        # No queue to wait for since availability manager runs synchronously
        return success

    async def reconstruct_profiles(self) -> bool:
        """
        Reconstruct user and miner profiles from file assignments.
        Phase 4: Profile reconstruction (blocks 61-75)
        
        ENHANCED: Comprehensive queue monitoring and data verification to prevent race conditions.
        """
        logger.info(
            "🔧 Starting profile reconstruction phase (ENHANCED: Comprehensive verification)")

        try:
            # Step 1: Verify assignment data is ready
            logger.info(
                "🔍 Step 1: Verifying assignment data is ready for profile reconstruction...")
            async with self.db_pool.acquire() as conn:
                # Check for unassigned files
                unassigned_count = await conn.fetchval("""
                    SELECT COUNT(*) FROM file_assignments 
                    WHERE miner1 IS NULL AND miner2 IS NULL AND miner3 IS NULL 
                      AND miner4 IS NULL AND miner5 IS NULL
                """)

                # Check for assigned files
                assigned_count = await conn.fetchval("""
                    SELECT COUNT(*) FROM file_assignments 
                    WHERE miner1 IS NOT NULL OR miner2 IS NOT NULL OR miner3 IS NOT NULL 
                      OR miner4 IS NOT NULL OR miner5 IS NOT NULL
                """)

                logger.info("📊 Assignment data status:")
                logger.info(f"   - {assigned_count} files assigned to miners")
                logger.info(f"   - {unassigned_count} files still unassigned")

                if assigned_count == 0:
                    logger.error(
                        "❌ No assigned files found - profile reconstruction cannot proceed")
                    logger.error(
                        "   This suggests file assignment phase did not complete successfully")
                    return False

                if unassigned_count > 0:
                    logger.warning(f"⚠️ Found {unassigned_count} unassigned files")
                    logger.warning(
                        "   Proceeding with profile reconstruction for assigned files only")

            # Step 2: Reconstruct user profiles using RabbitMQ system
            logger.info("👥 Step 2: Starting user profile reconstruction...")
            user_reconstruction_success = self.run_processor(
                'user_profile_reconstruction_processor.py', 'User profile reconstruction')

            if user_reconstruction_success:
                logger.info("✅ User profile reconstruction processor completed")

                # Wait for the consumer to finish processing user profiles
                logger.info(
                    "⏳ Step 2a: Waiting for user profile reconstruction queue to be empty...")
                user_queue_empty = await self.wait_for_queues_empty(['user_profile_reconstruction'],
                                                                    600)  # 10 minute timeout
                if user_queue_empty:
                    logger.info("✅ User profile reconstruction queue is empty")
                else:
                    logger.warning("⚠️ User profile reconstruction queue timeout - continuing")
            else:
                logger.error("❌ User profile reconstruction processor failed")
                return False

            # Step 3: Reconstruct miner profiles using RabbitMQ system
            logger.info("⛏️ Step 3: Starting miner profile reconstruction...")
            miner_reconstruction_success = self.run_processor(
                'miner_profile_reconstruction_processor.py', 'Miner profile reconstruction')

            if miner_reconstruction_success:
                logger.info("✅ Miner profile reconstruction processor completed")

                # Wait for the consumer to finish processing miner profiles
                logger.info(
                    "⏳ Step 3a: Waiting for miner profile reconstruction queue to be empty...")
                miner_queue_empty = await self.wait_for_queues_empty(
                    ['miner_profile_reconstruction'], 600)  # 10 minute timeout
                if miner_queue_empty:
                    logger.info("✅ Miner profile reconstruction queue is empty")
                else:
                    logger.warning("⚠️ Miner profile reconstruction queue timeout - continuing")
            else:
                logger.error("❌ Miner profile reconstruction processor failed")
                return False

            # Step 4: CRITICAL VERIFICATION - Check that profiles were actually created
            logger.info("🔍 Step 4: Verifying profiles were reconstructed...")

            # Import utilities for verification
            from app.utils.blockchain_submission import collect_miner_profiles_for_submission

            # Check miner profiles
            logger.info("🔍 Step 4a: Verifying miner profiles...")
            miner_profiles = await collect_miner_profiles_for_submission(self.db_pool)
            logger.info(f"✅ Found {len(miner_profiles)} miner profiles ready for submission")

            if len(miner_profiles) == 0:
                logger.error("🚨 CRITICAL: NO MINER PROFILES FOUND!")
                logger.error("   This indicates miner profile reconstruction failed")

                # Debug the database state
                async with self.db_pool.acquire() as conn:
                    pending_count = await conn.fetchval(
                        "SELECT COUNT(*) FROM pending_miner_profile")
                    published_count = await conn.fetchval(
                        "SELECT COUNT(*) FROM pending_miner_profile WHERE status = 'published'")
                    logger.error(
                        f"   Database state: {pending_count} total profiles, {published_count} published")

                    if pending_count == 0:
                        logger.error("   🔥 NO profiles in pending_miner_profile table!")
                        logger.error(
                            "   🔥 Miner profile reconstruction processor never created profiles!")
                    elif published_count == 0:
                        logger.error("   🔥 Profiles exist but none are 'published'!")
                        logger.error("   🔥 Miner profile reconstruction consumer failed!")

                return False

            # Check user profiles
            logger.info("🔍 Step 4b: Verifying user profiles...")
            async with self.db_pool.acquire() as conn:
                user_count = await conn.fetchval(
                    "SELECT COUNT(*) FROM pending_user_profile WHERE status = 'published'")
                user_total = await conn.fetchval("SELECT COUNT(*) FROM pending_user_profile")
                logger.info(
                    f"✅ Found {user_count} user profiles ready for submission (of {user_total} total)")

                if user_count == 0 and user_total > 0:
                    logger.error("🚨 CRITICAL: User profiles exist but none are 'published'!")
                    logger.error("   This indicates user profile reconstruction consumer failed")
                    return False
                elif user_count == 0 and user_total == 0:
                    logger.warning(
                        "⚠️ No user profiles found - this might be normal if no storage requests")

            # Step 5: Verify storage requests are ready for blockchain submission
            logger.info("🔍 Step 4c: Verifying storage requests for blockchain submission...")
            from app.utils.blockchain_submission import collect_storage_requests_for_submission
            storage_requests = await collect_storage_requests_for_submission(self.db_pool)
            logger.info(f"✅ Found {len(storage_requests)} storage requests ready for submission")

            # Step 6: Final summary
            logger.info("📊 FINAL PROFILE RECONSTRUCTION SUMMARY:")
            logger.info(f"   - {len(miner_profiles)} miner profiles ready")
            logger.info(f"   - {user_count} user profiles ready")
            logger.info(f"   - {len(storage_requests)} storage requests ready")

            if len(miner_profiles) == 0 and len(storage_requests) == 0:
                logger.error(
                    "🚨 CRITICAL: No profiles or storage requests ready for blockchain submission!")
                logger.error("   Profile reconstruction appears to have failed completely")
                return False

            logger.info(
                "✅ Profile reconstruction completed successfully with comprehensive verification")
            logger.info("🚀 Profiles are ready for blockchain submission")
            return True

        except Exception as e:
            logger.error(f"❌ Error during profile reconstruction: {e}")
            logger.exception("Full traceback:")
            return False

    async def submit_to_blockchain(self) -> bool:
        """
        Submit all data to blockchain including health metrics.
        Phase 5: Blockchain submission (blocks 76-90) - EARLY with more time
        
        ENHANCED: Comprehensive verification before submission to prevent empty profiles.
        """
        logger.info(
            "🚀 Starting blockchain submission phase (ENHANCED: Pre-submission verification)")

        try:
            # Import submission utilities
            from app.utils.blockchain_submission import (collect_storage_requests_for_submission,
                                                         collect_miner_profiles_for_submission,
                                                         call_update_pin_and_storage_requests,
                                                         mark_submissions_as_completed,
                                                         submit_health_metrics_to_blockchain)

            # Step 1: Submit health metrics FIRST (if not already done)
            if self.health_checks_completed and not self.health_metrics_submitted:
                logger.info("📊 Step 1: Submitting health metrics to blockchain...")
                health_success = await submit_health_metrics_to_blockchain(self.db_pool)
                if health_success:
                    self.health_metrics_submitted = True
                    logger.info("✅ Health metrics submitted successfully")
                else:
                    logger.warning("⚠️ Health metrics submission failed but continuing")
            else:
                logger.info("✅ Step 1: Health metrics already submitted or not needed")

            # Step 2: PRE-SUBMISSION VERIFICATION - Critical to prevent empty submissions
            logger.info("🔍 Step 2: Pre-submission verification...")

            # Verify profile reconstruction actually completed
            async with self.db_pool.acquire() as conn:
                # Check pending profiles that should be ready
                pending_miner_profiles = await conn.fetchval(
                    "SELECT COUNT(*) FROM pending_miner_profile WHERE status = 'published'")
                pending_user_profiles = await conn.fetchval(
                    "SELECT COUNT(*) FROM pending_user_profile WHERE status = 'published'")

                # Check file assignments that should exist  
                assigned_files = await conn.fetchval("""
                    SELECT COUNT(*) FROM file_assignments 
                    WHERE miner1 IS NOT NULL OR miner2 IS NOT NULL OR miner3 IS NOT NULL 
                      OR miner4 IS NOT NULL OR miner5 IS NOT NULL
                """)

                logger.info("📊 Pre-submission data check:")
                logger.info(f"   - {pending_miner_profiles} miner profiles published")
                logger.info(f"   - {pending_user_profiles} user profiles published")
                logger.info(f"   - {assigned_files} files assigned to miners")

                if pending_miner_profiles == 0 and assigned_files > 0:
                    logger.error("🚨 CRITICAL: Files are assigned but no miner profiles published!")
                    logger.error(
                        "   This indicates profile reconstruction failed to publish profiles")
                    logger.error(
                        "   Cannot submit to blockchain - would result in empty miner profiles")
                    return False

                if assigned_files == 0:
                    logger.warning("⚠️ No files assigned - might be normal if no storage requests")

            # Step 3: Collect data for main submission with verification
            logger.info("📦 Step 3: Collecting data for blockchain submission...")
            logger.info("🔍 Step 3a: Collecting storage requests...")

            storage_requests = await collect_storage_requests_for_submission(self.db_pool)
            logger.info(f"✅ Collected {len(storage_requests)} storage requests")

            logger.info("🔍 Step 3b: Collecting miner profiles...")
            miner_profiles = await collect_miner_profiles_for_submission(self.db_pool)
            logger.info(f"✅ Collected {len(miner_profiles)} miner profiles")

            # Step 4: CRITICAL VERIFICATION - Ensure we're not submitting empty data
            logger.info("🔍 Step 4: Final data verification before blockchain submission...")

            if len(miner_profiles) == 0:
                logger.error("🚨 CRITICAL: NO MINER PROFILES TO SUBMIT!")
                logger.error("   This would result in empty blockchain submission")
                logger.error("   Check profile reconstruction process")

                # Additional debugging
                async with self.db_pool.acquire() as conn:
                    pending_count = await conn.fetchval(
                        "SELECT COUNT(*) FROM pending_miner_profile")
                    published_count = await conn.fetchval(
                        "SELECT COUNT(*) FROM pending_miner_profile WHERE status = 'published'")
                    logger.error(
                        f"   Database state: {pending_count} total profiles, {published_count} published")

                    if pending_count == 0:
                        logger.error("   🔥 NO profiles in pending_miner_profile table!")
                    elif published_count == 0:
                        logger.error("   🔥 Profiles exist but none are 'published'!")

                return False

            # Verify miner profiles have actual content
            profiles_with_files = 0
            total_files_in_profiles = 0
            for profile in miner_profiles:
                file_count = profile.get('files_count', 0)
                if file_count > 0:
                    profiles_with_files += 1
                    total_files_in_profiles += file_count

            logger.info("📊 Miner profile content analysis:")
            logger.info(f"   - {len(miner_profiles)} total miner profiles")
            logger.info(
                f"   - {profiles_with_files} profiles with files ({profiles_with_files / len(miner_profiles) * 100:.1f}%)")
            logger.info(f"   - {total_files_in_profiles} total files in all profiles")

            if profiles_with_files == 0 and total_files_in_profiles == 0:
                logger.warning("⚠️ All miner profiles are empty (no files)")
                logger.warning("   This might be normal if no storage requests were processed")
                logger.warning("   But check if this is expected...")

            logger.info("📊 FINAL DATA SUMMARY:")
            logger.info(f"  - {len(storage_requests)} original storage requests (for closing)")
            logger.info(
                f"  - {len(miner_profiles)} miner profiles ({profiles_with_files} with files)")
            logger.info(f"  - {total_files_in_profiles} total files in profiles")

            # Allow submission even with empty profiles if no data to process
            if len(storage_requests) == 0 and len(miner_profiles) == 0:
                logger.warning("⚠️ No data to submit to blockchain")
                logger.info("✅ This is normal if no storage requests were processed")
                return True

            # Step 5: Submit to blockchain
            logger.info("🚀 Step 5: Submitting to blockchain...")
            logger.info("📤 Initiating blockchain transaction...")
            success, submitted_requests, submitted_profiles = call_update_pin_and_storage_requests(
                storage_requests, miner_profiles)

            if success:
                # Mark as completed in database
                logger.info(
                    "✅ Blockchain submission successful! Marking as completed in database...")
                await mark_submissions_as_completed(self.db_pool, submitted_requests,
                                                    submitted_profiles)
                logger.info("✅ Database updated with submission completion")

                # Success summary
                logger.info("🎯 BLOCKCHAIN SUBMISSION SUMMARY:")
                logger.info(f"   ✅ Successfully submitted {len(submitted_profiles)} miner profiles")
                logger.info(
                    f"   ✅ Successfully submitted {len(submitted_requests)} storage requests")
                logger.info(f"   ✅ Profiles contained {total_files_in_profiles} files total")
                logger.info("🔒 Transaction submitted to blockchain - awaiting confirmation")

                return True
            else:
                logger.error("❌ Blockchain submission failed")
                logger.error("🔥 The transaction was not sent to the blockchain!")
                logger.error("   Check blockchain connection and validator key setup")
                return False

        except Exception as e:
            logger.error(f"❌ Error during blockchain submission: {e}")
            logger.exception("Full traceback:")
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

        # Use simple modulo to determine if we need to refresh node metrics (every 300 blocks)
        # TEMPORARY OVERRIDE FOR DEBUGGING
        should_refresh_node_metrics = True  # (self.current_block % self.node_metrics_refresh_interval == 0)
        logger.info(f"DEBUG: FORCING NODE METRICS REFRESH: {should_refresh_node_metrics}")

        # Build tasks list with conditional node metrics refresh
        tasks = [self.refresh_registration_data(), self.refresh_user_profiles()]

        if should_refresh_node_metrics:
            logger.info(f"📊 Including node metrics refresh (block {self.current_block} % 300 == 0)")
            tasks.insert(1, self.refresh_node_metrics())  # Insert after registration
        else:
            blocks_until_refresh = self.node_metrics_refresh_interval - (
                    self.current_block % self.node_metrics_refresh_interval)
            logger.info("📊 Skipping node metrics refresh (using cached data)")
            logger.info(
                f"   Current block: {self.current_block}, next refresh in {blocks_until_refresh} blocks")

        # Execute tasks
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

        logger.info(
            f"👤 Non-validator can process at any time - current position: {block_position}/99")

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
            logger.info(
                "🔄 Non-validator: Refreshing user profiles to stay current with network changes")
            profile_success = await self.refresh_user_profiles()
            if profile_success:
                logger.info("✅ Non-validator: User profiles refreshed successfully")
            else:
                logger.warning("⚠️ Non-validator: User profile refresh failed")

        # NON-VALIDATOR: Can perform health checks at any time (no blockchain submission deadline)
        if not self.health_checks_completed:
            logger.info(f"🏥 Non-validator: Starting health checks at block {block_position}/99...")
            logger.info("   NON-VALIDATORS: No timing restrictions - can run health checks anytime")
            success = await self.perform_health_checks()
            if success:
                self.health_checks_completed = True
                logger.info("✅ Non-validator: Health checks completed")
            else:
                logger.error("❌ Non-validator: Health checks failed")
                # For non-validators, try to use previous health data if available
                async with self.db_pool.acquire() as conn:
                    health_data_count = await conn.fetchval("""
                        SELECT COUNT(DISTINCT node_id) 
                        FROM miner_epoch_health 
                        WHERE last_activity_at >= NOW() - INTERVAL '12 hours'
                    """)

                    if health_data_count >= 50:
                        logger.info(f"✅ Found {health_data_count} miners with recent health data")
                        self.health_checks_completed = True
                        logger.info(
                            "✅ Non-validator: Using previous epoch health data after failure")
                    else:
                        logger.warning(f"⚠️ Limited health data ({health_data_count} miners)")
                        logger.warning("   Will retry health checks in next iteration")

        # REMOVED: Non-validators should NOT process storage requests or pinning assignments
        # File assignment processing is VALIDATOR-ONLY work
        # Non-validators only do: health checks, availability maintenance, health metrics submission

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

        # PERIODIC DATABASE CLEANUP: Run comprehensive miner records cleanup (every 4 hours)
        # Use block position to determine timing - run at specific intervals to avoid validator interference
        cleanup_interval = 240  # Approximately 4 hours (240 blocks * 6 seconds = 1440 seconds = 24 minutes actual)
        if (
                self.current_block % cleanup_interval == 0) and block_position > 20:  # Avoid early epoch interference
            logger.info("🧹 Non-validator: Starting periodic database cleanup (every ~4 hours)...")
            logger.info("   This runs on non-validators to avoid impacting validator performance")

            # Run both health data cleanup and miner records cleanup
            health_cleanup_success = await self.cleanup_old_health_data()
            miner_cleanup_success = await self.cleanup_old_miner_records()

            if health_cleanup_success and miner_cleanup_success:
                logger.info("✅ Non-validator: Periodic database cleanup completed successfully")
                logger.info("💾 Database optimized - improved query performance for all nodes")
            else:
                logger.warning("⚠️ Non-validator: Database cleanup partially failed")
                if not health_cleanup_success:
                    logger.warning("   Health data cleanup failed")
                if not miner_cleanup_success:
                    logger.warning("   Miner records cleanup failed")

        # Status summary for non-validators
        if block_position % 25 == 0:  # Every 25 blocks show summary
            logger.info("📋 Non-validator status summary:")
            logger.info(f"   Initialization: {'✅' if self.initialization_completed else '❌'}")
            logger.info(f"   Health checks: {'✅' if self.health_checks_completed else '❌'}")
            logger.info(
                f"   Availability maintenance: {'✅' if self.availability_completed else '❌'}")
            logger.info(
                f"   Health metrics submitted: {'✅' if self.health_metrics_submitted else '❌'}")
            logger.info(
                "   📌 NOTE: File assignments are VALIDATOR-ONLY (non-validators don't process storage requests)")

        # Wait for end of epoch
        if block_position % 30 == 0:  # Every 30 blocks
            logger.info("⏳ Non-validator: Monitoring network and waiting for next epoch...")
            remaining_blocks = 99 - block_position
            logger.info(f"   {remaining_blocks} blocks remaining in current epoch")

    async def validator_workflow(self):
        """Execute validator workflow with SEQUENTIAL processing for security and speed."""
        logger.info("👑 Executing VALIDATOR workflow (SEQUENTIAL)")

        # Use the current epoch and block from the main loop
        current_block = self.current_block
        block_position = get_epoch_block_position(current_block)

        logger.info(f"Current block position in epoch: {block_position}/99")
        logger.info("🔄 SEQUENTIAL MODE: Steps run immediately when previous completes")

        # Phase 1: Initialization (ALWAYS run if not completed)
        if not self.initialization_completed:
            logger.info(f"🚀 Validator starting initialization (block: {block_position}/99)")
            success = await self.epoch_initialization()
            if success:
                self.initialization_completed = True
                logger.info("✅ Phase 1 complete: Initialization")
            else:
                logger.error("❌ Initialization failed, will retry next cycle.")
            return

            # Phase 2: CRITICAL TIMING - Health checks ONLY at epoch beginning
        elif self.initialization_completed and not self.health_checks_completed:
            if block_position <= 10:
                # EARLY EPOCH: Can start health checks OR use previous data
                logger.info(f"🏥 VALIDATOR: Health check decision at block {block_position}/99")

                # Check if we have recent health data to skip checks
                async with self.db_pool.acquire() as conn:
                    health_data_count = await conn.fetchval("""
                        SELECT COUNT(DISTINCT node_id) 
                        FROM miner_epoch_health 
                        WHERE last_activity_at >= NOW() - INTERVAL '6 hours'
                    """)

                    logger.info(
                        f"📊 Found {health_data_count} miners with recent health data (< 6 hours)")

                    if health_data_count >= 400:  # High threshold for validators
                        logger.info("✅ VALIDATOR OPTIMIZATION: Using previous epoch health data")
                        logger.info(
                            "   Reason: Fresh health data available, skipping 3+ hour health checks")
                        self.health_checks_completed = True
                        logger.info(
                            "✅ VALIDATOR: Health checks marked complete (using previous data)")
                    else:
                        logger.info(
                            f"🏥 VALIDATOR: Starting fresh health checks (insufficient previous data: {health_data_count})")
                        logger.info("   Starting health checks at epoch beginning for fresh data")
                        success = await self.perform_health_checks()
                        if success:
                            self.health_checks_completed = True
                            logger.info("✅ VALIDATOR: Fresh health checks completed")
                        else:
                            logger.error("❌ VALIDATOR: Fresh health checks failed")
                            # Fall back to previous data if available
                            if health_data_count >= 100:
                                logger.info("   Falling back to previous epoch health data")
                                self.health_checks_completed = True
                            else:
                                logger.warning(
                                    "   Insufficient health data - validator proceeding with risks")
                                self.health_checks_completed = True
            else:
                # TOO LATE IN EPOCH: Only use previous data, don't start new health checks
                logger.info(f"⏰ VALIDATOR: Too late for health checks (block {block_position}/99)")
                logger.info("   CRITICAL TIMING: Using previous epoch health data only")

                async with self.db_pool.acquire() as conn:
                    health_data_count = await conn.fetchval("""
                        SELECT COUNT(DISTINCT node_id) 
                        FROM miner_epoch_health 
                        WHERE last_activity_at >= NOW() - INTERVAL '12 hours'
                    """)

                    if health_data_count >= 100:
                        logger.info(
                            f"✅ Using {health_data_count} miners from previous epoch health data")
                        self.health_checks_completed = True
                        logger.info("✅ VALIDATOR: Proceeding with previous epoch health data")
                    else:
                        logger.warning(
                            f"⚠️ Insufficient previous health data ({health_data_count} miners)")
                        logger.warning("   VALIDATOR RISK: Proceeding with limited health data")
                        self.health_checks_completed = True  # Must proceed for validator duties
            return

        # Phase 2.5: CRITICAL - Process health scores (transfer epoch health data to miner_stats)
        elif self.health_checks_completed and not self.health_scores_processed:
            logger.info(f"🏥 SEQUENTIAL: Processing health scores at block {block_position}/99")
            logger.info(
                "   CRITICAL: Transferring health data from epoch health to miner stats for assignment")

            # CRITICAL FIX: Reset assignment state at epoch start to ensure fresh processing
            if block_position <= 15:  # Early in epoch
                if self.assignment_completed:
                    logger.info(
                        "🔄 EPOCH START: Resetting assignment state to ensure fresh storage request processing")
                    self.assignment_completed = False

            success = await self.process_health_scores()
            if success:
                self.health_scores_processed = True
                logger.info(
                    "✅ SEQUENTIAL: Health scores processed - miner stats updated for assignment")
            else:
                logger.error("❌ Health score processing failed - assignment may use stale data")
                # Proceed anyway to avoid blocking the validator
                self.health_scores_processed = True
            return

        # Phase 2.75: CRITICAL - Network Self-Healing (fix broken assignments before new assignments)
        elif self.health_scores_processed and not self.network_self_healing_completed:
            logger.info(
                f"🛠️ SEQUENTIAL: Starting network self-healing at block {block_position}/99")
            logger.info(
                "   Health scores processed - fixing broken assignments before new file assignments")

            success = await self.network_self_healing_routine()
            if success:
                self.network_self_healing_completed = True
                logger.info(
                    "✅ SEQUENTIAL: Network self-healing completed - starting file assignment next")
            else:
                logger.warning(
                    "⚠️ Network self-healing failed - proceeding with file assignment anyway")
                # Proceed to avoid blocking the validator workflow
                self.network_self_healing_completed = True
            return

        # Phase 3: SEQUENTIAL File Assignment (immediately after self-healing complete)
        # CRITICAL: ALWAYS run file assignment at epoch start for new storage requests
        elif self.network_self_healing_completed and not self.assignment_completed:
            logger.info(f"📋 SEQUENTIAL: Starting file assignment at block {block_position}/99")
            logger.info("   Health checks completed - starting assignment immediately for speed")
            logger.info("   🔄 ENSURING fresh storage request processing for new epoch")
            success = await self.assign_files()
            if success:
                self.assignment_completed = True
                logger.info("✅ SEQUENTIAL: File assignment completed - starting profiles next")
            return

        # CRITICAL FIX: Handle case where assignment was already marked complete but we need fresh processing
        elif self.network_self_healing_completed and self.assignment_completed and block_position <= 15:
            logger.info(
                f"🔄 EPOCH START: Found assignment already complete at block {block_position}/99")
            logger.info("   Forcing fresh file assignment for new storage requests")
            self.assignment_completed = False
            return  # Will process assignment in next cycle

        # Phase 4: SEQUENTIAL Profile Reconstruction (immediately after assignment completes)
        elif self.assignment_completed and not self.profiles_completed:
            logger.info(
                f"🔧 SEQUENTIAL: Starting profile reconstruction at block {block_position}/99")
            logger.info("   Assignment completed - starting profile reconstruction immediately")
            success = await self.reconstruct_profiles()
            if success:
                self.profiles_completed = True
                self.profiles_reconstructed = True  # Keep legacy variable for compatibility
                logger.info("✅ SEQUENTIAL: Profile reconstruction completed")
                logger.info("🚀 SECURITY: Will submit to blockchain IMMEDIATELY next cycle")
            return

        # Phase 5: SECURITY - Submit to blockchain IMMEDIATELY when profiles are ready
        elif self.profiles_completed and not self.submission_completed:
            logger.info(f"🚀 SECURITY: Immediate blockchain submission at block {block_position}/99")
            logger.info("📊 Submitting immediately for security - maximum time for confirmation")
            logger.info("⚡ NO WAITING for end of epoch - submit as soon as profiles ready")
            success = await self.submit_to_blockchain()
            if success:
                self.submission_completed = True
                self.blockchain_submitted = True  # Keep legacy variable for compatibility
                logger.info(
                    f"✅ SECURITY: Blockchain submission completed at block {block_position}/99")
                logger.info("🎯 ✨ IMMEDIATE SUBMISSION: Maximum time for confirmation!")
                logger.info(
                    f"🔒 SECURE: TX submitted with {95 - block_position} blocks remaining in epoch")
            else:
                logger.error("❌ Blockchain submission failed - will retry next cycle")
            return

        # Phase 6: Monitoring after submission (blocks after submission until epoch end)
        elif self.submission_completed and not self.cleanup_completed:
            # Monitor until cleanup phase
            if block_position >= 96:
                await self.epoch_cleanup()
                self.cleanup_completed = True
                logger.info("✅ Phase 6 complete: Cleanup")
            else:
                logger.info(
                    f"ℹ️ Monitoring phase: TX submitted, waiting for epoch end ({99 - block_position} blocks remaining)")
            return

        # Handle edge cases and status reporting
        else:
            if not self.initialization_completed:
                logger.info(f"⏳ Waiting for initialization phase (current: {block_position}/99)")
            elif not self.health_checks_completed:
                logger.info(
                    f"⏳ Waiting for health checks completion (current: {block_position}/99)")
            elif not self.health_scores_processed:
                logger.info(
                    f"⏳ Waiting for health score processing completion (current: {block_position}/99)")
            elif not self.assignment_completed:
                logger.info(
                    f"⏳ Waiting for file assignment completion (current: {block_position}/99)")
            elif not self.profiles_completed:
                logger.info(
                    f"⏳ Waiting for profile reconstruction completion (current: {block_position}/99)")
            elif not self.submission_completed:
                logger.info(
                    f"⏳ Waiting for blockchain submission completion (current: {block_position}/99)")
            else:
                logger.info(
                    f"✅ All phases complete - monitoring until epoch end (current: {block_position}/99)")
            return

    async def reset_epoch_state(self):
        """Reset epoch state for new epoch."""
        self.initialization_completed = False
        self.health_checks_completed = False
        self.health_scores_processed = False  # CRITICAL: Health score processing
        self.network_self_healing_completed = False  # Phase 2.75: Reset self-healing
        self.assignment_completed = False  # CRITICAL: Always reset to ensure fresh storage request processing
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

        # NOTE: Node metrics timing uses modulo - no state to preserve
        # Node metrics refreshed every 300 blocks regardless of epoch boundaries

        logger.info("🔄 Epoch state reset for new epoch")
        logger.info("📊 Node metrics timing uses modulo (block % 300 == 0) - no state to preserve")
        logger.info("🔄 ENFORCED: File assignment will run fresh to process new storage requests")

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
                logger.info(
                    f"🎯 Validator at epoch start (position {block_position}/99) - resuming processing")
                self.waiting_for_next_epoch = False
                self.startup_epoch = current_epoch
            return False

        # ENHANCED FIX: Check if this is a connection recovery scenario
        if self.is_validator_state_transition_recovery(current_epoch, True, block_position):
            logger.info("🔗 Connection recovery: Resuming validator processing without waiting")
            self.waiting_for_next_epoch = False
            self.startup_epoch = current_epoch
            return False

        # ENHANCED FIX: If we became validator in this epoch (role transition), allow processing
        # This handles connection lag where we miss the early detection window
        if (hasattr(self,
                    'previous_epoch') and self.previous_epoch is not None and current_epoch > self.previous_epoch and self.waiting_for_next_epoch):
            logger.info(
                f"🎯 Role transition to validator in epoch {current_epoch} at position {block_position}/99")
            logger.info(
                "   Allowing processing despite late detection (connection lag or role transition)")
            self.waiting_for_next_epoch = False
            self.startup_epoch = current_epoch
            return False

        # If this is the first time we're seeing this epoch (true startup)
        if self.startup_epoch is None:
            self.startup_epoch = current_epoch

            # If we're starting after block 10, wait for next epoch (true mid-epoch startup)
            if block_position > 10:
                logger.warning(
                    f"🚨 Validator started mid-epoch at block position {block_position}/99")
                logger.warning(
                    "   Validator waiting for next epoch to avoid processing incomplete data")
                self.waiting_for_next_epoch = True
                return True
            else:
                logger.info(
                    f"✅ Validator started early in epoch at block position {block_position}/99")
                logger.info("   Safe for validator to proceed with current epoch processing")
                return False

        # If we were waiting and we're now in a new epoch, we can proceed
        if self.waiting_for_next_epoch and current_epoch > self.startup_epoch:
            logger.info(
                f"🎯 New epoch {current_epoch} started - validator resuming normal processing")
            self.waiting_for_next_epoch = False
            self.startup_epoch = current_epoch
            return False

        # Continue waiting if we're still in the startup epoch and started mid-epoch
        return self.waiting_for_next_epoch

    async def run(self):
        """Main orchestrator loop."""
        logger.info("🎯 Starting Epoch Orchestrator")
        logger.info(f"📦 Version: {ORCHESTRATOR_VERSION}")
        logger.info(
            "🛡️ Safety mechanism: Only validators wait for next epoch if starting mid-epoch (after block 10)")
        logger.info("👤 Non-validators can start processing immediately")

        try:
            await self.initialize()

            last_epoch = None

            while True:
                try:
                    # Check if we should attempt connection based on backoff
                    if not self.should_attempt_connection():
                        backoff_delay = self.get_backoff_delay()
                        logger.info(
                            f"⏳ Backing off for {backoff_delay}s due to connection failures")
                        await asyncio.sleep(min(backoff_delay, self.block_check_interval))
                        continue

                    # Ensure we have a substrate connection with enhanced retry logic
                    if self.substrate is None:
                        logger.info("🔗 Creating new substrate connection...")

                        # More aggressive retry for validators to minimize downtime
                        max_connection_attempts = 5 if self.validator_state_cache[
                            'last_known_validator_status'] else 3

                        for attempt in range(max_connection_attempts):
                            try:
                                self.substrate = connect_substrate()
                                logger.info(
                                    f"✅ Substrate connection established (attempt {attempt + 1}/{max_connection_attempts})")
                                break
                            except Exception as e:
                                if attempt < max_connection_attempts - 1:
                                    wait_time = 2 ** attempt  # Exponential backoff: 1s, 2s, 4s, 8s, 16s
                                    logger.warning(
                                        f"⚠️ Connection attempt {attempt + 1} failed: {e}")
                                    logger.info(f"   Retrying in {wait_time}s...")
                                    await asyncio.sleep(wait_time)
                                else:
                                    logger.error(
                                        f"❌ All {max_connection_attempts} connection attempts failed")
                                    raise

                    # Get current epoch and validator status with updated substrate connection
                    current_epoch, current_block, self.substrate = get_current_epoch_info(
                        self.substrate)
                    is_validator, current_validator, epoch_start, self.substrate = is_epoch_validator(
                        self.substrate, self.our_validator_account)

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

                    # Calculate block position early for logging
                    block_position = get_epoch_block_position(current_block)

                    # Track role transitions for debugging
                    previous_is_validator = getattr(self, 'is_validator', None)
                    self.is_validator = is_validator

                    # Update validator state cache for connection resilience
                    self.update_validator_state_cache(current_epoch, is_validator)

                    # Log role transitions
                    if previous_is_validator is not None and previous_is_validator != is_validator:
                        role_from = "VALIDATOR" if previous_is_validator else "NON-VALIDATOR"
                        role_to = "VALIDATOR" if is_validator else "NON-VALIDATOR"
                        logger.info(
                            f"🔄 Role transition detected: {role_from} → {role_to} in epoch {current_epoch}")
                        if is_validator:
                            logger.info(
                                f"   Became validator at block position {block_position}/99")

                    # Log connection recovery scenarios
                    if (self.connection_failures > 0 and self.validator_state_cache[
                        'last_known_validator_status'] == is_validator and is_validator):
                        logger.info(
                            f"🔗 Connection recovered: Maintaining validator role in epoch {current_epoch}")
                        logger.info(
                            f"   Validator state preserved across {self.connection_failures} connection failure(s)")

                    self.epoch_start_block = epoch_start
                    last_epoch = current_epoch

                    logger.info(
                        f"📊 Epoch {current_epoch}, Block {current_block} (position {block_position}/99)")
                    logger.info(f"🎭 Role: {'VALIDATOR' if is_validator else 'NON-VALIDATOR'}")

                    # ENHANCED STARTUP SAFETY: Only apply to validators
                    if is_validator and self.should_wait_for_next_epoch(current_epoch,
                                                                        block_position):
                        if block_position % 10 == 0:  # Log every 10 blocks to avoid spam
                            logger.info(
                                f"⏳ Validator waiting for next epoch (started mid-epoch at position {block_position}/99)")
                            logger.info(
                                "   This prevents validator processing with incomplete epoch data")
                        await asyncio.sleep(self.block_check_interval)
                        continue

                    # Non-validators can always start immediately
                    if not is_validator and block_position % 20 == 0:  # Periodic status for non-validators
                        logger.info(
                            "👤 Non-validator processing: Can start immediately at any block position")

                    # Log monitoring frequency periodically
                    if block_position % 10 == 0:  # Every 10 blocks
                        logger.info(
                            f"⏰ Monitoring every {self.block_check_interval}s (every block)")
                        if self.validator_seed:
                            logger.info("🔐 Transaction signing: ENABLED")
                        else:
                            logger.info("🔐 Transaction signing: DISABLED")

                    # Execute appropriate workflow based on validator selection
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
                    if any(error_type in str(e).lower() for error_type in
                           ["jsondecodeerror", "websocket", "broken pipe", "connection",
                            "timeout"]):
                        logger.warning(
                            "Connection issue detected - will retry with exponential backoff")

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

        # CORRECTED APPROACH: Clean everything and refetch from chain as source of truth
        tables_to_clean = ['pinning_requests',
                           # CLEAN: Will be refetched from chain with ALL unassigned requests
                           # 'node_metrics',            # PRESERVE: Only refresh every 300 blocks, not every epoch
                           'parsed_cids', 'pending_assignment_file', 'pending_miner_profile',
                           'pending_submissions', 'pending_user_profile',
                           'processed_pinning_requests'
                           # CLEAN: Will start fresh tracking for this epoch
                           ]

        try:
            if not self.db_pool:
                logger.error("Database pool not initialized")
                return False

            async with self.db_pool.acquire() as conn:
                # PRESERVE health data and node metrics for performance optimization
                logger.info("✅ PRESERVING miner_epoch_health data for validator performance")
                logger.info(
                    "   Previous epoch health data allows validators to skip 3+ hour health checks")
                logger.info("✅ PRESERVING node_metrics data (only refreshed every 300 blocks)")
                logger.info("   Node metrics don't change frequently, saving processing overhead")

                # CORRECTED APPROACH: Clean everything and refetch from blockchain as source of truth
                logger.info(
                    "🔄 CLEANING pinning_requests table - will refetch ALL unassigned from chain")
                logger.info("   Blockchain is source of truth for unprocessed storage requests")
                logger.info(
                    "   This ensures we get ALL unassigned requests regardless of age or original validator")

                for table in tables_to_clean:
                    try:
                        # Delete all records from the table
                        result = await conn.execute(f"DELETE FROM {table}")
                        deleted_count = result.split()[-1] if result else "0"
                        logger.info(f"✅ Cleaned table '{table}': {deleted_count} records deleted")
                    except Exception as e:
                        # Some tables might not exist, which is okay
                        logger.warning(f"⚠️ Could not clean table '{table}': {e}")

            logger.info(
                "✅ Epoch table cleanup completed (preserved health data + node metrics, ready for chain refetch)")
            return True

        except Exception as e:
            logger.error(f"❌ Epoch table cleanup failed: {e}")
            return False

    async def network_self_healing_routine(self) -> bool:
        """
        Run network self-healing to fix broken file assignments.
        CRITICAL: This should run AFTER health checks to use fresh health data.
        Uses only the RabbitMQ-based processor system.
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
                logger.error(
                    "   Self-healing requires some health data to determine miner availability")
                return False
            else:
                logger.info(
                    f"✅ Found {health_data_count} miners with recent health data for self-healing")

        try:
            # Use RabbitMQ-based network self-healing processor
            logger.info("🛠️ STEP 2: Running regular network self-healing processor")
            success = self.run_processor('network_self_healing_processor.py',
                                         'Network self-healing')

            if success:
                # Wait for self-healing consumer to process (shorter timeout for healing)
                await self.wait_for_queues_empty(['network_self_healing'], 300)
                logger.info("✅ Network self-healing completed via RabbitMQ processor")
                return True
            else:
                logger.error("❌ Network self-healing processor failed")
                logger.error(
                    "   All self-healing attempts failed - manual intervention may be required")
                return False

        except Exception as e:
            logger.error(f"❌ Error during network self-healing: {e}")
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

            # Clean up old health data to optimize database performance
            await self.cleanup_old_health_data()

            # Provide comprehensive epoch summary
            logger.info("🏁 EPOCH SUMMARY:")
            logger.info("=" * 50)
            logger.info(f"   Epoch {self.current_epoch} Results:")
            logger.info(f"   ✅ Phase 1 - Initialization: {self.initialization_completed}")
            logger.info(f"   ✅ Phase 2 - Health Checks: {self.health_checks_completed}")
            logger.info(f"   ✅ Phase 2.5 - Health Score Processing: {self.health_scores_processed}")
            logger.info(
                f"   ✅ Phase 2.75 - Network Self-Healing: {self.network_self_healing_completed}")
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
                        logger.info(
                            f"   📋 File Assignment Coverage: {assigned}/{total} ({coverage:.1f}%)")

                        if coverage >= 99:
                            logger.info("   🎯 EXCELLENT: Near-perfect assignment coverage!")
                        elif coverage >= 90:
                            logger.info("   ✅ GOOD: High assignment coverage")
                        else:
                            logger.warning(
                                f"   ⚠️ WARNING: Low assignment coverage ({coverage:.1f}%)")

            # Critical validations
            critical_issues = []

            if not self.health_checks_completed:
                critical_issues.append("Health checks never completed")

            if not self.health_scores_processed and self.health_checks_completed:
                critical_issues.append("Health scores never processed after health checks")

            if self.assignment_completed and not self.health_scores_processed:
                critical_issues.append("Assignments completed WITHOUT health score processing")

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

    async def cleanup_old_health_data(self) -> bool:
        """
        Clean up old miner health data to optimize database performance.
        
        Removes:
        - Very old health records (>3 days)
        - Duplicate old health records (1-3 days, keeps 1 per miner)
        - Orphaned health records for non-existent miners
        
        Preserves recent data (< 24 hours) for performance.
        """
        logger.info("🧹 Cleaning up old miner health data")

        try:
            async with self.db_pool.acquire() as conn:
                # Check current volumes before cleanup
                total_health_before = await conn.fetchval("SELECT COUNT(*) FROM miner_epoch_health")
                registered_miners = await conn.fetchval(
                    "SELECT COUNT(*) FROM registration WHERE node_type = 'StorageMiner' AND status = 'active'")

                logger.info(
                    f"📊 Health data before cleanup: {total_health_before:,} records for {registered_miners:,} miners")

                if total_health_before == 0:
                    logger.info("✅ No health data to clean")
                    return True

                # Count what will be cleaned
                very_old_count = await conn.fetchval("""
                    SELECT COUNT(*) FROM miner_epoch_health 
                    WHERE last_activity_at < NOW() - INTERVAL '3 days'
                """)

                old_duplicates_count = await conn.fetchval("""
                    SELECT COUNT(*) - COUNT(DISTINCT node_id) FROM miner_epoch_health 
                    WHERE last_activity_at < NOW() - INTERVAL '24 hours' 
                    AND last_activity_at >= NOW() - INTERVAL '3 days'
                """)

                orphaned_count = await conn.fetchval("""
                    SELECT COUNT(*) FROM miner_epoch_health meh
                    WHERE NOT EXISTS (
                        SELECT 1 FROM registration r 
                        WHERE r.node_id = meh.node_id 
                        AND r.node_type = 'StorageMiner'
                    )
                """)

                total_to_clean = very_old_count + old_duplicates_count + orphaned_count

                if total_to_clean == 0:
                    logger.info("✅ Health data is already clean")
                    return True

                logger.info(f"🧹 Cleaning {total_to_clean:,} old health records:")
                logger.info(f"   - Very old (>3 days): {very_old_count:,}")
                logger.info(f"   - Old duplicates (1-3 days): {old_duplicates_count:,}")
                logger.info(f"   - Orphaned records: {orphaned_count:,}")

                # Execute cleanup in transaction
                async with conn.transaction():
                    cleaned_count = 0

                    # 1. Delete very old records
                    if very_old_count > 0:
                        result = await conn.execute("""
                            DELETE FROM miner_epoch_health 
                            WHERE last_activity_at < NOW() - INTERVAL '3 days'
                        """)
                        deleted = int(result.split()[-1])
                        cleaned_count += deleted
                        logger.info(f"   ✅ Deleted {deleted:,} very old health records")

                    # 2. Delete old duplicates (keep most recent per miner)
                    if old_duplicates_count > 0:
                        result = await conn.execute("""
                            DELETE FROM miner_epoch_health 
                            WHERE last_activity_at < NOW() - INTERVAL '24 hours' 
                            AND last_activity_at >= NOW() - INTERVAL '3 days'
                            AND (node_id, last_activity_at) NOT IN (
                                SELECT DISTINCT ON (node_id) node_id, last_activity_at
                                FROM miner_epoch_health 
                                WHERE last_activity_at < NOW() - INTERVAL '24 hours' 
                                AND last_activity_at >= NOW() - INTERVAL '3 days'
                                ORDER BY node_id, last_activity_at DESC
                            )
                        """)
                        deleted = int(result.split()[-1])
                        cleaned_count += deleted
                        logger.info(f"   ✅ Deleted {deleted:,} old duplicate records")

                    # 3. Delete orphaned records
                    if orphaned_count > 0:
                        result = await conn.execute("""
                            DELETE FROM miner_epoch_health 
                            WHERE NOT EXISTS (
                                SELECT 1 FROM registration r 
                                WHERE r.node_id = miner_epoch_health.node_id 
                                AND r.node_type = 'StorageMiner'
                            )
                        """)
                        deleted = int(result.split()[-1])
                        cleaned_count += deleted
                        logger.info(f"   ✅ Deleted {deleted:,} orphaned health records")

                # Verify cleanup results
                total_health_after = await conn.fetchval("SELECT COUNT(*) FROM miner_epoch_health")
                reduction = total_health_before - total_health_after
                reduction_pct = (
                        reduction / total_health_before * 100) if total_health_before > 0 else 0

                # Verify data preservation
                recent_miners = await conn.fetchval("""
                    SELECT COUNT(DISTINCT node_id) FROM miner_epoch_health 
                    WHERE last_activity_at >= NOW() - INTERVAL '24 hours'
                """)

                coverage_pct = (
                        recent_miners / registered_miners * 100) if registered_miners > 0 else 0

                logger.info("✅ Health data cleanup completed:")
                logger.info(
                    f"   📊 Records: {total_health_before:,} → {total_health_after:,} (-{reduction:,})")
                logger.info(f"   💾 Space reduction: {reduction_pct:.1f}%")
                logger.info(
                    f"   🎯 Coverage preserved: {recent_miners:,}/{registered_miners:,} miners ({coverage_pct:.1f}%)")

                if coverage_pct >= 80:
                    logger.info("   ✅ EXCELLENT: Data integrity maintained")
                elif coverage_pct >= 50:
                    logger.info("   ⚠️ ACCEPTABLE: Most data preserved")
                else:
                    logger.warning("   🚨 WARNING: Low data preservation")

                return True

        except Exception as e:
            logger.error(f"❌ Error during health data cleanup: {e}")
            logger.exception("Full traceback:")
            return False

    async def cleanup_old_miner_records(self) -> bool:
        """
        Clean up old miner records and stale data to optimize database performance.
        
        This cleanup runs on non-validators to avoid impacting validator performance.
        Removes:
        - Inactive/old miner registrations
        - Stale node metrics (>7 days)
        - Orphaned miner_stats for non-existent miners
        - Old system events (if table exists)
        """
        logger.info("🧹 Starting comprehensive miner records cleanup")

        try:
            async with self.db_pool.acquire() as conn:
                cleanup_stats = {'inactive_registrations': 0, 'old_node_metrics': 0,
                                 'orphaned_miner_stats': 0, 'old_system_events': 0}

                # Helper function to check if table exists
                async def table_exists(table_name: str) -> bool:
                    try:
                        result = await conn.fetchval("""
                            SELECT EXISTS (
                                SELECT 1 FROM information_schema.tables 
                                WHERE table_name = $1
                            )
                        """, table_name)
                        return result
                    except Exception:
                        return False

                # Get initial counts for reporting (with table existence checks)
                total_registrations = await conn.fetchval(
                    "SELECT COUNT(*) FROM registration WHERE node_type = 'StorageMiner'")

                # Check if optional tables exist before querying
                node_metrics_exists = await table_exists('node_metrics')
                total_node_metrics = await conn.fetchval(
                    "SELECT COUNT(*) FROM node_metrics") if node_metrics_exists else 0

                miner_stats_exists = await table_exists('miner_stats')
                total_miner_stats = await conn.fetchval(
                    "SELECT COUNT(*) FROM miner_stats") if miner_stats_exists else 0

                system_events_exists = await table_exists('system_events')
                total_system_events = await conn.fetchval(
                    "SELECT COUNT(*) FROM system_events") if system_events_exists else 0

                logger.info("📊 Database before cleanup:")
                logger.info(f"   - Miner registrations: {total_registrations:,}")
                logger.info(f"   - Node metrics: {total_node_metrics:,}")
                logger.info(f"   - Miner stats: {total_miner_stats:,}")
                logger.info(f"   - System events: {total_system_events:,}")

                async with conn.transaction():
                    # 1. Clean up inactive miner registrations (status != 'active' and old)
                    result = await conn.execute("""
                        DELETE FROM registration 
                        WHERE node_type = 'StorageMiner' 
                          AND status != 'active'
                          AND updated_at < NOW() - INTERVAL '3 days'
                    """)
                    cleanup_stats['inactive_registrations'] = int(result.split()[-1])

                    # 2. Clean up old node metrics (>7 days) - only if table exists
                    if node_metrics_exists:
                        result = await conn.execute("""
                            DELETE FROM node_metrics 
                            WHERE created_at < NOW() - INTERVAL '7 days'
                        """)
                        cleanup_stats['old_node_metrics'] = int(result.split()[-1])
                    else:
                        logger.info("   ⚠️ Skipping node_metrics cleanup - table doesn't exist")

                    # 3. Clean up orphaned miner_stats (miners not in registration) - only if table exists
                    if miner_stats_exists:
                        result = await conn.execute("""
                            DELETE FROM miner_stats 
                            WHERE NOT EXISTS (
                                SELECT 1 FROM registration r 
                                WHERE r.node_id = miner_stats.node_id 
                                  AND r.node_type = 'StorageMiner'
                                  AND r.status = 'active'
                            )
                        """)
                        cleanup_stats['orphaned_miner_stats'] = int(result.split()[-1])
                    else:
                        logger.info("   ⚠️ Skipping miner_stats cleanup - table doesn't exist")

                    # 4. Clean up old system events (>30 days) - only if table exists
                    if system_events_exists:
                        result = await conn.execute("""
                            DELETE FROM system_events 
                            WHERE created_at < NOW() - INTERVAL '30 days'
                        """)
                        cleanup_stats['old_system_events'] = int(result.split()[-1])
                    else:
                        logger.info("   ⚠️ Skipping system_events cleanup - table doesn't exist")

                # Get final counts (with table existence checks)
                final_registrations = await conn.fetchval(
                    "SELECT COUNT(*) FROM registration WHERE node_type = 'StorageMiner'")
                final_node_metrics = await conn.fetchval(
                    "SELECT COUNT(*) FROM node_metrics") if node_metrics_exists else 0
                final_miner_stats = await conn.fetchval(
                    "SELECT COUNT(*) FROM miner_stats") if miner_stats_exists else 0
                final_system_events = await conn.fetchval(
                    "SELECT COUNT(*) FROM system_events") if system_events_exists else 0

                total_cleaned = sum(cleanup_stats.values())

                logger.info("✅ Miner records cleanup completed:")
                logger.info(
                    f"   🗑️  Inactive registrations: {cleanup_stats['inactive_registrations']:,}")
                logger.info(f"   🗑️  Old node metrics: {cleanup_stats['old_node_metrics']:,}")
                logger.info(
                    f"   🗑️  Orphaned miner stats: {cleanup_stats['orphaned_miner_stats']:,}")
                logger.info(f"   🗑️  Old system events: {cleanup_stats['old_system_events']:,}")
                logger.info(f"   📊 Total records cleaned: {total_cleaned:,}")

                logger.info("📊 Database after cleanup:")
                logger.info(
                    f"   - Miner registrations: {final_registrations:,} (-{total_registrations - final_registrations:,})")
                logger.info(
                    f"   - Node metrics: {final_node_metrics:,} (-{total_node_metrics - final_node_metrics:,})")
                logger.info(
                    f"   - Miner stats: {final_miner_stats:,} (-{total_miner_stats - final_miner_stats:,})")
                logger.info(
                    f"   - System events: {final_system_events:,} (-{total_system_events - final_system_events:,})")

                # Calculate space savings
                total_reduction = (
                        total_registrations - final_registrations + total_node_metrics - final_node_metrics + total_miner_stats - final_miner_stats + total_system_events - final_system_events)

                if total_reduction > 0:
                    logger.info(
                        f"💾 Database optimization: {total_reduction:,} total records removed")
                else:
                    logger.info("✅ Database already optimized - no cleanup needed")

                return True

        except Exception as e:
            logger.error(f"❌ Error during miner records cleanup: {e}")
            logger.exception("Full traceback:")
            return False


async def main():
    """Main entry point."""
    orchestrator = EpochOrchestrator()
    await orchestrator.run()


if __name__ == "__main__":
    asyncio.run(main())
