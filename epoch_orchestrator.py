#!/usr/bin/env python3
import asyncio
import logging
import os
import subprocess
import sys
import time
from datetime import datetime

from app.utils.blockchain_submission import (
    call_update_pin_and_storage_requests,
    collect_miner_profiles_for_submission,
    collect_storage_requests_for_submission,
    mark_submissions_as_completed,
    submit_health_metrics_to_blockchain,
    submit_unpin_requests_to_blockchain,
)
from rabbitmq import (
    availability_manager_processor,
    file_assignment_processor,
    miner_profile_reconstruction_processor,
    network_self_healing_processor,
    node_metrics_processor,
    pinning_request_processor,
    registration_processor,
    unpin_request_processor,
    user_profile_processor,
    user_profile_reconstruction_processor,
)
from substrate_fetcher.validator_workflow import ValidatorWorkflow

sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from app.db.connection import close_db_pool, get_db_pool, init_db_pool
from app.utils.epoch_validator import (
    connect_substrate,
    get_current_epoch_info,
    get_epoch_block_position,
    get_validator_account_from_env,
    is_epoch_validator,
)

ORCHESTRATOR_VERSION = "0.2"
logger = logging.getLogger("epoch-orchestrator")


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
        self.health_metrics_submitted = False
        self.availability_completed = False  # Track availability maintenance
        self.blockchain_submitted = False

        # New state variables for enhanced workflow tracking
        self.profiles_completed = False  # Phase 4: Profile reconstruction
        self.submission_completed = False  # Phase 5: Blockchain submission
        self.cleanup_completed = False  # Phase 6: Cleanup and summary

        # Enhanced state persistence across connection failures
        self.validator_state_cache = {
            "last_known_epoch": None,
            "last_known_validator_status": False,
            "last_successful_connection": None,
            "validator_epoch_start": None,  # Track when we became validator
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
        self.block_check_interval = int(os.getenv("BLOCK_CHECK_INTERVAL", "6"))  # seconds (every block)
        self.queue_check_timeout = int(os.getenv("QUEUE_CHECK_TIMEOUT", "300"))  # seconds

        # Validator seed for transaction signing
        self.validator_seed = os.getenv("VALIDATOR_SEED")  # Optional for signing transactions

    async def initialize(self):
        """Initialize connections and get our validator account."""
        try:
            # Get our validator account
            self.our_validator_account = get_validator_account_from_env()

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
        self.validator_state_cache["last_successful_connection"] = time.time()

    def record_connection_failure(self):
        """Record connection failure, increment failure count."""
        self.connection_failures += 1
        self.last_failure_time = time.time()
        logger.warning(f"Connection failure #{self.connection_failures}, next attempt in {self.get_backoff_delay()}s")

        # Log state cache for debugging connection issues
        if self.validator_state_cache["last_known_validator_status"]:
            logger.info(
                f"📋 Validator state cache: Was validator in epoch {self.validator_state_cache['last_known_epoch']}"
            )

    def update_validator_state_cache(self, epoch: int, is_validator: bool):
        """Update the validator state cache with current information."""
        # Track when we become validator
        if is_validator and not self.validator_state_cache["last_known_validator_status"]:
            self.validator_state_cache["validator_epoch_start"] = epoch
            logger.info(f"📝 Cached: Became validator in epoch {epoch}")

        self.validator_state_cache["last_known_epoch"] = epoch
        self.validator_state_cache["last_known_validator_status"] = is_validator

    def is_validator_state_transition_recovery(
        self, current_epoch: int, is_validator: bool, block_position: int
    ) -> bool:
        """
        Determine if this is a recovery from connection failure where we were already validator.

        Returns:
            True if this is a connection recovery scenario (not true mid-epoch startup)
        """
        cache = self.validator_state_cache

        # If we have no cached state, this could be true startup
        if cache["last_known_epoch"] is None:
            return False

        # FIXED: Only return True if we haven't already processed recovery for this epoch
        # Add a flag to prevent repeated recovery detection
        recovery_key = f"recovery_processed_{current_epoch}"
        if hasattr(self, recovery_key) and getattr(self, recovery_key):
            return False

        # If we were validator in the same epoch before connection failure
        if (
            cache["last_known_validator_status"]
            and cache["last_known_epoch"] == current_epoch
            and is_validator
            and cache["validator_epoch_start"] == current_epoch
            and self.waiting_for_next_epoch
        ):  # Only if we're actually waiting
            logger.info(
                f"🔄 Connection recovery detected: Was validator in epoch {current_epoch} before connection failure"
            )
            logger.info(f"   Validator since epoch start, connection failed at block ~{block_position}")

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
            logger.info(f"🚀 Starting {description} (processor: {processor_name})")

            # Run the processor
            result = subprocess.run(
                ["python", f"rabbitmq/{processor_name}"],
                capture_output=True,
                text=True,
                timeout=600,
            )  # 10 minute timeout

            if result.returncode == 0:
                logger.info(f"✅ {description} completed successfully")
                # Log processor output for debugging if there are any warnings
                if result.stdout and ("WARNING" in result.stdout.upper() or "ERROR" in result.stdout.upper()):
                    logger.warning(f"⚠️ {description} completed but had warnings/errors in output:")
                    logger.warning(f"STDOUT: {result.stdout}")
                return True
            else:
                logger.error(f"❌ {description} failed with return code {result.returncode}")
                logger.error(f"📋 Processor: {processor_name}")
                if result.stdout:
                    logger.error(f"📄 STDOUT: {result.stdout}")
                if result.stderr:
                    logger.error(f"🚨 STDERR: {result.stderr}")
                else:
                    logger.error("🚨 No STDERR output - processor may have failed silently")
                return False

        except subprocess.TimeoutExpired as e:
            logger.error(f"❌ {description} timed out after 10 minutes")
            logger.error(f"📋 Processor: {processor_name}")
            logger.error("⏰ This may indicate the processor is stuck or processing too much data")

            # Log any output that was captured before the timeout
            if hasattr(e, "stdout") and e.stdout:
                logger.error(f"📄 STDOUT (before timeout): {e.stdout}")
            if hasattr(e, "stderr") and e.stderr:
                logger.error(f"🚨 STDERR (before timeout): {e.stderr}")
            else:
                logger.error("🚨 No STDERR output captured before timeout")

            return False
        except Exception as e:
            logger.error(f"❌ Error running {description}: {e}")
            logger.error(f"📋 Processor: {processor_name}")
            logger.exception(f"Full traceback for {description} error:")
            return False

    async def wait_for_queues_empty(self, queue_names: list[str], timeout: int = 300) -> bool:
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
            import os

            import aio_pika

            # Get RabbitMQ connection URL
            rabbitmq_url = os.getenv("RABBITMQ_URL", "amqp://admin:admin@rabbitmq-service:5672/")

            start_time = asyncio.get_event_loop().time()
            check_interval = 3  # Check every 10 seconds

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
                            queue = await channel.declare_queue(queue_name, durable=True, passive=True)
                            message_count = queue.declaration_result.message_count
                            queue_status[queue_name] = message_count

                            if message_count > 0:
                                all_empty = False

                        except Exception as e:
                            # Queue doesn't exist or can't access - treat as empty
                            logger.debug(f"Queue {queue_name} not accessible (treating as empty): {e}")
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

    async def refresh_registration_data(self):
        """Refresh registration table with new data."""
        logger.info("🔄 Refreshing registration data")

        # Clear and refill registration table
        await registration_processor.main()
        await self.wait_for_queues_empty(["registration_latest"], 120)

    async def refresh_node_metrics(self):
        """Refresh node metrics data."""
        logger.info("🔄 Refreshing node metrics")

        await node_metrics_processor.main()
        await self.wait_for_queues_empty(["node_metrics_latest"], 120)

    async def refresh_user_profiles(self):
        """Refresh user profiles data."""
        logger.info("🔄 Refreshing user profiles")

        await user_profile_processor.main()
        await self.wait_for_queues_empty(["user_profile"], 300)
        await user_profile_processor.cleanup_orphaned_files()

    async def refresh_miner_profiles(self):
        """Refresh miner profiles data."""
        logger.info(
            "🔄 Refreshing miner profiles (removed - using pending_miner_profile only)"
        )  # Removed miner_profile_processor - using pending_miner_profile table only

    async def perform_health_checks(self) -> bool:
        """Perform miner health checks."""
        logger.info("🏥 Performing miner health checks")

        success = self.run_processor("miner_health_processor.py", "Miner health checks")

        if success:
            # Wait for health check consumer to process
            await self.wait_for_queues_empty(["miner_health_check"], 600)

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
                    WHERE meh.last_activity_at >= NOW() - INTERVAL '24 hours'
                    ORDER BY meh.node_id, meh.last_activity_at DESC
                """)

                if not health_calculations:
                    logger.warning("⚠️ No recent health data found to process")
                    return False

                logger.info(f"📊 Processing health scores for {len(health_calculations)} miners")

                # Update miner_stats with pin check data (health_score is auto-calculated from successful_pin_checks/total_pin_checks)
                updated_count = 0
                for health in health_calculations:
                    node_id = health["node_id"]
                    ping_successes = health["ping_successes"] or 0
                    ping_failures = health["ping_failures"] or 0
                    pin_successes = health["pin_check_successes"] or 0
                    pin_failures = health["pin_check_failures"] or 0

                    # Calculate totals for the generated health_score column
                    total_pin_checks = pin_successes + pin_failures
                    successful_pin_checks = pin_successes

                    # Update or insert into miner_stats (health_score is auto-calculated)
                    # Note: miner_stats table only has pin check columns, not ping columns
                    await conn.execute(
                        """
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
                    """,
                        node_id,
                        successful_pin_checks,
                        total_pin_checks,
                    )

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
                    logger.warning(f"⚠️ Only {healthy_miners} healthy miners - may impact assignment quality")

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
        logger.info("   🔄 This includes both NEW requests AND old unprocessed requests from previous validators")

        # Step 1: Run the processor to fetch requests from the chain and put them on the queue
        logger.info("   Running pinning_request_processor.py to fetch ALL unassigned requests from chain...")
        await pinning_request_processor.main()
        logger.info("   ✅ Pinning request processor completed.")
        logger.info("   ⏳ Waiting for 'pinning_request' queue to be processed...")
        await self.wait_for_queues_empty(["pinning_request"], 300)
        logger.info("   ✅ 'pinning_request' queue processed.")

        logger.info("✅ ALL unassigned pinning requests (new + old unprocessed) and their files have been processed.")
        return True

    async def process_unpinning_requests(self) -> bool:
        logger.info("Running unpin_request_processor.py")
        await unpin_request_processor.main()
        await self.wait_for_queues_empty(
            ["unpin_request"],
            300,
        )
        return True

    async def assign_files(self) -> bool:
        """
        Assign miners to files using the previous ValidatorWorkflow approach.
        Phase 3: File assignment (blocks 36-60)"""
        logger.info("📋 Starting file assignment phase")

        logger.info("📌 Step 1: Processing pinning requests for new files before assignment...")
        await self.process_pinning_requests()

        logger.info("📌 Step 2: Processing unpinning requests too...")
        await self.process_unpinning_requests()

        try:
            # Create workflow instance
            workflow = ValidatorWorkflow(validator_account_id=self.our_validator_account)

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

                for row in rows:
                    # Convert to the format expected by ValidatorWorkflow (using file CID as request_hash)
                    storage_request = (
                        (
                            row["owner"],
                            row["file_hash"],
                        ),  # Use file CID as unique identifier
                        {
                            "file_hash": row["file_hash"],  # Individual file CID
                            "file_name": row["file_name"],
                            "file_size": row["file_size"] or 0,
                            # Handle NULL sizes
                            "total_replicas": row["total_replicas"],
                            "created_at": row["created_at"],
                        },
                    )
                    storage_requests.append(storage_request)

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
                    AND COALESCE(nm.ipfs_storage_max, 0) >= 2199023255552
                    ORDER BY COALESCE(ms.health_score, 100) DESC
                """)

                # DEBUG LOGGING: Print raw miner profiles from DB
                logger.info(f"DEBUG: Raw miner profiles from DB: {len(rows)} miners")

                for row in rows:
                    # Convert to the format expected by ValidatorWorkflow
                    miner_profile = {
                        "node_id": row["node_id"],
                        "ipfs_peer_id": row["ipfs_peer_id"],
                        "owner_account": row["owner_account"],
                        "storage_capacity_bytes": row["storage_capacity_bytes"],
                        "total_files_pinned": row["total_files_pinned"],
                        "total_files_size_bytes": row["total_files_size_bytes"],
                        "health_score": row["health_score"],
                    }
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
                    node_reg = type(
                        "NodeReg",
                        (),
                        {
                            "node_id": row["node_id"],
                            "ipfs_node_id": row["ipfs_peer_id"],
                        },
                    )()
                    node_registration.append(node_reg)

            # Process individual files using ValidatorWorkflow
            (
                user_profiles,
                processed_miner_profiles,
            ) = await workflow.process_storage_requests(
                storage_requests=storage_requests,
                miner_profiles=miner_profiles,
                node_registration=node_registration,
            )

            logger.info("✅ ValidatorWorkflow completed:")
            logger.info(f"   📝 Generated {len(user_profiles)} user profile entries")
            logger.info(f"   ⛏️ Generated {len(processed_miner_profiles)} miner profile entries")

            # Update file_assignments with all 5 miners at once using bulk UPDATE
            logger.info("💾 Step 3c: Updating file assignments in database...")
            logger.info(f"💾 Bulk updating {len(user_profiles)} file assignments with all 5 miners...")

            # Prepare bulk data for all 5 miners at once
            bulk_data = []
            for profile in user_profiles:
                file_cid = profile["file_hash"]
                owner = profile["user_id"]
                assigned_miners = profile.get("assigned_miners", [])

                # Assign miners to miner1, miner2, miner3, miner4, miner5 slots
                miner_slots = [None] * 5
                for i, miner_id in enumerate(assigned_miners[:5]):  # Max 5 miners
                    miner_slots[i] = miner_id

                bulk_data.append(
                    (
                        file_cid,
                        owner,
                        miner_slots[0],
                        miner_slots[1],
                        miner_slots[2],
                        miner_slots[3],
                        miner_slots[4],
                    )
                )

            async with self.db_pool.acquire() as conn:
                async with conn.transaction():
                    logger.info(f"💾 {len(bulk_data)} assignments updated with all 5 miners")

                    # Flag all affected miners for profile reconstruction
                    affected_miners = set()
                    for _, _, miner1, miner2, miner3, miner4, miner5 in bulk_data:
                        for miner in [miner1, miner2, miner3, miner4, miner5]:
                            if miner:
                                affected_miners.add(miner)

                    if affected_miners:
                        logger.info(f"🏷️ Flagging {len(affected_miners)} miners for profile reconstruction")
                        await conn.executemany(
                            """
                            INSERT INTO pending_miner_profile (node_id, status, created_at) 
                            VALUES ($1, 'needs_reconstruction', NOW())
                            ON CONFLICT (node_id) DO UPDATE SET 
                                status = 'needs_reconstruction',
                                created_at = NOW()
                            """,
                            [(miner_id,) for miner_id in affected_miners],
                        )

                    # Also store in storage_requests table for blockchain submission
                    await conn.execute("DELETE FROM storage_requests")

                    # Pre-process storage_requests data for bulk insert
                    storage_bulk_data = []
                    for profile in user_profiles:
                        owner = profile["user_id"]
                        assigned_miners = profile.get("assigned_miners", [])

                        # Convert created_at (datetime) to Unix timestamp
                        created_at_value = profile.get("created_at", 0)
                        if hasattr(created_at_value, "timestamp"):
                            # It's a datetime object, convert to Unix timestamp
                            timestamp = int(created_at_value.timestamp())
                        elif isinstance(created_at_value, str):
                            # It's a datetime string, parse and convert
                            try:
                                dt = datetime.fromisoformat(created_at_value.replace("Z", "+00:00"))
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

                        storage_bulk_data.append(
                            (
                                owner,
                                profile["file_hash"],
                                profile.get("file_name", ""),
                                profile["file_size_in_bytes"],
                                len(assigned_miners),
                                timestamp,
                                # last_charged_at
                                timestamp,  # created_at
                                assigned_miners,
                                self.our_validator_account,
                                "assigned",
                            )
                        )

                    # Bulk insert storage requests for blockchain submission
                    if storage_bulk_data:
                        await conn.executemany(
                            """
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
                        """,
                            storage_bulk_data,
                        )

                    logger.info(f"💾 Updated {len(bulk_data)} individual file assignments")
                    logger.info(
                        f"💾 Created {len(storage_bulk_data)} storage request entries for blockchain submission"
                    )

            # CRITICAL ENHANCEMENT: Verify no unassigned files remain before declaring success
            logger.info("🔍 Step 4: Verifying assignment completion...")
            async with self.db_pool.acquire() as conn:
                unassigned_count = await conn.fetchval("""
                    SELECT COUNT(*) FROM file_assignments 
                    WHERE miner1 IS NULL AND miner2 IS NULL AND miner3 IS NULL 
                      AND miner4 IS NULL AND miner5 IS NULL
                """)

                if unassigned_count > 0:
                    logger.warning(f"⚠️ Found {unassigned_count} files still unassigned after assignment process")
                    logger.warning(
                        "   This suggests assignment process was incomplete"
                    )  # Don't return False immediately - might be files with no available miners
                else:
                    logger.info("✅ All files have been assigned to miners")

            logger.info("✅ Individual file assignment completed successfully with ValidatorWorkflow")
            logger.info("🎯 File assignments ready for profile reconstruction")
            return True

        except Exception as e:
            logger.error(f"❌ Error during ValidatorWorkflow file assignment: {e}")
            logger.exception("Full traceback:")
            return False

    async def run_availability_maintenance(self) -> bool:
        """Run file availability maintenance to handle empty assignments and failures."""
        logger.info("🛠️ Running file availability maintenance")

        await availability_manager_processor.main()

    async def reconstruct_profiles(self) -> bool:
        """
        Reconstruct user and miner profiles from file assignments.
        Phase 4: Profile reconstruction (blocks 61-75)

        ENHANCED: Comprehensive queue monitoring and data verification to prevent race conditions.
        """
        logger.info("🔧 Starting file assignment and profiler reconstruction phase...")

        try:
            logger.info("📋 Step 1.5: Running file assignment processor...")
            await file_assignment_processor.main()
            await self.wait_for_queues_empty(["file_assignment"], 300)
            logger.info("✅ File assignment processor completed")

            # Step 2: Reconstruct user profiles using RabbitMQ system
            logger.info("👥 Step 2: Starting user profile reconstruction...")
            await user_profile_reconstruction_processor.main()
            await self.wait_for_queues_empty(
                ["user_profile_reconstruction"],
                600,
            )

            # Step 3: Reconstruct miner profiles using RabbitMQ system
            logger.info("⛏️ Step 3: Starting miner profile reconstruction...")
            await miner_profile_reconstruction_processor.main()
            await self.wait_for_queues_empty(
                ["miner_profile_reconstruction"],
                600,
            )
            logger.info("✅ Profile reconstruction completed successfully...")

            return True
        except Exception:
            logger.exception("Error during profile reconstruction:")
            return False

    async def submit_to_blockchain(self) -> bool:
        """
        Submit all data to blockchain including health metrics.
        Phase 5: Blockchain submission (blocks 76-90) - EARLY with more time

        ENHANCED: Comprehensive verification before submission to prevent empty profiles.
        """
        logger.info("🚀 Starting blockchain submission phase")

        try:
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

            logger.info("🔍 Step 2: Pre-submission verification...")

            # Verify profile reconstruction actually completed
            async with self.db_pool.acquire() as conn:
                # Check pending profiles that should be ready
                pending_miner_profiles = await conn.fetchval(
                    "SELECT COUNT(*) FROM pending_miner_profile WHERE status = 'published'"
                )
                pending_user_profiles = await conn.fetchval(
                    "SELECT COUNT(*) FROM pending_user_profile WHERE status = 'published'"
                )

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
                    logger.error("   This indicates profile reconstruction failed to publish profiles")

                if assigned_files == 0:
                    logger.warning("⚠️ No files assigned - might be normal if no storage requests")

            # Step 3: Collect data for main submission with verification
            logger.info("🔍 Step 3a: Collecting storage requests...")

            storage_requests = await collect_storage_requests_for_submission(self.db_pool)
            logger.info(f"✅ Collected {len(storage_requests)} storage requests")

            logger.info("🔍 Step 3b: Collecting miner profiles...")
            miner_profiles = await collect_miner_profiles_for_submission(self.db_pool)
            logger.info(f"✅ Collected {len(miner_profiles)} miner profiles")

            # Step 4: CRITICAL VERIFICATION - Ensure we're not submitting empty data
            logger.info("🔍 Step 4: Final data verification before blockchain submission...")

            # Verify miner profiles have actual content
            profiles_with_files = 0
            total_files_in_profiles = 0
            for profile in miner_profiles:
                file_count = profile.get("files_count", 0)
                if file_count > 0:
                    profiles_with_files += 1
                    total_files_in_profiles += file_count

            logger.info("📊 Miner profile content analysis:")
            logger.info(f"   - {len(miner_profiles)} total miner profiles")
            if len(miner_profiles) > 0:
                logger.info(
                    f"   - {profiles_with_files} profiles with files ({profiles_with_files / len(miner_profiles) * 100:.1f}%)"
                )
            else:
                logger.info(f"   - {profiles_with_files} profiles with files (0 total profiles)")
            logger.info(f"   - {total_files_in_profiles} total files in all profiles")

            if profiles_with_files == 0 and total_files_in_profiles == 0:
                logger.warning("⚠️ All miner profiles are empty (no files)")
                logger.warning("   This might be normal if no storage requests were processed")
                logger.warning("   But check if this is expected...")

            logger.info("📊 FINAL DATA SUMMARY:")
            logger.info(f"  - {len(storage_requests)} original storage requests (for closing)")
            logger.info(f"  - {len(miner_profiles)} miner profiles ({profiles_with_files} with files)")
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
                storage_requests,
                miner_profiles,
            )

            await submit_unpin_requests_to_blockchain(
                self.db_pool,
                miner_profiles,
            )

            if success:
                # Mark as completed in database
                logger.info("✅ Blockchain submission successful! Marking as completed in database...")
                await mark_submissions_as_completed(self.db_pool, submitted_requests, submitted_profiles)
                logger.info("✅ Database updated with submission completion")

                # Success summary
                logger.info("🎯 BLOCKCHAIN SUBMISSION SUMMARY:")
                logger.info(f"   ✅ Successfully submitted {len(submitted_profiles)} miner profiles")
                logger.info(f"   ✅ Successfully submitted {len(submitted_requests)} storage requests")
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
            from app.utils.blockchain_submission import (
                submit_health_metrics_to_blockchain,
            )

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

        # should_refresh_node_metrics = self.current_block % self.node_metrics_refresh_interval == 0
        should_refresh_node_metrics = True

        # Build tasks list with conditional node metrics refresh
        tasks = [self.refresh_registration_data(), self.refresh_user_profiles()]

        if should_refresh_node_metrics:
            logger.info(f"📊 Including node metrics refresh (block {self.current_block} % 300 == 0)")
            tasks.insert(1, self.refresh_node_metrics())  # Insert after registration
        else:
            blocks_until_refresh = self.node_metrics_refresh_interval - (
                self.current_block % self.node_metrics_refresh_interval
            )
            logger.info("📊 Skipping node metrics refresh (using cached data)")
            logger.info(f"   Current block: {self.current_block}, next refresh in {blocks_until_refresh} blocks")

        await asyncio.gather(*tasks, return_exceptions=True)

    async def non_validator_workflow(self):
        """Execute non-validator workflow."""
        # Get current position for context
        current_block = self.current_block
        block_position = get_epoch_block_position(current_block)

        logger.info(f"👤 Non-validator can process at any time - current position: {block_position}/99")

        # Initialize epoch data if not done
        if not self.initialization_completed:
            logger.info("🚀 Non-validator: Starting epoch initialization...")
            await self.epoch_initialization()
            self.initialization_completed = True

        # NON-VALIDATOR: Can perform health checks at any time (no blockchain submission deadline)
        if not self.health_checks_completed:
            logger.info(f"🏥 Non-validator: Starting health checks at block {block_position}/99...")
            self.health_checks_completed = await self.perform_health_checks()

        # Submit health metrics to blockchain
        if self.health_checks_completed and not self.health_metrics_submitted:
            logger.info("📊 Non-validator: Submitting health metrics to blockchain...")
            success = await self.submit_health_metrics()
            if success:
                self.health_metrics_submitted = True
                logger.info("✅ Non-validator: Health metrics submitted to blockchain")
            else:
                logger.error("❌ Non-validator: Health metrics submission failed")

        if self.health_checks_completed and not self.health_scores_processed:
            await self.network_self_healing_routine()

        # PERIODIC DATABASE CLEANUP: Run comprehensive miner records cleanup (every 4 hours)
        # Use block position to determine timing - run at specific intervals to avoid validator interference
        cleanup_interval = (
            240
            # Approximately 4 hours (240 blocks * 6 seconds = 1440 seconds = 24 minutes actual)
        )
        if (self.current_block % cleanup_interval == 0) and block_position > 20:  # Avoid early epoch interference
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

        # Wait for end of epoch
        if block_position % 30 == 0:  # Every 30 blocks
            logger.info(
                f"Non-validator: Monitoring network and waiting for next epoch... {99 - block_position} blocks remaining"
            )

    async def validator_workflow(self):
        """Execute validator workflow with SEQUENTIAL processing for security and speed."""
        logger.info("👑 Executing VALIDATOR workflow")

        # Use the current epoch and block from the main loop
        current_block = self.current_block
        block_position = get_epoch_block_position(current_block)

        logger.info(f"Current block position in epoch: {block_position}/99")

        # Phase 1: Initialization (ALWAYS run if not completed)
        if not self.initialization_completed:
            logger.info(f"🚀 Validator starting initialization (block: {block_position}/99)")
            await self.epoch_initialization()
            self.initialization_completed = True

        elif self.initialization_completed and not self.health_checks_completed:
            if block_position <= 25:
                # EARLY EPOCH: Can start health checks OR use previous data
                logger.info(f"🏥 VALIDATOR: Health check decision at block {block_position}/99")

                # Check if we have recent health data to skip checks
                async with self.db_pool.acquire() as conn:
                    health_data_count = await conn.fetchval("""
                        SELECT COUNT(DISTINCT node_id) 
                        FROM miner_epoch_health 
                        WHERE last_activity_at >= NOW() - INTERVAL '2 hours'
                    """)

                    logger.info(f"📊 Found {health_data_count} miners with recent health data (< 2 hours)")

                    if health_data_count >= 400:  # High threshold for validators
                        self.health_checks_completed = True
                        logger.info("✅ VALIDATOR: Health checks marked complete (using previous data)")
                    else:
                        logger.info(
                            f"🏥 VALIDATOR: Starting fresh health checks (insufficient previous data: {health_data_count})"
                        )
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
                                logger.warning("   Insufficient health data - validator proceeding with risks")
                                self.health_checks_completed = True
            else:
                logger.info(f"⏰ VALIDATOR: Too late for health checks (block {block_position}/99)")
                logger.info("   CRITICAL TIMING: Using previous epoch health data only")
                self.health_checks_completed = True
            return

        # Phase 2.5: CRITICAL - Process health scores (transfer epoch health data to miner_stats)
        elif self.health_checks_completed and not self.health_scores_processed:
            logger.info(f"🏥 Processing health scores at block {block_position}/99")
            logger.info("   CRITICAL: Transferring health data from epoch health to miner stats for assignment")

            if block_position <= 15:  # Early in epoch
                if self.assignment_completed:
                    logger.info("🔄 EPOCH START: Resetting assignment state to ensure fresh storage request processing")
                    self.assignment_completed = False

            success = await self.process_health_scores()
            if success:
                self.health_scores_processed = True
                logger.info("✅ SEQUENTIAL: Health scores processed - miner stats updated for assignment")
            else:
                logger.error("❌ Health score processing failed - assignment may use stale data")
                # Proceed anyway to avoid blocking the validator
                self.health_scores_processed = True

            return

        # Phase 3: SEQUENTIAL File Assignment (immediately after self-healing complete)
        elif not self.assignment_completed:
            logger.info(f"📋 Starting file assignment at block {block_position}/99")
            success = await self.assign_files()
            if success:
                self.assignment_completed = True
                logger.info("✅ File assignment completed - starting profiles next")
            else:
                logger.error("❌ File assignment failed - marking complete to prevent infinite loop")
                self.assignment_completed = True
            return

        # Phase 4: SEQUENTIAL Profile Reconstruction
        elif self.assignment_completed and not self.profiles_completed:
            logger.info(f"🔧 Starting profile reconstruction at block {block_position}/99")
            await self.reconstruct_profiles()
            self.profiles_completed = True

        # Phase 5: Submit to blockchain IMMEDIATELY when profiles are ready
        elif self.profiles_completed and not self.submission_completed:
            # Add circuit breaker for late submissions
            if block_position >= 97:
                logger.error(f"❌ CRITICAL: Too late for blockchain submission (block {block_position}/99)")
                return

            if await self.submit_to_blockchain():
                logger.info(f"✅ SECURITY: Blockchain submission completed at block {block_position}/99")
            else:
                logger.error("❌ Blockchain submission failed")

            self.submission_completed = True
            self.blockchain_submitted = True

            return

        # Phase 6: Monitoring after submission (blocks after submission until epoch end)
        elif self.submission_completed and not self.cleanup_completed:
            # Monitor until cleanup phase
            if block_position >= 98:
                await self.epoch_cleanup()
                self.cleanup_completed = True
                logger.info("✅ Cleanup complete!")
            return

        # Handle edge cases and status reporting
        else:
            if not self.initialization_completed:
                logger.info(f"⏳ Waiting for initialization phase (current: {block_position}/99)")
            elif not self.health_checks_completed:
                logger.info(f"⏳ Waiting for health checks completion (current: {block_position}/99)")
            elif not self.health_scores_processed:
                logger.info(f"⏳ Waiting for health score processing completion (current: {block_position}/99)")
            elif not self.assignment_completed:
                logger.info(f"⏳ Waiting for file assignment completion (current: {block_position}/99)")
            elif not self.profiles_completed:
                logger.info(f"⏳ Waiting for profile reconstruction completion (current: {block_position}/99)")
            elif not self.submission_completed:
                logger.info(f"⏳ Waiting for blockchain submission completion (current: {block_position}/99)")
            else:
                logger.info(f"✅ All phases complete - monitoring until epoch end (current: {block_position}/99)")
            return

    async def reset_epoch_state(self):
        """Reset epoch state for new epoch."""
        self.initialization_completed = False
        self.health_checks_completed = False
        self.health_scores_processed = False
        self.assignment_completed = False
        self.profiles_completed = False
        self.submission_completed = False
        self.cleanup_completed = False

        # Legacy state variables (keeping for compatibility)
        self.pinning_completed = False
        self.blockchain_submitted = False
        self.availability_completed = False
        self.health_metrics_submitted = False

        self.waiting_for_next_epoch = False

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
            logger.info("🔗 Connection recovery: Resuming validator processing without waiting")
            self.waiting_for_next_epoch = False
            self.startup_epoch = current_epoch
            return False

        # ENHANCED FIX: If we became validator in this epoch (role transition), allow processing
        # This handles connection lag where we miss the early detection window
        if (
            hasattr(self, "previous_epoch")
            and self.previous_epoch is not None
            and current_epoch > self.previous_epoch
            and self.waiting_for_next_epoch
        ):
            logger.info(f"🎯 Role transition to validator in epoch {current_epoch} at position {block_position}/99")
            logger.info("   Allowing processing despite late detection (connection lag or role transition)")
            self.waiting_for_next_epoch = False
            self.startup_epoch = current_epoch
            return False

        # If this is the first time we're seeing this epoch (true startup)
        if self.startup_epoch is None:
            self.startup_epoch = current_epoch

            # If we're starting after block 10, wait for next epoch (true mid-epoch startup)
            if block_position > 10:
                logger.warning(f"🚨 Validator started mid-epoch at block position {block_position}/99")
                logger.warning("   Validator waiting for next epoch to avoid processing incomplete data")
                self.waiting_for_next_epoch = True
                return True
            else:
                logger.info(f"✅ Validator started early in epoch at block position {block_position}/99")
                logger.info("   Safe for validator to proceed with current epoch processing")
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
                        max_connection_attempts = 5 if self.validator_state_cache["last_known_validator_status"] else 3

                        for attempt in range(max_connection_attempts):
                            try:
                                self.substrate = connect_substrate()
                                logger.info(
                                    f"✅ Substrate connection established (attempt {attempt + 1}/{max_connection_attempts})"
                                )
                                break
                            except Exception as e:
                                if attempt < max_connection_attempts - 1:
                                    wait_time = 2**attempt  # Exponential backoff: 1s, 2s, 4s, 8s, 16s
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

                    # Calculate block position early for logging
                    block_position = get_epoch_block_position(current_block)

                    # Track role transitions for debugging
                    previous_is_validator = getattr(self, "is_validator", None)
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
                    if (
                        self.connection_failures > 0
                        and self.validator_state_cache["last_known_validator_status"] == is_validator
                        and is_validator
                    ):
                        logger.info(f"🔗 Connection recovered: Maintaining validator role in epoch {current_epoch}")
                        logger.info(
                            f"   Validator state preserved across {self.connection_failures} connection failure(s)"
                        )

                    self.epoch_start_block = epoch_start
                    last_epoch = current_epoch

                    logger.info(f"📊 Epoch {current_epoch}, Block {current_block} (position {block_position}/99)")

                    # ENHANCED STARTUP SAFETY: Only apply to validators
                    if is_validator and self.should_wait_for_next_epoch(current_epoch, block_position):
                        if block_position % 10 == 0:  # Log every 10 blocks to avoid spam
                            logger.info(
                                f"⏳ Validator waiting for next epoch (started mid-epoch at position {block_position}/99)"
                            )
                            logger.info("   This prevents validator processing with incomplete epoch data")
                        await asyncio.sleep(self.block_check_interval)
                        continue

                    # Non-validators can always start immediately
                    if not is_validator and block_position % 20 == 0:  # Periodic status for non-validators
                        logger.info("👤 Non-validator processing: Can start immediately at any block position")

                    # Log monitoring frequency periodically
                    if block_position % 10 == 0:  # Every 10 blocks
                        logger.info(f"⏰ Monitoring every {self.block_check_interval}s (every block)")
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
                    if any(
                        error_type in str(e).lower()
                        for error_type in [
                            "jsondecodeerror",
                            "websocket",
                            "broken pipe",
                            "connection",
                            "timeout",
                        ]
                    ):
                        logger.warning("Connection issue detected - will retry with exponential backoff")

                        # Close existing connection
                        if hasattr(self, "substrate") and self.substrate:
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
        tables_to_clean = [
            "pinning_requests",
            "parsed_cids",
            "pending_miner_profile",
            "pending_submissions",
            "pending_user_profile",
        ]

        try:
            if not self.db_pool:
                logger.error("Database pool not initialized")
                return False

            async with self.db_pool.acquire() as conn:
                # PRESERVE health data and node metrics for performance optimization
                logger.info("✅ PRESERVING miner_epoch_health data for validator performance")
                logger.info("   Previous epoch health data allows validators to skip 3+ hour health checks")
                logger.info("✅ PRESERVING node_metrics data (only refreshed every 300 blocks)")
                logger.info("   Node metrics don't change frequently, saving processing overhead")
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
                "✅ Epoch table cleanup completed (preserved health data + node metrics, ready for chain refetch)"
            )
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
        await network_self_healing_processor.main()
        await self.wait_for_queues_empty(
            ["network_self_healing"],
            300,
        )

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
                        total = assignment_stats["total_files"]
                        assigned = assignment_stats["assigned_files"]
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
            if hasattr(self, "epoch_start_time"):
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
                    "SELECT COUNT(*) FROM registration WHERE node_type = 'StorageMiner' AND status = 'active'"
                )

                logger.info(
                    f"📊 Health data before cleanup: {total_health_before:,} records for {registered_miners:,} miners"
                )

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
                reduction_pct = (reduction / total_health_before * 100) if total_health_before > 0 else 0

                # Verify data preservation
                recent_miners = await conn.fetchval("""
                    SELECT COUNT(DISTINCT node_id) FROM miner_epoch_health 
                    WHERE last_activity_at >= NOW() - INTERVAL '24 hours'
                """)

                coverage_pct = (recent_miners / registered_miners * 100) if registered_miners > 0 else 0

                logger.info("✅ Health data cleanup completed:")
                logger.info(f"   📊 Records: {total_health_before:,} → {total_health_after:,} (-{reduction:,})")
                logger.info(f"   💾 Space reduction: {reduction_pct:.1f}%")
                logger.info(
                    f"   🎯 Coverage preserved: {recent_miners:,}/{registered_miners:,} miners ({coverage_pct:.1f}%)"
                )

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
                cleanup_stats = {
                    "inactive_registrations": 0,
                    "old_node_metrics": 0,
                    "orphaned_miner_stats": 0,
                    "old_system_events": 0,
                }

                # Helper function to check if table exists
                async def table_exists(table_name: str) -> bool:
                    try:
                        result = await conn.fetchval(
                            """
                            SELECT EXISTS (
                                SELECT 1 FROM information_schema.tables 
                                WHERE table_name = $1
                            )
                        """,
                            table_name,
                        )
                        return result
                    except Exception:
                        return False

                # Get initial counts for reporting (with table existence checks)
                total_registrations = await conn.fetchval(
                    "SELECT COUNT(*) FROM registration WHERE node_type = 'StorageMiner'"
                )

                # Check if optional tables exist before querying
                node_metrics_exists = await table_exists("node_metrics")
                total_node_metrics = (
                    await conn.fetchval("SELECT COUNT(*) FROM node_metrics") if node_metrics_exists else 0
                )

                miner_stats_exists = await table_exists("miner_stats")
                total_miner_stats = await conn.fetchval("SELECT COUNT(*) FROM miner_stats") if miner_stats_exists else 0

                system_events_exists = await table_exists("system_events")
                total_system_events = (
                    await conn.fetchval("SELECT COUNT(*) FROM system_events") if system_events_exists else 0
                )

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
                    cleanup_stats["inactive_registrations"] = int(result.split()[-1])

                    # 2. Clean up old node metrics (>7 days) - only if table exists
                    if node_metrics_exists:
                        result = await conn.execute("""
                            DELETE FROM node_metrics 
                            WHERE created_at < NOW() - INTERVAL '7 days'
                        """)
                        cleanup_stats["old_node_metrics"] = int(result.split()[-1])
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
                        cleanup_stats["orphaned_miner_stats"] = int(result.split()[-1])
                    else:
                        logger.info("   ⚠️ Skipping miner_stats cleanup - table doesn't exist")

                    # 4. Clean up old system events (>30 days) - only if table exists
                    if system_events_exists:
                        result = await conn.execute("""
                            DELETE FROM system_events 
                            WHERE created_at < NOW() - INTERVAL '30 days'
                        """)
                        cleanup_stats["old_system_events"] = int(result.split()[-1])
                    else:
                        logger.info("   ⚠️ Skipping system_events cleanup - table doesn't exist")

                # Get final counts (with table existence checks)
                final_registrations = await conn.fetchval(
                    "SELECT COUNT(*) FROM registration WHERE node_type = 'StorageMiner'"
                )
                final_node_metrics = (
                    await conn.fetchval("SELECT COUNT(*) FROM node_metrics") if node_metrics_exists else 0
                )
                final_miner_stats = await conn.fetchval("SELECT COUNT(*) FROM miner_stats") if miner_stats_exists else 0
                final_system_events = (
                    await conn.fetchval("SELECT COUNT(*) FROM system_events") if system_events_exists else 0
                )

                total_cleaned = sum(cleanup_stats.values())

                logger.info("✅ Miner records cleanup completed:")
                logger.info(f"   🗑️  Inactive registrations: {cleanup_stats['inactive_registrations']:,}")
                logger.info(f"   🗑️  Old node metrics: {cleanup_stats['old_node_metrics']:,}")
                logger.info(f"   🗑️  Orphaned miner stats: {cleanup_stats['orphaned_miner_stats']:,}")
                logger.info(f"   🗑️  Old system events: {cleanup_stats['old_system_events']:,}")
                logger.info(f"   📊 Total records cleaned: {total_cleaned:,}")

                logger.info("📊 Database after cleanup:")
                logger.info(
                    f"   - Miner registrations: {final_registrations:,} (-{total_registrations - final_registrations:,})"
                )
                logger.info(f"   - Node metrics: {final_node_metrics:,} (-{total_node_metrics - final_node_metrics:,})")
                logger.info(f"   - Miner stats: {final_miner_stats:,} (-{total_miner_stats - final_miner_stats:,})")
                logger.info(
                    f"   - System events: {final_system_events:,} (-{total_system_events - final_system_events:,})"
                )

                # Calculate space savings
                total_reduction = (
                    total_registrations
                    - final_registrations
                    + total_node_metrics
                    - final_node_metrics
                    + total_miner_stats
                    - final_miner_stats
                    + total_system_events
                    - final_system_events
                )

                if total_reduction > 0:
                    logger.info(f"💾 Database optimization: {total_reduction:,} total records removed")
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
