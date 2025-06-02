#!/usr/bin/env python3
"""
Epoch Orchestrator

This is the main orchestrator that manages the entire IPFS Service Validator application
based on whether we are the current epoch validator or not.

Epoch Structure (100 blocks):
- Block 0-10: Initialization (registration, node metrics, user profiles)
- Block 11-50: Pinning requests processing (validator only)
- Block 51-80: File assignment and health checks
- Block 81-95: Profile reconstruction
- Block 96-99: Finalization and preparation for next epoch

Non-Validator Mode:
- Refresh data at epoch start
- Perform health checks and submit to chain
- Wait for next epoch

Validator Mode:
- Refresh data at epoch start
- Process pinning requests (blocks 11-50)
- Assign files and perform health checks
- Reconstruct profiles (must finish by block 95)
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
        
        # Safety mechanism for mid-epoch startup
        self.startup_epoch = None
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
        """Record successful connection, reset failure count."""
        self.connection_failures = 0
        self.last_failure_time = 0
    
    def record_connection_failure(self):
        """Record connection failure, increment failure count."""
        self.connection_failures += 1
        self.last_failure_time = time.time()
        logger.warning(f"Connection failure #{self.connection_failures}, next attempt in {self.get_backoff_delay()}s")
    
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
        """Assign files to miners using the simple, reliable processor."""
        logger.info("📋 Assigning files to miners (simple approach)")
        
        # Try subprocess approach first (cleaner isolation)
        success = self.run_processor(
            'simple_file_assignment_processor.py',
            'Simple file assignment'
        )
        
        if success:
            # Wait for file assignment consumer to process
            await self.wait_for_queues_empty(['file_assignment_processing'], 600)
            logger.info("✅ File assignment completed via subprocess")
            return True
        else:
            # Fallback to direct integration if subprocess fails
            logger.warning("⚠️ Subprocess file assignment failed, trying direct integration...")
            direct_success = await self.assign_files_direct()
            
            if direct_success:
                logger.info("✅ File assignment completed via direct integration")
                return True
            else:
                logger.error("❌ Both subprocess and direct file assignment failed")
                return False
    
    async def assign_files_direct(self) -> bool:
        """
        Assign files to miners using direct integration (not subprocess).
        This avoids database pool sharing issues.
        """
        logger.info("📋 Assigning files to miners (direct integration)")
        
        try:
            # Import the processor class
            from rabbitmq.simple_file_assignment_processor import SimpleFileAssignmentProcessor
            
            # Create processor instance that will use our existing db_pool
            processor = SimpleFileAssignmentProcessor()
            
            # Set the database pool directly (avoid re-initialization)
            processor.db_pool = self.db_pool
            
            # Initialize only the non-database parts
            try:
                # Initialize Substrate connection
                from app.utils.config import NODE_URL
                from substrateinterface import SubstrateInterface
                node_url = NODE_URL or 'wss://rpc.hippius.network'
                processor.substrate = SubstrateInterface(url=node_url)
                logger.info(f"✅ Connected to Substrate at {node_url}")
                
                # Initialize RabbitMQ
                import aio_pika
                rabbitmq_url = os.getenv('RABBITMQ_URL', 'amqp://localhost')
                processor.rabbitmq_connection = await aio_pika.connect_robust(rabbitmq_url)
                processor.rabbitmq_channel = await processor.rabbitmq_connection.channel()
                
                # Declare queue
                await processor.rabbitmq_channel.declare_queue(
                    processor.queue_name, 
                    durable=True
                )
                logger.info(f"✅ RabbitMQ connected and queue declared")
                
                # Run the file assignment process
                await processor.process_file_assignments()
                
                logger.info("✅ Direct file assignment completed successfully")
                
                # Cleanup processor resources (but not database pool)
                try:
                    if processor.rabbitmq_connection and not processor.rabbitmq_connection.is_closed:
                        await processor.rabbitmq_connection.close()
                    if processor.substrate:
                        processor.substrate.close()
                except Exception as e:
                    logger.warning(f"⚠️ Error during processor cleanup: {e}")
                
                return True
                
            except Exception as e:
                logger.error(f"❌ Error during direct file assignment: {e}")
                return False
                
        except Exception as e:
            logger.error(f"❌ Error importing or setting up direct file assignment: {e}")
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
        """Reconstruct user and miner profiles using simple, reliable logic."""
        logger.info("🔧 Reconstructing profiles (simple approach)")
        
        try:
            # Import the simple profile rebuild function
            from app.utils.blockchain_submission import rebuild_user_profiles_simple
            
            # Rebuild user profiles from file assignments
            user_profiles_rebuilt = await rebuild_user_profiles_simple(self.db_pool)
            logger.info(f"✅ Rebuilt {user_profiles_rebuilt} user profiles from file assignments")
            
            # Process user profiles first
            user_success = self.run_processor(
                'user_profile_reconstruction_processor.py',
                'User profile reconstruction'
            )
            
            # Process miner profiles
            miner_success = self.run_processor(
                'miner_profile_reconstruction_processor.py', 
                'Miner profile reconstruction'
            )
            
            # Wait for profile reconstruction queues to empty
            if user_success or miner_success:
                await self.wait_for_queues_empty([
                    'user_profile_reconstruction',
                    'miner_profile_reconstruction'
                ], 300)
            
            success = user_success and miner_success
            
            if success:
                logger.info("✅ Profile reconstruction completed successfully")
            else:
                logger.warning("⚠️ Profile reconstruction had some issues but simple rebuild completed")
                # Consider it successful if we at least rebuilt user profiles
                success = user_profiles_rebuilt > 0
            
            return success
            
        except Exception as e:
            logger.error(f"❌ Error during profile reconstruction: {e}")
            return False
    
    async def submit_to_blockchain(self) -> bool:
        """Submit reconstructed profiles and storage requests to the blockchain."""
        logger.info("📤 Submitting profiles and storage requests to blockchain")
        
        try:
            from app.utils.blockchain_submission import (
                collect_storage_requests_for_submission,
                collect_miner_profiles_for_submission,
                call_update_pin_and_storage_requests,
                mark_submissions_as_completed
            )
            
            if not self.db_pool:
                logger.error("Database pool not initialized")
                return False
            
            # Collect data for submission
            logger.info("Collecting storage requests and miner profiles for submission...")
            
            storage_requests = await collect_storage_requests_for_submission(self.db_pool)
            miner_profiles = await collect_miner_profiles_for_submission(self.db_pool)
            
            if not storage_requests and not miner_profiles:
                logger.info("No data to submit to blockchain")
                return True
            
            logger.info(f"Prepared for submission:")
            logger.info(f"  - {len(storage_requests)} original storage requests (for closing)")
            logger.info(f"  - {len(miner_profiles)} miner profiles")
            
            # Submit to blockchain (this function will try different sizes if needed)
            success, submitted_requests, submitted_profiles = call_update_pin_and_storage_requests(storage_requests, miner_profiles)
            
            if success:
                # Mark only the actually submitted items as completed in database
                await mark_submissions_as_completed(self.db_pool, submitted_requests, submitted_profiles)
                
                logger.info(f"✅ Successfully submitted to blockchain and updated database")
                logger.info(f"   - Submitted: {len(submitted_requests)}/{len(storage_requests)} original storage requests (for closing)")
                logger.info(f"   - Submitted: {len(submitted_profiles)}/{len(miner_profiles)} miner profiles")
                
                return True
            else:
                logger.error("❌ Failed to submit to blockchain")
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
        
        # Initialize epoch data
        if not self.initialization_completed:
            success = await self.epoch_initialization()
            if success:
                self.initialization_completed = True
            else:
                logger.error("Failed to initialize epoch data")
                return
        
        # Periodically refresh user profiles to stay current (every ~20 blocks)
        current_block = self.current_block
        block_position = get_epoch_block_position(current_block)
        if block_position % 20 == 0 and block_position > 10:  # Every 20 blocks after initialization
            logger.info("🔄 Refreshing user profiles to stay current with network changes")
            await self.refresh_user_profiles()
        
        # Perform health checks and submit to chain
        if not self.health_checks_completed:
            success = await self.perform_health_checks()
            if success:
                self.health_checks_completed = True
                logger.info("✅ Health checks completed")
            else:
                logger.error("❌ Health checks failed")
        
        # Run availability maintenance (non-validators can help maintain the network)
        if self.health_checks_completed and not self.availability_completed:
            success = await self.run_availability_maintenance()
            if success:
                self.availability_completed = True
                logger.info("✅ File availability maintenance completed")
            else:
                logger.warning("⚠️ File availability maintenance failed")
        
        # Submit health metrics to blockchain
        if self.health_checks_completed and not self.health_metrics_submitted:
            success = await self.submit_health_metrics()
            if success:
                self.health_metrics_submitted = True
                logger.info("✅ Health metrics submitted to blockchain")
            else:
                logger.error("❌ Health metrics submission failed")
        
        # Wait for end of epoch
        logger.info("⏳ Waiting for end of epoch...")
    
    async def validator_workflow(self):
        """Execute validator workflow."""
        logger.info("👑 Executing VALIDATOR workflow")
        
        # Use the current epoch and block from the main loop
        current_epoch = self.current_epoch
        current_block = self.current_block
        block_position = get_epoch_block_position(current_block)
        
        logger.info(f"Current block position in epoch: {block_position}/99")
        
        # Phase 1: Initialization (blocks 0-10)
        if block_position <= 10 and not self.initialization_completed:
            # Step 1: Run network self-healing routine (validator only)
            if not getattr(self, 'self_healing_completed', False):
                logger.info("🛠️ Starting epoch with network self-healing routine")
                healing_success = await self.network_self_healing_routine()
                self.self_healing_completed = True
                
                if healing_success:
                    logger.info("✅ Network self-healing completed - proceeding with normal initialization")
                else:
                    logger.warning("⚠️ Network self-healing had issues - proceeding with normal initialization")
            
            # Step 2: Normal epoch initialization
            success = await self.epoch_initialization()
            if success:
                self.initialization_completed = True
            else:
                logger.error("Failed to initialize epoch data")
                return
        
        # Phase 2: Pinning requests (blocks 11-50)
        elif 11 <= block_position <= 50:
            if not self.pinning_completed:
                # Process pinning requests periodically
                success = await self.process_pinning_requests()
                if success:
                    # Also process the files from pinning requests
                    await self.process_pinning_files()
                
                # Don't mark as completed until block 50 to allow periodic processing
                if block_position >= 45:
                    self.pinning_completed = True
        
        # Phase 3: File assignment and health checks (blocks 51-80)
        elif 51 <= block_position <= 80:
            # CATCHUP: Process any late-arriving pinning requests/files from after block 50
            if block_position == 51 and not getattr(self, 'catchup_processing_completed', False):
                logger.info("🔄 Catchup processing: handling any files that arrived after pinning phase")
                
                # Process any remaining pinning requests
                logger.info("   → Processing late pinning requests...")
                await self.process_pinning_requests()
                
                # Process any remaining files
                logger.info("   → Processing late pinning files...")
                await self.process_pinning_files()
                
                self.catchup_processing_completed = True
                logger.info("✅ Catchup processing completed")
            
            if not self.assignment_completed:
                success = await self.assign_files()
                if success:
                    self.assignment_completed = True
            
            # Run availability maintenance after file assignment
            if self.assignment_completed and not self.availability_completed:
                success = await self.run_availability_maintenance()
                if success:
                    self.availability_completed = True
                    logger.info("✅ File availability maintenance completed")
                else:
                    logger.warning("⚠️ File availability maintenance failed")
            
            if not self.health_checks_completed:
                success = await self.perform_health_checks()
                if success:
                    self.health_checks_completed = True
            
            # Submit health metrics to blockchain
            if self.health_checks_completed and not self.health_metrics_submitted:
                success = await self.submit_health_metrics()
                if success:
                    self.health_metrics_submitted = True
                    logger.info("✅ Health metrics submitted to blockchain")
                else:
                    logger.error("❌ Health metrics submission failed")
        
        # Phase 4: Profile reconstruction (blocks 81-95)
        elif 81 <= block_position <= 95:
            # CRITICAL: Refetch user profiles before reconstruction to include new files
            if block_position == 81 and not getattr(self, 'user_profiles_refreshed_for_reconstruction', False):
                logger.info("🔄 Refetching user profiles before reconstruction to include new files from storage requests")
                refresh_success = await self.refresh_user_profiles()
                if refresh_success:
                    self.user_profiles_refreshed_for_reconstruction = True
                    logger.info("✅ User profiles refreshed with latest data including new storage request files")
                else:
                    logger.warning("⚠️ Failed to refresh user profiles - proceeding with existing data")
            
            if not self.profiles_reconstructed:
                success = await self.reconstruct_profiles()
                if success:
                    self.profiles_reconstructed = True
                    logger.info("✅ All profile reconstruction completed")
                else:
                    logger.error("❌ Profile reconstruction failed - must complete before block 95!")
            
            # Submit to blockchain after reconstruction (must happen before block 95)
            if self.profiles_reconstructed and not self.blockchain_submitted:
                if block_position <= 93:  # Leave some buffer time
                    logger.info("📤 Submitting reconstructed profiles to blockchain...")
                    submission_success = await self.submit_to_blockchain()
                    if submission_success:
                        self.blockchain_submitted = True
                        logger.info("✅ Blockchain submission completed successfully")
                    else:
                        logger.error("❌ Blockchain submission failed - will retry next block")
                else:
                    logger.warning("⚠️ Too late in epoch to submit to blockchain safely")
        
        # Phase 5: Finalization (blocks 96-99)
        elif 96 <= block_position <= 99:
            logger.info("🏁 Finalization phase - preparing for next epoch")
            logger.info(f"   Epoch {current_epoch} Summary:")
            logger.info(f"   ✅ Self-Healing: {getattr(self, 'self_healing_completed', False)}")
            logger.info(f"   ✅ Initialization: {self.initialization_completed}")
            logger.info(f"   ✅ Pinning: {self.pinning_completed}")
            logger.info(f"   ✅ Catchup Processing: {getattr(self, 'catchup_processing_completed', False)}")
            logger.info(f"   ✅ Assignment: {self.assignment_completed}")
            logger.info(f"   ✅ Availability: {self.availability_completed}")
            logger.info(f"   ✅ Health Checks: {self.health_checks_completed}")
            logger.info(f"   ✅ Health Metrics Submitted: {self.health_metrics_submitted}")
            logger.info(f"   ✅ User Profiles Refreshed: {getattr(self, 'user_profiles_refreshed_for_reconstruction', False)}")
            logger.info(f"   ✅ Profile Reconstruction: {self.profiles_reconstructed}")
            logger.info(f"   ✅ Blockchain Submission: {self.blockchain_submitted}")
            
            if not self.health_metrics_submitted and self.health_checks_completed:
                logger.warning("⚠️ Health checks completed but metrics not submitted to blockchain!")
            
            if not self.blockchain_submitted and self.profiles_reconstructed:
                logger.warning("⚠️ Profile reconstruction completed but blockchain submission failed!")
                logger.warning("   This may affect validator rewards for this epoch.")
    
    async def reset_epoch_state(self):
        """Reset state for new epoch."""
        logger.info("🔄 Resetting epoch state for new epoch")
        
        self.initialization_completed = False
        self.self_healing_completed = False  # Reset self-healing state
        self.pinning_completed = False
        self.assignment_completed = False
        self.health_checks_completed = False
        self.health_metrics_submitted = False
        self.availability_completed = False  # Reset availability state
        self.profiles_reconstructed = False
        self.blockchain_submitted = False
        self.user_profiles_refreshed_for_reconstruction = False  # Reset user profile refresh flag
        self.catchup_processing_completed = False  # Reset catchup processing flag
        
        # Reset startup safety mechanism
        self.waiting_for_next_epoch = False
    
    def should_wait_for_next_epoch(self, current_epoch: int, block_position: int) -> bool:
        """
        Determine if we should wait for the next epoch before starting processing.
        This prevents processing with incomplete data when starting mid-epoch.
        
        Args:
            current_epoch: Current epoch number
            block_position: Current position in epoch (0-99)
            
        Returns:
            True if we should wait, False if we can proceed
        """
        # If this is the first time we're seeing this epoch (startup)
        if self.startup_epoch is None:
            self.startup_epoch = current_epoch
            
            # If we're starting after block 10, wait for next epoch
            if block_position > 10:
                logger.warning(f"🚨 Application started mid-epoch at block position {block_position}/99")
                logger.warning(f"   Waiting for next epoch to avoid processing incomplete data")
                self.waiting_for_next_epoch = True
                return True
            else:
                logger.info(f"✅ Application started early in epoch at block position {block_position}/99")
                logger.info(f"   Safe to proceed with current epoch processing")
                return False
        
        # If we were waiting and we're now in a new epoch, we can proceed
        if self.waiting_for_next_epoch and current_epoch > self.startup_epoch:
            logger.info(f"🎯 New epoch {current_epoch} started - resuming normal processing")
            self.waiting_for_next_epoch = False
            return False
        
        # Continue waiting if we're still in the startup epoch
        return self.waiting_for_next_epoch
    
    async def run(self):
        """Main orchestrator loop."""
        logger.info("🎯 Starting Epoch Orchestrator")
        logger.info("🛡️ Safety mechanism: Will wait for next epoch if starting mid-epoch (after block 10)")
        
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
                    
                    # Ensure we have a substrate connection
                    if self.substrate is None:
                        logger.info("🔗 Creating new substrate connection...")
                        self.substrate = connect_substrate()
                    
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
                        await self.reset_epoch_state()
                        
                        # Update startup epoch tracking for new epoch
                        if self.startup_epoch == last_epoch:
                            self.startup_epoch = current_epoch
                    
                    # Update state
                    self.current_epoch = current_epoch
                    self.current_block = current_block
                    self.is_validator = is_validator
                    self.epoch_start_block = epoch_start
                    last_epoch = current_epoch
                    
                    block_position = get_epoch_block_position(current_block)
                    
                    logger.info(f"📊 Epoch {current_epoch}, Block {current_block} (position {block_position}/99)")
                    logger.info(f"🎭 Role: {'VALIDATOR' if is_validator else 'NON-VALIDATOR'}")
                    
                    # Check if we should wait for next epoch (startup safety)
                    if self.should_wait_for_next_epoch(current_epoch, block_position):
                        if block_position % 10 == 0:  # Log every 10 blocks to avoid spam
                            logger.info(f"⏳ Waiting for next epoch (started mid-epoch at position {block_position}/99)")
                            logger.info(f"   This prevents processing incomplete data from partial epoch")
                        await asyncio.sleep(self.block_check_interval)
                        continue
                    
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
            'miner_epoch_health', 
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
                for table in tables_to_clean:
                    try:
                        # Delete all records from the table
                        result = await conn.execute(f"DELETE FROM {table}")
                        deleted_count = result.split()[-1] if result else "0"
                        logger.info(f"✅ Cleaned table '{table}': {deleted_count} records deleted")
                    except Exception as e:
                        # Some tables might not exist, which is okay
                        logger.warning(f"⚠️ Could not clean table '{table}': {e}")
            
            logger.info("✅ Epoch table cleanup completed")
            return True
            
        except Exception as e:
            logger.error(f"❌ Epoch table cleanup failed: {e}")
            return False

    async def network_self_healing_routine(self) -> bool:
        """
        Automatic network self-healing routine.
        Runs at epoch start when we are the validator to fix any assignment/profile issues.
        This ensures the network maintains health without manual intervention.
        """
        logger.info("🛠️ Starting automatic network self-healing routine")
        logger.info("   This fixes empty assignments, profile issues, and data consistency")
        
        try:
            if not self.db_pool:
                logger.error("Database pool not initialized for self-healing")
                return False
            
            # 1. ASSESS CURRENT HEALTH
            logger.info("📊 Assessing network health...")
            
            async with self.db_pool.acquire() as conn:
                # Check assignment coverage
                assignment_stats = await conn.fetchrow("""
                    SELECT 
                        COUNT(*) as total_files,
                        COUNT(CASE WHEN miner1 IS NOT NULL OR miner2 IS NOT NULL OR miner3 IS NOT NULL 
                                   OR miner4 IS NOT NULL OR miner5 IS NOT NULL THEN 1 END) as files_with_miners,
                        COUNT(CASE WHEN miner1 IS NULL AND miner2 IS NULL AND miner3 IS NULL 
                                   AND miner4 IS NULL AND miner5 IS NULL THEN 1 END) as empty_assignments
                    FROM file_assignments
                """)
                
                # Check profile coverage
                profile_stats = await conn.fetchrow("""
                    SELECT 
                        COUNT(*) as total_profiles,
                        COUNT(CASE WHEN status = 'published' THEN 1 END) as published_profiles,
                        COUNT(CASE WHEN files_count = 0 OR files_count IS NULL THEN 1 END) as zero_file_profiles
                    FROM pending_user_profile
                """)
                
                total_files = assignment_stats['total_files'] or 0
                files_with_miners = assignment_stats['files_with_miners'] or 0
                empty_assignments = assignment_stats['empty_assignments'] or 0
                zero_file_profiles = profile_stats['zero_file_profiles'] or 0
                
                assignment_coverage = (files_with_miners / total_files * 100) if total_files > 0 else 100
                
                logger.info(f"📁 Assignment health: {files_with_miners}/{total_files} files have miners ({assignment_coverage:.1f}%)")
                logger.info(f"📁 Empty assignments: {empty_assignments}")
                logger.info(f"👤 Zero-file profiles: {zero_file_profiles}")
                
                # Determine if healing is needed
                needs_assignment_healing = assignment_coverage < 95 or empty_assignments > 5
                needs_profile_healing = zero_file_profiles > 0
                
                if not needs_assignment_healing and not needs_profile_healing:
                    logger.info("✅ Network health is good - no healing needed")
                    return True
                
                logger.info(f"🚨 Network needs healing:")
                logger.info(f"   Assignment healing: {'YES' if needs_assignment_healing else 'NO'}")
                logger.info(f"   Profile healing: {'YES' if needs_profile_healing else 'NO'}")
            
            # 2. FIX EMPTY ASSIGNMENTS
            if needs_assignment_healing:
                logger.info("🔧 Fixing empty file assignments...")
                
                # Get reliable miners for assignment
                reliable_miners = await self.get_reliable_miners_for_healing()
                if not reliable_miners:
                    logger.warning("⚠️ No reliable miners available for healing")
                else:
                    logger.info(f"✅ Found {len(reliable_miners)} reliable miners for healing")
                    
                    # Fix empty assignments
                    fixed_count = await self.fix_empty_assignments(reliable_miners)
                    logger.info(f"✅ Fixed {fixed_count} empty file assignments")
            
            # 3. REBUILD PROFILES
            if needs_profile_healing:
                logger.info("🔧 Rebuilding user profiles from file assignments...")
                
                # Use the simple profile rebuild function
                from app.utils.blockchain_submission import rebuild_user_profiles_simple
                rebuilt_profiles = await rebuild_user_profiles_simple(self.db_pool)
                logger.info(f"✅ Rebuilt {rebuilt_profiles} user profiles")
            
            # 4. VERIFY HEALING SUCCESS
            logger.info("🔍 Verifying healing results...")
            
            async with self.db_pool.acquire() as conn:
                # Re-check assignment coverage
                post_heal_stats = await conn.fetchrow("""
                    SELECT 
                        COUNT(*) as total_files,
                        COUNT(CASE WHEN miner1 IS NOT NULL OR miner2 IS NOT NULL OR miner3 IS NOT NULL 
                                   OR miner4 IS NOT NULL OR miner5 IS NOT NULL THEN 1 END) as files_with_miners,
                        COUNT(CASE WHEN miner1 IS NULL AND miner2 IS NULL AND miner3 IS NULL 
                                   AND miner4 IS NULL AND miner5 IS NULL THEN 1 END) as empty_assignments
                    FROM file_assignments
                """)
                
                post_heal_coverage = (post_heal_stats['files_with_miners'] / post_heal_stats['total_files'] * 100) if post_heal_stats['total_files'] > 0 else 100
                
                logger.info(f"📊 Post-healing assignment coverage: {post_heal_coverage:.1f}%")
                logger.info(f"📊 Remaining empty assignments: {post_heal_stats['empty_assignments']}")
                
                if post_heal_coverage >= 95 and post_heal_stats['empty_assignments'] <= 5:
                    logger.info("✅ Network self-healing successful!")
                    return True
                else:
                    logger.warning("⚠️ Network self-healing partially successful")
                    return True  # Still continue with epoch processing
            
        except Exception as e:
            logger.error(f"❌ Error during network self-healing: {e}")
            logger.exception("Full traceback:")
            # Don't fail the epoch if healing fails - just log and continue
            return True
    
    async def get_reliable_miners_for_healing(self) -> List[Dict[str, Any]]:
        """Get reliable miners for the healing routine."""
        try:
            async with self.db_pool.acquire() as conn:
                # Use SQL INTERVAL instead of Python datetime to avoid parameter issues
                miners = await conn.fetch("""
                    SELECT 
                        r.node_id,
                        r.ipfs_peer_id,
                        r.registered_at,
                        COALESCE(nm.ipfs_storage_max, 1000000000) as storage_max,
                        COALESCE(nm.ipfs_repo_size, 0) as storage_used,
                        COALESCE(ms.health_score, 100) as health_score
                    FROM registration r
                    LEFT JOIN (
                        SELECT DISTINCT ON (miner_id) 
                            miner_id, ipfs_storage_max, ipfs_repo_size
                        FROM node_metrics 
                        ORDER BY miner_id, block_number DESC
                    ) nm ON r.node_id = nm.miner_id
                    LEFT JOIN miner_stats ms ON r.node_id = ms.node_id
                    WHERE r.node_type = 'StorageMiner' 
                      AND r.status = 'active'
                      AND r.registered_at <= NOW() - INTERVAL '1 day'
                      AND COALESCE(ms.health_score, 100) >= 50
                    ORDER BY RANDOM()
                """)
                
                # Filter for capacity (10MB minimum available)
                reliable_miners = []
                for miner in miners:
                    available_space = miner['storage_max'] - miner['storage_used']
                    if available_space > 10_000_000:  # At least 10MB available
                        reliable_miners.append({
                            'node_id': miner['node_id'],
                            'ipfs_peer_id': miner['ipfs_peer_id'],
                            'health_score': miner['health_score'],
                            'available_space': available_space
                        })
                
                return reliable_miners
                
        except Exception as e:
            logger.error(f"❌ Error getting reliable miners for healing: {e}")
            return []
    
    async def fix_empty_assignments(self, reliable_miners: List[Dict[str, Any]]) -> int:
        """Fix files with empty miner assignments."""
        try:
            async with self.db_pool.acquire() as conn:
                # Get files with empty assignments
                empty_files = await conn.fetch("""
                    SELECT 
                        fa.cid,
                        fa.owner,
                        f.size,
                        fa.miner1, fa.miner2, fa.miner3, fa.miner4, fa.miner5
                    FROM file_assignments fa
                    JOIN files f ON fa.cid = f.cid
                    WHERE (fa.miner1 IS NULL OR fa.miner2 IS NULL OR fa.miner3 IS NULL 
                           OR fa.miner4 IS NULL OR fa.miner5 IS NULL)
                    AND f.size IS NOT NULL
                    ORDER BY f.size ASC
                    LIMIT 100
                """)
                
                if not empty_files:
                    return 0
                
                logger.info(f"🔧 Fixing {len(empty_files)} files with empty assignments")
                
                fixed_count = 0
                
                for file_info in empty_files:
                    try:
                        cid = file_info['cid']
                        file_size = file_info['size'] or 0
                        
                        # Get current assignments
                        current_miners = [
                            file_info['miner1'], file_info['miner2'], file_info['miner3'],
                            file_info['miner4'], file_info['miner5']
                        ]
                        assigned_miners = [m for m in current_miners if m is not None]
                        empty_slots = 5 - len(assigned_miners)
                        
                        if empty_slots <= 0:
                            continue
                        
                        # Find suitable miners for this file size
                        safety_margin = int(file_size * 0.2)
                        required_space = file_size + safety_margin
                        
                        suitable_miners = [
                            m for m in reliable_miners 
                            if m['node_id'] not in assigned_miners and m['available_space'] >= required_space
                        ]
                        
                        if not suitable_miners:
                            logger.debug(f"No suitable miners for file {cid[:16]}... (size: {file_size:,})")
                            continue
                        
                        # Select miners (round-robin style)
                        selected_miners = suitable_miners[:empty_slots]
                        
                        # Update the assignments
                        new_assignments = current_miners.copy()
                        selected_index = 0
                        
                        for i in range(5):
                            if new_assignments[i] is None and selected_index < len(selected_miners):
                                new_assignments[i] = selected_miners[selected_index]['node_id']
                                selected_index += 1
                        
                        # Update database
                        await conn.execute("""
                            UPDATE file_assignments 
                            SET miner1 = $2, miner2 = $3, miner3 = $4, miner4 = $5, miner5 = $6
                            WHERE cid = $1
                        """, cid, new_assignments[0], new_assignments[1], new_assignments[2], 
                             new_assignments[3], new_assignments[4])
                        
                        fixed_count += 1
                        
                        logger.debug(f"Fixed assignments for {cid[:16]}... - added {len(selected_miners)} miners")
                        
                    except Exception as e:
                        logger.error(f"Error fixing assignments for file {file_info.get('cid', 'unknown')}: {e}")
                        continue
                
                return fixed_count
                
        except Exception as e:
            logger.error(f"❌ Error fixing empty assignments: {e}")
            return 0


async def main():
    """Main entry point."""
    orchestrator = EpochOrchestrator()
    await orchestrator.run()


if __name__ == "__main__":
    asyncio.run(main()) 