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
    connect_substrate
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
        self.epoch_initialized = False
        self.pinning_requests_processed = False
        self.files_assigned = False
        self.health_checks_completed = False
        self.profiles_reconstructed = False
        
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
        """Assign files to miners."""
        logger.info("📋 Assigning files to miners")
        
        success = self.run_processor(
            'file_assignment_processor.py',
            'File assignment'
        )
        
        if success:
            # Wait for file assignment consumer to process
            await self.wait_for_queues_empty(['file_assignment_processing'], 600)
        
        return success
    
    async def reconstruct_profiles(self) -> bool:
        """Reconstruct user and miner profiles."""
        logger.info("🔧 Reconstructing profiles")
        
        # Process user profiles first
        user_success = self.run_processor(
            'user_profile_reconstruction_processor.py',
            'User profile reconstruction'
        )
        
        if user_success:
            await self.wait_for_queues_empty(['user_profile_reconstruction'], 300)
        
        # Process miner profiles
        miner_success = self.run_processor(
            'miner_profile_reconstruction_processor.py',
            'Miner profile reconstruction'
        )
        
        if miner_success:
            await self.wait_for_queues_empty(['miner_profile_reconstruction'], 300)
        
        return user_success and miner_success
    
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
        if not self.epoch_initialized:
            success = await self.epoch_initialization()
            if success:
                self.epoch_initialized = True
            else:
                logger.error("Failed to initialize epoch data")
                return
        
        # Perform health checks and submit to chain
        if not self.health_checks_completed:
            success = await self.perform_health_checks()
            if success:
                self.health_checks_completed = True
                logger.info("✅ Health checks completed and submitted to chain")
            else:
                logger.error("❌ Health checks failed")
        
        # Wait for end of epoch
        logger.info("⏳ Waiting for end of epoch...")
    
    async def validator_workflow(self):
        """Execute validator workflow."""
        logger.info("👑 Executing VALIDATOR workflow")
        
        current_epoch, current_block = get_current_epoch_info(self.substrate)
        block_position = get_epoch_block_position(current_block)
        
        logger.info(f"Current block position in epoch: {block_position}/99")
        
        # Phase 1: Initialization (blocks 0-10)
        if block_position <= 10 and not self.epoch_initialized:
            success = await self.epoch_initialization()
            if success:
                self.epoch_initialized = True
            else:
                logger.error("Failed to initialize epoch data")
                return
        
        # Phase 2: Pinning requests (blocks 11-50)
        elif 11 <= block_position <= 50:
            if not self.pinning_requests_processed:
                # Process pinning requests periodically
                success = await self.process_pinning_requests()
                if success:
                    # Also process the files from pinning requests
                    await self.process_pinning_files()
                
                # Don't mark as completed until block 50 to allow periodic processing
                if block_position >= 45:
                    self.pinning_requests_processed = True
        
        # Phase 3: File assignment and health checks (blocks 51-80)
        elif 51 <= block_position <= 80:
            if not self.files_assigned:
                success = await self.assign_files()
                if success:
                    self.files_assigned = True
            
            if not self.health_checks_completed:
                success = await self.perform_health_checks()
                if success:
                    self.health_checks_completed = True
        
        # Phase 4: Profile reconstruction (blocks 81-95)
        elif 81 <= block_position <= 95:
            if not self.profiles_reconstructed:
                success = await self.reconstruct_profiles()
                if success:
                    self.profiles_reconstructed = True
                    logger.info("✅ All profile reconstruction completed before block 95")
                else:
                    logger.error("❌ Profile reconstruction failed - must complete before block 95!")
        
        # Phase 5: Finalization (blocks 96-99)
        elif block_position >= 96:
            logger.info("🏁 Epoch finalization phase - preparing for next epoch")
    
    async def reset_epoch_state(self):
        """Reset state for new epoch."""
        logger.info("🔄 Resetting epoch state for new epoch")
        
        self.epoch_initialized = False
        self.pinning_requests_processed = False
        self.files_assigned = False
        self.health_checks_completed = False
        self.profiles_reconstructed = False
    
    async def run(self):
        """Main orchestrator loop."""
        logger.info("🎯 Starting Epoch Orchestrator")
        
        try:
            await self.initialize()
            
            last_epoch = None
            
            while True:
                try:
                    # Get current epoch and validator status
                    current_epoch, current_block = get_current_epoch_info(self.substrate)
                    is_validator, current_validator, epoch_start = is_epoch_validator(
                        self.substrate, self.our_validator_account
                    )
                    
                    # Check if we've moved to a new epoch
                    if last_epoch is not None and current_epoch != last_epoch:
                        logger.info(f"🔄 New epoch detected: {last_epoch} -> {current_epoch}")
                        await self.reset_epoch_state()
                    
                    # Update state
                    self.current_epoch = current_epoch
                    self.current_block = current_block
                    self.is_validator = is_validator
                    self.epoch_start_block = epoch_start
                    last_epoch = current_epoch
                    
                    block_position = get_epoch_block_position(current_block)
                    
                    logger.info(f"📊 Epoch {current_epoch}, Block {current_block} (position {block_position}/99)")
                    logger.info(f"🎭 Role: {'VALIDATOR' if is_validator else 'NON-VALIDATOR'}")
                    
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
                    logger.exception("Full traceback:")
                    await asyncio.sleep(60)  # Wait longer on error
                    
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


async def main():
    """Main entry point."""
    orchestrator = EpochOrchestrator()
    await orchestrator.run()


if __name__ == "__main__":
    asyncio.run(main()) 