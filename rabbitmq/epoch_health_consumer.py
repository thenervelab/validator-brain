"""
Epoch Health Consumer for processing comprehensive epoch health checks.

This consumer:
1. Reads messages from the epoch_health_check queue
2. Performs IPFS ping tests on miners
3. Performs IPFS pin tests on ALL files assigned to miners
4. Records results in the miner_epoch_health table (not file_failures)
5. Provides comprehensive epoch-based health assessment
6. Automatically submits results to blockchain when all miners are processed
"""

import asyncio
import json
import logging
import os
import sys
from datetime import datetime
from typing import Dict, List, Any, Optional

# Add parent directory to path to import app modules
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import aio_pika
from dotenv import load_dotenv
from substrateinterface import SubstrateInterface, Keypair

from app.db.connection import get_db_pool, init_db_pool, close_db_pool
from substrate_fetcher.ipfs_health_utils import perform_ipfs_ping, perform_ipfs_pin_check
from app.services.substrate_client import TYPE_REGISTRY

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def string_to_bounded_vec(s: str) -> List[int]:
    """Convert a string to a BoundedVec (list of bytes)."""
    return list(s.encode('utf-8'))


def load_hips_keypair(keystore_path: str = None) -> Keypair:
    """Load keypair from environment or keystore."""
    # Try to get seed from environment first
    seed_phrase = os.getenv('VALIDATOR_SEED_PHRASE')
    if seed_phrase:
        logger.info("Loading keypair from VALIDATOR_SEED_PHRASE environment variable")
        return Keypair.create_from_mnemonic(seed_phrase)
    
    # Fallback to keystore path if provided
    if keystore_path and os.path.exists(keystore_path):
        logger.info(f"Loading keypair from keystore: {keystore_path}")
        # Implementation would depend on keystore format
        # For now, raise an error if no seed phrase is available
        raise ValueError("Keystore loading not implemented. Please set VALIDATOR_SEED_PHRASE environment variable.")
    
    # If no seed phrase or keystore, raise error
    raise ValueError("No keypair source available. Please set VALIDATOR_SEED_PHRASE environment variable.")


def call_update_pin_check_metrics(miners_metrics: List[Dict[str, Any]]) -> bool:
    """Calls the update_pin_check_metrics extrinsic with enhanced debugging"""
    try:
        node_url = os.getenv('NODE_URL', 'wss://rpc.hippius.network')
        substrate = SubstrateInterface(
            url=node_url,
            # type_registry=TYPE_REGISTRY,
            use_remote_preset=True
        )
        logger.info(f"Connected to Substrate node at {node_url}")

        keypair = load_hips_keypair()
        logger.info(f"Using account {keypair.ss58_address} for signing")

        # Debug: Print metadata to verify pallet exists
        metadata = substrate.get_metadata()
        if 'ExecutionUnit' not in [p.name for p in metadata.pallets]:
            logger.error("ExecutionUnit pallet not found in chain metadata!")
            return False

        formatted_metrics = []
        for metric in miners_metrics:
            logger.info(f"Submitting metrics for node {metric['node_id']}")
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
            return True
        else:
            logger.error(f"Extrinsic failed: {receipt.error_message}")
            return False

    except Exception as e:
        logger.error(f"Error submitting to blockchain: {e}", exc_info=True)
        return False
    finally:
        if 'substrate' in locals():
            substrate.close()


class EpochHealthConsumer:
    """Consumer for processing comprehensive epoch health checks."""
    
    def __init__(self):
        self.rabbitmq_url = os.getenv('RABBITMQ_URL', 'amqp://admin:admin@localhost:5672/')
        self.queue_name = 'epoch_health_check'
        
        self.rabbitmq_connection = None
        self.rabbitmq_channel = None
        self.db_pool = None
        self.stop_event = asyncio.Event()
        
        # Configuration for epoch health checks
        self.max_files_per_miner = int(os.getenv('EPOCH_MAX_FILES_PER_MINER', '50'))  # Higher limit for epoch checks
        
        # Track completion for blockchain submission
        self.current_epoch = None
        self.expected_miners = 0
        self.processed_miners = 0
        
    async def connect(self):
        """Connect to RabbitMQ and database."""
        try:
            # Initialize database pool
            await init_db_pool()
            self.db_pool = get_db_pool()
            logger.info("Database connection pool initialized")
            
            # Connect to RabbitMQ
            self.rabbitmq_connection = await aio_pika.connect_robust(self.rabbitmq_url)
            self.rabbitmq_channel = await self.rabbitmq_connection.channel()
            
            # Set prefetch count to process one message at a time
            await self.rabbitmq_channel.set_qos(prefetch_count=1)
            
            logger.info("Connected to RabbitMQ")
            
        except Exception as e:
            logger.error(f"Failed to connect: {e}")
            raise
    
    async def disconnect(self):
        """Disconnect from all services."""
        if self.rabbitmq_connection:
            await self.rabbitmq_connection.close()
        if self.db_pool:
            await close_db_pool()
    
    async def ensure_miner_in_health_table(self, node_id: str, ipfs_peer_id: str, epoch: int) -> None:
        """Ensure the miner exists in the miner_epoch_health table."""
        async with self.db_pool.acquire() as conn:
            try:
                await conn.execute("""
                    INSERT INTO miner_epoch_health (node_id, ipfs_peer_id, epoch, last_activity_at)
                    VALUES ($1, $2, $3, NOW())
                    ON CONFLICT (node_id, epoch) DO UPDATE SET
                        ipfs_peer_id = EXCLUDED.ipfs_peer_id,
                        last_activity_at = NOW()
                """, node_id, ipfs_peer_id, epoch)
                
                logger.debug(f"Ensured miner {node_id} exists in health table for epoch {epoch}")
                
            except Exception as e:
                logger.error(f"Error ensuring miner {node_id} in health table: {e}")
    
    async def check_completion_and_submit(self, epoch: int) -> None:
        """Check if all miners are processed and submit to blockchain if complete."""
        async with self.db_pool.acquire() as conn:
            try:
                # Count processed miners for this epoch
                processed_count = await conn.fetchval("""
                    SELECT COUNT(*) FROM miner_epoch_health 
                    WHERE epoch = $1
                """, epoch)
                
                # Get expected count from the queue or registration table
                expected_count = await conn.fetchval("""
                    SELECT COUNT(*) FROM registration 
                    WHERE node_type = 'StorageMiner' AND status = 'active'
                """, )
                
                logger.info(f"Epoch {epoch} progress: {processed_count}/{expected_count} miners processed")
                
                # Check if all miners are processed
                if processed_count >= expected_count and processed_count > 0:
                    logger.info(f"All miners processed for epoch {epoch}! Preparing blockchain submission...")
                    
                    # Get all health data for this epoch
                    health_data = await conn.fetch("""
                        SELECT 
                            node_id,
                            pin_check_successes,
                            pin_check_failures,
                            (pin_check_successes + pin_check_failures) as total_pin_checks
                        FROM miner_epoch_health 
                        WHERE epoch = $1 AND (pin_check_successes + pin_check_failures) > 0
                        ORDER BY node_id
                    """, epoch)
                    
                    if health_data:
                        # Format data for blockchain submission
                        miners_metrics = []
                        for row in health_data:
                            miners_metrics.append({
                                "node_id": row['node_id'],
                                "total_pin_checks": row['total_pin_checks'],
                                "successful_pin_checks": row['pin_check_successes']
                            })
                        
                        logger.info(f"Submitting {len(miners_metrics)} miner metrics to blockchain for epoch {epoch}")
                        
                        # Submit to blockchain
                        success = call_update_pin_check_metrics(miners_metrics)
                        
                        if success:
                            # Mark epoch as submitted
                            await conn.execute("""
                                INSERT INTO epoch_submissions (epoch, submitted_at, miner_count, success)
                                VALUES ($1, NOW(), $2, true)
                                ON CONFLICT (epoch) DO UPDATE SET
                                    submitted_at = NOW(),
                                    miner_count = EXCLUDED.miner_count,
                                    success = true
                            """, epoch, len(miners_metrics))
                            
                            logger.info(f"✅ Successfully submitted epoch {epoch} health metrics to blockchain!")
                        else:
                            logger.error(f"❌ Failed to submit epoch {epoch} health metrics to blockchain")
                            
                            # Mark as failed submission
                            await conn.execute("""
                                INSERT INTO epoch_submissions (epoch, submitted_at, miner_count, success)
                                VALUES ($1, NOW(), $2, false)
                                ON CONFLICT (epoch) DO UPDATE SET
                                    submitted_at = NOW(),
                                    miner_count = EXCLUDED.miner_count,
                                    success = false
                            """, epoch, len(miners_metrics))
                    else:
                        logger.warning(f"No health data found for epoch {epoch} - skipping blockchain submission")
                        
            except Exception as e:
                logger.error(f"Error checking completion for epoch {epoch}: {e}")
    
    async def process_epoch_health_check(self, message_data: Dict[str, Any]) -> bool:
        """
        Process a comprehensive epoch health check message.
        
        Args:
            message_data: Epoch health check data from the queue
            
        Returns:
            True if successful, False otherwise
        """
        try:
            node_id = message_data['node_id']
            ipfs_peer_id = message_data['ipfs_peer_id']
            epoch = message_data['epoch']
            files = message_data.get('files', [])
            total_files = message_data.get('total_files', len(files))
            block_number = message_data.get('block_number')
            check_type = message_data.get('check_type', 'epoch_comprehensive')
            
            logger.info(f"Processing {check_type} health check for miner {node_id} (IPFS: {ipfs_peer_id}) in epoch {epoch}")
            logger.info(f"Miner has {total_files} files assigned, checking {min(len(files), self.max_files_per_miner)} files")
            
            # Ensure miner exists in health table
            await self.ensure_miner_in_health_table(node_id, ipfs_peer_id, epoch)
            
            # Perform ping test first
            logger.info(f"Performing ping test for {node_id}")
            ping_successful = True
            try:
                await perform_ipfs_ping(
                    self.db_pool, 
                    node_id, 
                    ipfs_peer_id, 
                    epoch, 
                    block_number, 
                    self.stop_event
                    # Note: No availability_manager for epoch checks - we only update miner_epoch_health
                )
            except Exception as e:
                logger.error(f"Ping test failed for {node_id}: {e}")
                ping_successful = False
            
            # Perform pin tests on ALL files (or up to max limit)
            if files and ping_successful:
                # Limit the number of files to check for epoch assessment
                files_to_check = files[:self.max_files_per_miner]
                if len(files) > self.max_files_per_miner:
                    logger.info(f"Limiting epoch file checks to {self.max_files_per_miner} out of {len(files)} files for miner {node_id}")
                
                logger.info(f"Performing pin tests for {node_id} on {len(files_to_check)} files")
                
                successful_checks = 0
                failed_checks = 0
                
                for i, file_cid in enumerate(files_to_check, 1):
                    if self.stop_event.is_set():
                        logger.warning(f"Stop event detected, stopping file checks for {node_id}")
                        break
                    
                    logger.debug(f"Checking file {i}/{len(files_to_check)} for {node_id}: {file_cid}")
                    
                    try:
                        await perform_ipfs_pin_check(
                            self.db_pool,
                            node_id,
                            ipfs_peer_id,
                            file_cid,
                            epoch,
                            self.stop_event
                            # Note: No availability_manager for epoch checks
                        )
                        successful_checks += 1
                        
                    except Exception as e:
                        logger.debug(f"Error checking file {file_cid} for miner {node_id}: {e}")
                        failed_checks += 1
                
                logger.info(f"Completed epoch pin tests for {node_id}: {successful_checks} successful, {failed_checks} failed")
                
                # Update summary statistics in miner_epoch_health
                await self.update_epoch_summary(node_id, epoch, successful_checks, failed_checks, total_files)
                
            elif not ping_successful:
                logger.warning(f"Skipping pin tests for {node_id} due to ping failure")
                # Record that all files failed due to ping failure
                await self.update_epoch_summary(node_id, epoch, 0, total_files, total_files)
            else:
                logger.warning(f"No files found for miner {node_id}, skipping pin tests")
                await self.update_epoch_summary(node_id, epoch, 0, 0, 0)
            
            logger.info(f"Completed comprehensive epoch health check for miner {node_id}")
            
            # Check if all miners are processed and submit to blockchain if complete
            await self.check_completion_and_submit(epoch)
            
            return True
            
        except Exception as e:
            logger.error(f"Error processing epoch health check for {message_data.get('node_id', 'unknown')}: {e}")
            logger.exception("Full traceback:")
            return False
    
    async def update_epoch_summary(self, node_id: str, epoch: int, successful_checks: int, failed_checks: int, total_files: int) -> None:
        """Update epoch summary statistics in miner_epoch_health."""
        async with self.db_pool.acquire() as conn:
            try:
                await conn.execute("""
                    UPDATE miner_epoch_health 
                    SET 
                        pin_check_successes = pin_check_successes + $3,
                        pin_check_failures = pin_check_failures + $4,
                        last_activity_at = NOW()
                    WHERE node_id = $1 AND epoch = $2
                """, node_id, epoch, successful_checks, failed_checks)
                
                logger.debug(f"Updated epoch summary for {node_id}: +{successful_checks} successes, +{failed_checks} failures")
                
            except Exception as e:
                logger.error(f"Error updating epoch summary for {node_id}: {e}")
    
    async def message_handler(self, message: aio_pika.IncomingMessage):
        """Handle incoming messages from the queue."""
        async with message.process():
            try:
                # Parse message
                message_data = json.loads(message.body.decode())
                
                # Process the epoch health check
                success = await self.process_epoch_health_check(message_data)
                
                if success:
                    logger.debug(f"Successfully processed epoch health check for {message_data.get('node_id', 'unknown')}")
                else:
                    logger.error(f"Failed to process epoch health check for {message_data.get('node_id', 'unknown')}")
                
            except json.JSONDecodeError as e:
                logger.error(f"Failed to decode message: {e}")
            except Exception as e:
                logger.error(f"Error in message handler: {e}")
                logger.exception("Full traceback:")
    
    async def start_consuming(self):
        """Start consuming messages from the queue."""
        try:
            # Declare the queue (in case it doesn't exist)
            queue = await self.rabbitmq_channel.declare_queue(
                self.queue_name,
                durable=True
            )
            
            logger.info(f"Starting to consume from queue '{self.queue_name}'")
            logger.info(f"Max files per miner for epoch checks: {self.max_files_per_miner}")
            logger.info("🔗 Blockchain submission enabled - will auto-submit when all miners are processed")
            
            # Start consuming
            await queue.consume(self.message_handler)
            
            # Keep the consumer running
            logger.info("Epoch health check consumer is running. Press Ctrl+C to stop.")
            try:
                await self.stop_event.wait()
            except KeyboardInterrupt:
                logger.info("Received interrupt signal")
                self.stop_event.set()
            
        except Exception as e:
            logger.error(f"Error in consumer: {e}")
            raise
    
    async def run(self):
        """Main run method."""
        try:
            await self.connect()
            await self.start_consuming()
        finally:
            await self.disconnect()


async def main():
    """Main function."""
    consumer = EpochHealthConsumer()
    
    try:
        await consumer.run()
    except KeyboardInterrupt:
        logger.info("Shutting down epoch health check consumer...")
    except Exception as e:
        logger.error(f"Error in main: {e}")
        raise


if __name__ == "__main__":
    asyncio.run(main()) 