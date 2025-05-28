"""
Enhanced Consumer for processing miner health checks with automatic reassignment.

This consumer:
1. Reads messages from the miner_health_check queue
2. Performs IPFS ping tests on miners
3. Performs IPFS pin tests on ALL files assigned to miners
4. Records failures and successes in the availability tracking system
5. Automatically triggers replica reassignment for failed files
6. Updates the miner_epoch_health table with results
"""

import asyncio
import json
import logging
import os
import sys
import random
from datetime import datetime
from typing import Dict, List, Any, Optional

# Add parent directory to path to import app modules
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import aio_pika
from dotenv import load_dotenv

from app.db.connection import get_db_pool, init_db_pool, close_db_pool
from substrate_fetcher.ipfs_health_utils import perform_ipfs_ping, perform_ipfs_pin_check
from substrate_fetcher.file_availability_manager import FileAvailabilityManager, AvailabilityRules

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class MinerHealthConsumerWithReassignment:
    """Enhanced consumer for processing miner health checks with automatic reassignment."""
    
    def __init__(self):
        self.rabbitmq_url = os.getenv('RABBITMQ_URL', 'amqp://admin:admin@localhost:5672/')
        self.queue_name = 'miner_health_check'
        
        self.rabbitmq_connection = None
        self.rabbitmq_channel = None
        self.db_pool = None
        self.stop_event = asyncio.Event()
        
        # Configuration for file checking
        self.max_files_per_miner = int(os.getenv('MAX_FILES_PER_MINER', '20'))  # Check more files
        self.enable_reassignment = os.getenv('ENABLE_AUTO_REASSIGNMENT', 'true').lower() == 'true'
        
        # Availability manager
        self.availability_manager = None
        
        # Availability rules configuration
        self.availability_rules = AvailabilityRules(
            min_replicas=int(os.getenv('MIN_REPLICAS', '5')),
            max_replicas=int(os.getenv('MAX_REPLICAS', '5')),
            min_availability_score=float(os.getenv('MIN_AVAILABILITY_SCORE', '0.7')),
            max_consecutive_failures=int(os.getenv('MAX_CONSECUTIVE_FAILURES', '3')),
            failure_window_hours=int(os.getenv('FAILURE_WINDOW_HOURS', '24')),
            reassignment_cooldown_hours=int(os.getenv('REASSIGNMENT_COOLDOWN_HOURS', '6'))
        )
        
    async def connect(self):
        """Connect to RabbitMQ and database."""
        try:
            # Initialize database pool
            await init_db_pool()
            self.db_pool = get_db_pool()
            logger.info("Database connection pool initialized")
            
            # Initialize availability manager
            self.availability_manager = FileAvailabilityManager(self.db_pool, self.availability_rules)
            logger.info("File availability manager initialized")
            
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
    
    async def process_health_check(self, message_data: Dict[str, Any]) -> bool:
        """
        Process a single health check message with comprehensive file checking.
        
        Args:
            message_data: Health check data from the queue
            
        Returns:
            True if successful, False otherwise
        """
        try:
            node_id = message_data['node_id']
            ipfs_peer_id = message_data['ipfs_peer_id']
            epoch = message_data['epoch']
            files = message_data.get('files', [])
            block_number = message_data.get('block_number')
            
            logger.info(f"Processing comprehensive health check for miner {node_id} (IPFS: {ipfs_peer_id}) in epoch {epoch}")
            logger.info(f"Miner has {len(files)} files assigned")
            
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
                    self.stop_event,
                    self.availability_manager
                )
            except Exception as e:
                logger.error(f"Ping test failed for {node_id}: {e}")
                ping_successful = False
                # Record ping failure for all files
                if self.availability_manager:
                    for file_cid in files[:5]:  # Record for a few files to indicate miner issues
                        await self.availability_manager.record_failure(
                            file_cid, node_id, epoch, 'ping_failed', str(e)
                        )
            
            # Perform pin tests on ALL files (or up to max limit)
            if files and ping_successful:
                # Limit the number of files to check to prevent overload
                files_to_check = files[:self.max_files_per_miner]
                if len(files) > self.max_files_per_miner:
                    logger.info(f"Limiting file checks to {self.max_files_per_miner} out of {len(files)} files for miner {node_id}")
                
                logger.info(f"Performing pin tests for {node_id} on {len(files_to_check)} files")
                
                successful_checks = 0
                failed_checks = 0
                failed_files = []
                
                for i, file_cid in enumerate(files_to_check, 1):
                    if self.stop_event.is_set():
                        logger.warning(f"Stop event detected, stopping file checks for {node_id}")
                        break
                    
                    logger.info(f"Checking file {i}/{len(files_to_check)} for {node_id}: {file_cid}")
                    
                    try:
                        await perform_ipfs_pin_check(
                            self.db_pool,
                            node_id,
                            ipfs_peer_id,
                            file_cid,
                            epoch,
                            self.stop_event,
                            self.availability_manager
                        )
                        successful_checks += 1
                        
                    except Exception as e:
                        logger.error(f"Error checking file {file_cid} for miner {node_id}: {e}")
                        failed_checks += 1
                        failed_files.append(file_cid)
                        
                        # Record the failure
                        if self.availability_manager:
                            await self.availability_manager.record_failure(
                                file_cid, node_id, epoch, 'pin_failed', str(e)
                            )
                
                logger.info(f"Completed pin tests for {node_id}: {successful_checks} successful, {failed_checks} failed")
                
                # Trigger reassignment if enabled and there are failures
                if self.enable_reassignment and failed_files:
                    logger.info(f"Triggering reassignment check for {len(failed_files)} failed files on miner {node_id}")
                    try:
                        reassignment_stats = await self.availability_manager.process_reassignments()
                        logger.info(f"Reassignment results: {reassignment_stats}")
                    except Exception as e:
                        logger.error(f"Error during reassignment process: {e}")
                
            elif not ping_successful:
                logger.warning(f"Skipping pin tests for {node_id} due to ping failure")
            else:
                logger.warning(f"No files found for miner {node_id}, skipping pin tests")
            
            logger.info(f"Completed comprehensive health check for miner {node_id}")
            return True
            
        except Exception as e:
            logger.error(f"Error processing health check for {message_data.get('node_id', 'unknown')}: {e}")
            logger.exception("Full traceback:")
            return False
    
    async def message_handler(self, message: aio_pika.IncomingMessage):
        """Handle incoming messages from the queue."""
        async with message.process():
            try:
                # Parse message
                message_data = json.loads(message.body.decode())
                
                # Process the health check
                success = await self.process_health_check(message_data)
                
                if success:
                    logger.debug(f"Successfully processed health check for {message_data.get('node_id', 'unknown')}")
                else:
                    logger.error(f"Failed to process health check for {message_data.get('node_id', 'unknown')}")
                
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
            logger.info(f"Max files per miner: {self.max_files_per_miner}")
            logger.info(f"Auto-reassignment enabled: {self.enable_reassignment}")
            logger.info(f"Availability rules: {self.availability_rules}")
            
            # Start consuming
            await queue.consume(self.message_handler)
            
            # Keep the consumer running
            logger.info("Enhanced health check consumer (ALL FILES + REASSIGNMENT) is running. Press Ctrl+C to stop.")
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
    consumer = MinerHealthConsumerWithReassignment()
    
    try:
        await consumer.run()
    except KeyboardInterrupt:
        logger.info("Shutting down enhanced health check consumer...")
    except Exception as e:
        logger.error(f"Error in main: {e}")
        raise


if __name__ == "__main__":
    asyncio.run(main()) 