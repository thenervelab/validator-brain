#!/usr/bin/env python3
"""
Simple Miner Health Consumer

This consumer processes miner health check messages and:
1. Performs IPFS ping tests on miners
2. Performs IPFS pin tests on a few assigned files
3. Removes failed miners from file assignments (sets to NULL)
4. Updates miner_epoch_health table with results
5. Lets the file assignment system handle reassignment
"""

import asyncio
import json
import logging
import os
import sys
from datetime import datetime
from typing import Dict, List, Any

# Add parent directory to path for imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import aio_pika
from aio_pika import IncomingMessage
from dotenv import load_dotenv

from app.db.connection import init_db_pool, close_db_pool, get_db_pool
from app.db.queries.file_assignment_queries import remove_miner_from_assignments, get_miner_file_count
from substrate_fetcher.ipfs_health_utils import perform_ipfs_ping, perform_ipfs_pin_check

# Load environment variables
load_dotenv()

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class MinerHealthConsumer:
    def __init__(self):
        self.rabbitmq_url = os.getenv('RABBITMQ_URL', 'amqp://admin:admin@localhost:5672/')
        self.queue_name = 'miner_health_check'
        self.rabbitmq_connection = None
        self.rabbitmq_channel = None
        self.db_pool = None
        self.stop_event = asyncio.Event()
        
        # Configuration
        self.ping_failure_threshold = int(os.getenv('PING_FAILURE_THRESHOLD', '1'))  # Remove after 1 ping failure
        self.pin_failure_threshold = int(os.getenv('PIN_FAILURE_THRESHOLD', '2'))   # Remove after 2 pin failures
        
    async def connect_rabbitmq(self):
        """Connect to RabbitMQ."""
        try:
            self.rabbitmq_connection = await aio_pika.connect_robust(self.rabbitmq_url)
            self.rabbitmq_channel = await self.rabbitmq_connection.channel()
            
            # Set prefetch count to process one message at a time
            await self.rabbitmq_channel.set_qos(prefetch_count=1)
            
            logger.info("Connected to RabbitMQ")
        except Exception as e:
            logger.error(f"Failed to connect to RabbitMQ: {e}")
            raise
    
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
    
    async def update_health_results(self, node_id: str, epoch: int, ping_success: bool, 
                                  pin_successes: int, pin_failures: int) -> None:
        """Update the miner_epoch_health table with test results."""
        async with self.db_pool.acquire() as conn:
            try:
                await conn.execute("""
                    UPDATE miner_epoch_health
                    SET 
                        ping_successes = ping_successes + $3,
                        ping_failures = ping_failures + $4,
                        pin_check_successes = pin_check_successes + $5,
                        pin_check_failures = pin_check_failures + $6,
                        last_activity_at = NOW()
                    WHERE node_id = $1 AND epoch = $2
                """, node_id, epoch, 
                    1 if ping_success else 0,  # ping_successes increment
                    0 if ping_success else 1,  # ping_failures increment
                    pin_successes,              # pin_check_successes increment
                    pin_failures                # pin_check_failures increment
                )
                
                logger.debug(f"Updated health results for miner {node_id} in epoch {epoch}")
                
            except Exception as e:
                logger.error(f"Error updating health results for miner {node_id}: {e}")
    
    async def remove_failed_miner(self, node_id: str, reason: str) -> bool:
        """Remove a failed miner from all file assignments."""
        try:
            async with self.db_pool.acquire() as conn:
                # Get file count before removal
                file_stats = await get_miner_file_count(conn, node_id)
                file_count = file_stats.get('file_count', 0)
                
                if file_count == 0:
                    logger.info(f"Miner {node_id} has no files assigned, skipping removal")
                    return True
                
                # Remove miner from all assignments
                removed_count = await remove_miner_from_assignments(conn, node_id)
                
                logger.warning(f"Removed failed miner {node_id} from {removed_count} file assignments. Reason: {reason}")
                logger.info(f"File assignment system will detect empty slots and reassign files automatically")
                
                return True
                
        except Exception as e:
            logger.error(f"Error removing failed miner {node_id}: {e}")
            return False
    
    async def process_health_check(self, message_data: Dict[str, Any]) -> bool:
        """Process a single health check message."""
        try:
            node_id = message_data['node_id']
            ipfs_peer_id = message_data['ipfs_peer_id']
            epoch = message_data['epoch']
            block_number = message_data.get('block_number')
            files_to_check = message_data.get('files_to_check', [])
            total_files_assigned = message_data.get('total_files_assigned', 0)
            
            logger.info(f"Processing health check for miner {node_id} (IPFS: {ipfs_peer_id}) in epoch {epoch}")
            logger.info(f"Miner has {total_files_assigned} files assigned, checking {len(files_to_check)} files")
            
            # Ensure miner exists in health table
            await self.ensure_miner_in_health_table(node_id, ipfs_peer_id, epoch)
            
            # Perform ping test
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
                )
                logger.info(f"Ping test successful for {node_id}")
            except Exception as e:
                logger.error(f"Ping test failed for {node_id}: {e}")
                ping_successful = False
            
            # If ping fails, remove miner immediately
            if not ping_successful:
                await self.update_health_results(node_id, epoch, False, 0, 0)
                await self.remove_failed_miner(node_id, f"Ping test failed: {e}")
                return True
            
            # Perform pin tests on assigned files
            pin_successes = 0
            pin_failures = 0
            
            if files_to_check:
                logger.info(f"Performing pin tests for {node_id} on {len(files_to_check)} files")
                
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
                        )
                        pin_successes += 1
                        logger.debug(f"Pin check successful for file {file_cid} on {node_id}")
                        
                    except Exception as e:
                        logger.error(f"Pin check failed for file {file_cid} on {node_id}: {e}")
                        pin_failures += 1
                
                logger.info(f"Pin test results for {node_id}: {pin_successes} successful, {pin_failures} failed")
                
                # Check if miner should be removed due to pin failures
                if pin_failures >= self.pin_failure_threshold:
                    await self.update_health_results(node_id, epoch, True, pin_successes, pin_failures)
                    await self.remove_failed_miner(node_id, f"Pin tests failed: {pin_failures}/{len(files_to_check)} files")
                    return True
            else:
                logger.info(f"No files to check for miner {node_id}")
            
            # Update health results
            await self.update_health_results(node_id, epoch, True, pin_successes, pin_failures)
            
            logger.info(f"Health check completed successfully for miner {node_id}")
            return True
            
        except Exception as e:
            logger.error(f"Error processing health check for {message_data.get('node_id', 'unknown')}: {e}")
            logger.exception("Full traceback:")
            return False
    
    async def message_handler(self, message: IncomingMessage):
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
            logger.info(f"Ping failure threshold: {self.ping_failure_threshold}")
            logger.info(f"Pin failure threshold: {self.pin_failure_threshold}")
            
            # Start consuming
            await queue.consume(self.message_handler)
            
            # Keep the consumer running
            logger.info("Simple miner health consumer is running. Press Ctrl+C to stop.")
            try:
                await self.stop_event.wait()
            except KeyboardInterrupt:
                logger.info("Received interrupt signal")
                self.stop_event.set()
            
        except Exception as e:
            logger.error(f"Error in consumer: {e}")
            raise
    
    async def close(self):
        """Close all connections."""
        if self.rabbitmq_connection:
            await self.rabbitmq_connection.close()
            logger.info("Closed RabbitMQ connection")


async def main():
    """Main entry point."""
    consumer = MinerHealthConsumer()
    
    try:
        # Initialize database pool
        await init_db_pool()
        consumer.db_pool = await get_db_pool()
        logger.info("Database connection pool initialized")
        
        # Connect to RabbitMQ
        await consumer.connect_rabbitmq()
        
        # Start consuming
        await consumer.start_consuming()
        
    except KeyboardInterrupt:
        logger.info("Shutting down miner health consumer...")
    except Exception as e:
        logger.error(f"Error in consumer: {e}")
        raise
    finally:
        await consumer.close()
        await close_db_pool()


if __name__ == "__main__":
    asyncio.run(main()) 