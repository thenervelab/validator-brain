"""
Consumer for processing miner health checks from RabbitMQ queue.

This consumer:
1. Reads messages from the miner_health_check queue
2. Performs IPFS ping tests on miners
3. Performs IPFS pin tests on random files assigned to miners
4. Updates the miner_epoch_health table with results
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

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class MinerHealthConsumer:
    """Consumer for processing miner health checks."""
    
    def __init__(self):
        self.rabbitmq_url = os.getenv('RABBITMQ_URL', 'amqp://admin:admin@localhost:5672/')
        self.queue_name = 'miner_health_check'
        
        self.rabbitmq_connection = None
        self.rabbitmq_channel = None
        self.db_pool = None
        self.stop_event = asyncio.Event()
        
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
    
    async def process_health_check(self, message_data: Dict[str, Any]) -> bool:
        """
        Process a single health check message.
        
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
            
            logger.info(f"Processing health check for miner {node_id} (IPFS: {ipfs_peer_id}) in epoch {epoch}")
            
            # Ensure miner exists in health table
            await self.ensure_miner_in_health_table(node_id, ipfs_peer_id, epoch)
            
            # Perform ping test
            logger.info(f"Performing ping test for {node_id}")
            await perform_ipfs_ping(
                self.db_pool, 
                node_id, 
                ipfs_peer_id, 
                epoch, 
                block_number, 
                self.stop_event
            )
            
            # Perform pin test on a random file if files are available
            if files:
                # Select a random file for pin testing
                random_file = random.choice(files)
                logger.info(f"Performing pin test for {node_id} on file {random_file}")
                
                await perform_ipfs_pin_check(
                    self.db_pool,
                    node_id,
                    ipfs_peer_id,
                    random_file,
                    epoch,
                    self.stop_event
                )
            else:
                logger.warning(f"No files found for miner {node_id}, skipping pin test")
            
            logger.info(f"Completed health check for miner {node_id}")
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
            
            # Start consuming
            await queue.consume(self.message_handler)
            
            # Keep the consumer running
            logger.info("Health check consumer is running. Press Ctrl+C to stop.")
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
    consumer = MinerHealthConsumer()
    
    try:
        await consumer.run()
    except KeyboardInterrupt:
        logger.info("Shutting down health check consumer...")
    except Exception as e:
        logger.error(f"Error in main: {e}")
        raise


if __name__ == "__main__":
    asyncio.run(main()) 