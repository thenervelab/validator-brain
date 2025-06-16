"""
RabbitMQ consumer for processing node metrics messages.

This consumer:
1. Listens to the node_metrics queue
2. Processes each message containing node metrics
3. Stores the metrics in the node_metrics table
"""

import asyncio
import json
import logging
import os
import sys
from typing import Dict, Any

# Add parent directory to path to import app modules
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import aio_pika
from dotenv import load_dotenv

from app.db.connection import get_db_pool, init_db_pool, close_db_pool

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class NodeMetricsConsumer:
    """Consumer for processing node metrics messages."""
    
    def __init__(self):
        """Initialize the consumer."""
        self.rabbitmq_connection = None
        self.rabbitmq_channel = None
        self.queue_name = 'node_metrics_latest'
        self.db_pool = None
        
    async def connect_rabbitmq(self):
        """Connect to RabbitMQ."""
        rabbitmq_url = os.getenv('RABBITMQ_URL', 'amqp://admin:admin@localhost:5672/')
        
        self.rabbitmq_connection = await aio_pika.connect_robust(rabbitmq_url)
        self.rabbitmq_channel = await self.rabbitmq_connection.channel()
        
        # Set prefetch count to process one message at a time
        await self.rabbitmq_channel.set_qos(prefetch_count=1)
        
        logger.info("Connected to RabbitMQ")
        
    async def init_database(self):
        """Initialize database connection pool."""
        await init_db_pool()
        self.db_pool = get_db_pool()
        logger.info("Database connection pool initialized")
        
    async def store_node_metrics(self, metrics: Dict[str, Any]) -> bool:
        """
        Store node metrics in the database.
        
        Args:
            metrics: Dictionary containing node metrics data
            
        Returns:
            True if successful, False otherwise
        """
        try:
            async with self.db_pool.acquire() as conn:
                # Start a transaction
                async with conn.transaction():
                    # Delete all old records for this miner
                    deleted = await conn.execute("""
                        DELETE FROM node_metrics 
                        WHERE miner_id = $1
                    """, metrics['miner_id'])
                    
                    # Insert the new record
                    await conn.execute("""
                        INSERT INTO node_metrics (
                            miner_id, ipfs_repo_size, ipfs_storage_max, block_number
                        ) VALUES ($1, $2, $3, $4)
                    """, 
                        metrics['miner_id'],
                        metrics['ipfs_repo_size'],
                        metrics['ipfs_storage_max'],
                        metrics['block_number']
                    )
                
                logger.info(f"Stored metrics for miner {metrics['miner_id']} at block {metrics['block_number']} (deleted {deleted} old records)")
                return True
                
        except Exception as e:
            logger.error(f"Error storing node metrics: {e}")
            return False
    
    async def process_message(self, message: aio_pika.IncomingMessage):
        """
        Process a single message from the queue.
        
        Args:
            message: The message to process
        """
        async with message.process():
            try:
                # Parse the message body
                metrics_data = json.loads(message.body.decode())
                logger.info(f"Processing metrics for miner {metrics_data.get('miner_id', 'unknown')}")
                
                # Store the metrics
                success = await self.store_node_metrics(metrics_data)
                
                if not success:
                    # Reject the message to retry later
                    await message.reject(requeue=True)
                    logger.warning("Failed to store metrics, message requeued")
                
            except json.JSONDecodeError as e:
                logger.error(f"Invalid JSON in message: {e}")
                # Don't requeue invalid messages
                await message.reject(requeue=False)
            except Exception as e:
                logger.error(f"Error processing message: {e}")
                # Requeue on unexpected errors
                await message.reject(requeue=True)
    
    async def start_consuming(self):
        """Start consuming messages from the queue."""
        # Declare the queue (in case it doesn't exist)
        queue = await self.rabbitmq_channel.declare_queue(
            self.queue_name,
            durable=True
        )
        
        logger.info(f"Starting to consume from queue '{self.queue_name}'")
        
        # Start consuming
        await queue.consume(self.process_message)
        
        # Keep the consumer running
        try:
            await asyncio.Future()
        except asyncio.CancelledError:
            logger.info("Consumer cancelled")
    
    async def close(self):
        """Close all connections."""
        if self.rabbitmq_connection:
            await self.rabbitmq_connection.close()
            logger.info("Closed RabbitMQ connection")
        
        if self.db_pool:
            await close_db_pool()
            logger.info("Closed database connection pool")


async def main():
    """Main entry point."""
    consumer = NodeMetricsConsumer()
    
    try:
        # Initialize connections
        await consumer.init_database()
        await consumer.connect_rabbitmq()
        
        # Start consuming
        await consumer.start_consuming()
        
    except KeyboardInterrupt:
        logger.info("Received interrupt signal")
    except Exception as e:
        logger.error(f"Error in consumer: {e}")
        raise
    finally:
        await consumer.close()


if __name__ == "__main__":
    asyncio.run(main()) 