#!/usr/bin/env python3
"""
Consumer that processes individual files from pinning requests.

This consumer:
1. Reads messages from the pinning_file_processing queue
2. Gets file size from IPFS
3. Stores file information in pending_assignment_file table
"""

import asyncio
import json
import logging
import os
import sys
from typing import Dict, Any

# Add parent directory to path for imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import aio_pika
from aio_pika import IncomingMessage

from app.db.connection import init_db_pool, close_db_pool
from app.db.models.pending_assignment_file import PendingAssignmentFile
from app.utils.file_utils import fetch_ipfs_file_size

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class PinningFileConsumer:
    def __init__(self):
        self.rabbitmq_url = os.getenv('RABBITMQ_URL', 'amqp://admin:admin@localhost:5672/')
        self.queue_name = 'pinning_file_processing'
        self.rabbitmq_connection = None
        self.rabbitmq_channel = None
    
    async def connect_rabbitmq(self):
        """Connect to RabbitMQ"""
        try:
            self.rabbitmq_connection = await aio_pika.connect_robust(self.rabbitmq_url)
            self.rabbitmq_channel = await self.rabbitmq_connection.channel()
            
            # Set prefetch count to process one message at a time
            await self.rabbitmq_channel.set_qos(prefetch_count=1)
            
            logger.info("Connected to RabbitMQ")
        except Exception as e:
            logger.error(f"Failed to connect to RabbitMQ: {e}")
            raise
    
    async def process_file(self, file_data: Dict[str, Any]) -> bool:
        """
        Process a single file: get its size and store in database.
        
        Args:
            file_data: Dictionary containing cid, filename, and owner
            
        Returns:
            True if processed successfully, False otherwise
        """
        cid = file_data.get('cid')
        filename = file_data.get('filename', '')
        owner = file_data.get('owner')
        
        if not cid or not owner:
            logger.error(f"Invalid file data: missing cid or owner. Data: {file_data}")
            return False
        
        logger.info(f"Processing file: {filename} ({cid[:16]}...) for owner {owner}")
        
        try:
            # Check if file already exists
            existing = await PendingAssignmentFile.get_by_cid(cid)
            if existing and existing.file_size_bytes and existing.file_size_bytes > 0:
                logger.info(f"File {cid} already exists with valid size, skipping")
                return True
            
            # If file exists but size is 0, we will re-fetch
            if existing:
                pending_file = existing
            else:
                # Create the pending assignment file record
                pending_file = await PendingAssignmentFile.create(
                    cid=cid,
                    owner=owner,
                    filename=filename
                )
            
            # Get file size from IPFS using our robust utility
            file_size = await fetch_ipfs_file_size(cid)
            
            if file_size is not None:
                # Update with file size
                await pending_file.update_size(file_size)
                logger.info(f"Successfully processed file {filename} ({cid[:16]}...) - Size: {file_size:,} bytes")
            else:
                # Mark as failed
                await pending_file.mark_failed("Could not get file size from IPFS")
                logger.warning(f"Failed to get size for file {filename} ({cid[:16]}...)")
            
            return True
            
        except Exception as e:
            logger.error(f"Error processing file {cid}: {e}")
            return False
    
    async def process_message(self, message: IncomingMessage) -> None:
        """Process a single message from the queue"""
        async with message.process():
            try:
                # Parse message
                file_data = json.loads(message.body.decode())
                logger.debug(f"Processing message: {json.dumps(file_data, indent=2)}")
                
                # Process the file
                success = await self.process_file(file_data)
                
                if not success:
                    cid = file_data.get('cid', 'unknown')
                    owner = file_data.get('owner', 'unknown')
                    raise Exception(f"Failed to process file {cid} for owner {owner}")
                
            except json.JSONDecodeError as e:
                logger.error(f"Invalid JSON in message: {e}")
                # Don't requeue invalid JSON messages
                return
            except Exception as e:
                logger.error(f"Error processing message: {e}")
                # Message will be requeued due to the exception
                raise
    
    async def start_consuming(self):
        """Start consuming messages from the queue"""
        queue = await self.rabbitmq_channel.declare_queue(
            self.queue_name,
            durable=True
        )
        
        logger.info(f"Starting to consume from queue '{self.queue_name}'")
        
        # Start consuming
        await queue.consume(self.process_message)
        
        # Keep the consumer running
        await asyncio.Future()
    
    async def close(self):
        """Close connections"""
        if self.rabbitmq_connection:
            await self.rabbitmq_connection.close()
            logger.info("Closed RabbitMQ connection")


async def main():
    consumer = PinningFileConsumer()
    
    try:
        # Initialize database pool
        await init_db_pool()
        logger.info("Database connection pool initialized")
        
        # Connect to RabbitMQ
        await consumer.connect_rabbitmq()
        
        # Start consuming
        await consumer.start_consuming()
        
    except KeyboardInterrupt:
        logger.info("Received interrupt signal, shutting down...")
    except Exception as e:
        logger.error(f"Error in consumer: {e}")
        raise
    finally:
        await consumer.close()
        await close_db_pool()


if __name__ == "__main__":
    asyncio.run(main()) 