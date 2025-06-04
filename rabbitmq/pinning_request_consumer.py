"""
Consumer for processing pinning requests from RabbitMQ queue.

This consumer:
1. Reads messages from the pinning_request queue
2. Checks if the request has already been processed
3. Decodes the file hash from hex to CID
4. Updates the pinning_requests table
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

from app.db.connection import get_db_pool, init_db_pool, close_db_pool

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def hex_to_string(hex_string: str) -> str:
    """
    Convert a hex string to its ASCII representation.
    
    Args:
        hex_string: Hex string to convert
        
    Returns:
        ASCII string
    """
    try:
        # Remove '0x' prefix if present
        if hex_string.startswith('0x'):
            hex_string = hex_string[2:]
        
        # Convert hex to bytes then to string
        return bytes.fromhex(hex_string).decode('utf-8')
    except Exception as e:
        logger.error(f"Error converting hex to string: {e}")
        return hex_string


class PinningRequestConsumer:
    """Consumer for processing pinning requests."""
    
    def __init__(self, rabbitmq_url: str = None):
        """
        Initialize the consumer.
        
        Args:
            rabbitmq_url: URL of the RabbitMQ server
        """
        self.rabbitmq_url = rabbitmq_url or os.getenv('RABBITMQ_URL', 'amqp://admin:admin@localhost:5672/')
        self.queue_name = 'pinning_request'
        
        self.rabbitmq_connection = None
        self.rabbitmq_channel = None
        self.db_pool = None
        
    async def connect(self):
        """Connect to RabbitMQ and database."""
        try:
            # Initialize database pool
            await init_db_pool()
            self.db_pool = get_db_pool()
            logger.info("Connected to database")
            
            # Connect to RabbitMQ
            self.rabbitmq_connection = await aio_pika.connect_robust(self.rabbitmq_url)
            self.rabbitmq_channel = await self.rabbitmq_connection.channel()
            
            # Set prefetch count to process one message at a time
            await self.rabbitmq_channel.set_qos(prefetch_count=1)
            
            logger.info(f"Connected to RabbitMQ")
            
        except Exception as e:
            logger.error(f"Failed to connect: {e}")
            raise
    
    async def process_pinning_request(self, request_data: Dict[str, Any]) -> bool:
        """
        Process a pinning request.
        
        Args:
            request_data: The request data from the queue
            
        Returns:
            True if processed successfully, False otherwise
        """
        request_hash = request_data.get('request_hash')
        owner = request_data.get('owner')
        
        if not request_hash or not owner:
            logger.error(f"Invalid request data: missing request_hash or owner. Data: {request_data}")
            return False
        
        logger.info(f"Processing pinning request: {owner} -> {request_hash[:16]}...")
        
        # Check if this request has already been processed
        async with self.db_pool.acquire() as conn:
            existing = await conn.fetchrow("""
                SELECT id, processed_at 
                FROM processed_pinning_requests 
                WHERE request_hash = $1
            """, request_hash)
            
            if existing:
                logger.info(f"Request {request_hash[:16]}... already processed at {existing['processed_at']}")
                return True
        
        try:
            # Convert file_hash from hex to CID
            file_hash_hex = request_data.get('file_hash', '')
            file_cid = hex_to_string(file_hash_hex) if file_hash_hex else ''
            
            # Extract miner IDs - handle None case
            miner_ids = request_data.get('miner_ids', [])
            if miner_ids is None:
                miner_ids = []
            miner_count = len(miner_ids)
            
            async with self.db_pool.acquire() as conn:
                # Insert or update the pinning request
                await conn.execute("""
                    INSERT INTO pinning_requests (
                        request_hash, owner, file_hash, file_name, 
                        total_replicas, is_assigned, selected_validator,
                        created_at, last_charged_at
                    )
                    VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
                    ON CONFLICT (request_hash) DO UPDATE SET
                        owner = EXCLUDED.owner,
                        file_hash = EXCLUDED.file_hash,
                        file_name = EXCLUDED.file_name,
                        total_replicas = EXCLUDED.total_replicas,
                        is_assigned = EXCLUDED.is_assigned,
                        selected_validator = EXCLUDED.selected_validator,
                        last_charged_at = EXCLUDED.last_charged_at,
                        updated_at = CURRENT_TIMESTAMP
                """, 
                    request_hash,
                    owner,
                    file_cid,
                    request_data.get('file_name', ''),
                    request_data.get('total_replicas', 0),
                    request_data.get('is_assigned', False),
                    request_data.get('selected_validator', ''),
                    request_data.get('created_at', 0),
                    request_data.get('last_charged_at', 0)
                )
                
                # If we have a valid file CID, handle assignment properly
                if file_cid:
                    # Always ensure the file exists in the files table
                    file_name = request_data.get('file_name', '') or f"file_{file_cid[:8]}"
                    await conn.execute("""
                        INSERT INTO files (cid, name, size)
                        VALUES ($1, $2, $3)
                        ON CONFLICT (cid) DO UPDATE SET
                            name = EXCLUDED.name,
                            size = EXCLUDED.size
                    """, file_cid, file_name, 0)  # Default size to 0, will be updated by file processor
                    
                    if miner_ids and len(miner_ids) >= 3:  # Have sufficient pre-assigned miners
                        # Case 1: Storage request has sufficient pre-assigned miners (use them)
                        miners_padded = (miner_ids + [None] * 5)[:5]
                        logger.info(f"✅ Creating assignment with {len(miner_ids)} pre-assigned miners")
                        
                        await conn.execute("""
                            INSERT INTO file_assignments (cid, owner, miner1, miner2, miner3, miner4, miner5)
                            VALUES ($1, $2, $3, $4, $5, $6, $7)
                            ON CONFLICT (cid) DO UPDATE SET
                                owner = EXCLUDED.owner,
                                miner1 = EXCLUDED.miner1,
                                miner2 = EXCLUDED.miner2,
                                miner3 = EXCLUDED.miner3,
                                miner4 = EXCLUDED.miner4,
                                miner5 = EXCLUDED.miner5,
                                updated_at = CURRENT_TIMESTAMP
                        """, file_cid, owner, miners_padded[0], miners_padded[1], 
                            miners_padded[2], miners_padded[3], miners_padded[4])
                    else:
                        # Case 2: Insufficient or no miners - queue for assignment phase  
                        logger.info(f"📋 Queuing file for assignment phase (has {len(miner_ids)} miners, need ≥3)")
                        
                        # Add to pending_assignment_file for proper assignment
                        await conn.execute("""
                            INSERT INTO pending_assignment_file (cid, owner, filename, status)
                            VALUES ($1, $2, $3, 'pending')
                            ON CONFLICT (cid) DO UPDATE SET
                                owner = EXCLUDED.owner,
                                filename = EXCLUDED.filename,
                                status = 'pending'
                        """, file_cid, owner, file_name)
                        
                        # DO NOT create file_assignments entry with NULL miners
                        logger.info(f"✅ File queued for assignment phase - will get proper miners during assignment")
                
                # Record that we've processed this request
                await conn.execute("""
                    INSERT INTO processed_pinning_requests (request_hash, miner_count)
                    VALUES ($1, $2)
                    ON CONFLICT (request_hash) DO UPDATE SET
                        processed_at = CURRENT_TIMESTAMP,
                        miner_count = EXCLUDED.miner_count
                """, request_hash, miner_count)
                
                logger.info(f"✅ Successfully processed pinning request {request_hash[:16]}...")
                if miner_ids:
                    logger.info(f"   📋 Created assignment with {miner_count} pre-assigned miners")
                else:
                    logger.info(f"   📝 Created empty assignment entry - miners will be assigned in assignment phase")
                logger.info(f"   📁 File: {file_cid[:16]}... added to assignment queue")
                return True
                
        except Exception as e:
            logger.error(f"Error processing pinning request for {owner} (hash: {request_hash[:16] if request_hash else 'unknown'}...): {e}")
            logger.exception("Full traceback:")
            return False
    
    async def process_message(self, message: aio_pika.IncomingMessage):
        """
        Process a single message from the queue.
        
        Args:
            message: The message to process
        """
        async with message.process():
            try:
                # Parse message body
                data = json.loads(message.body.decode())
                logger.debug(f"Processing message: {json.dumps(data, indent=2)}")
                
                # Process the pinning request
                success = await self.process_pinning_request(data)
                
                if not success:
                    # Reject and requeue if processing failed
                    request_hash = data.get('request_hash', 'unknown')
                    owner = data.get('owner', 'unknown')
                    raise Exception(f"Failed to process pinning request for owner {owner}, hash {request_hash[:16] if request_hash != 'unknown' else 'unknown'}...")
                
            except json.JSONDecodeError as e:
                logger.error(f"Invalid JSON in message: {e}")
                # Don't requeue invalid JSON messages
                return
            except Exception as e:
                logger.error(f"Error processing message: {e}")
                # Message will be requeued due to the exception
                raise
    
    async def start_consuming(self):
        """Start consuming messages from the queue."""
        try:
            # Declare the queue
            queue = await self.rabbitmq_channel.declare_queue(
                self.queue_name,
                durable=True
            )
            
            logger.info(f"Starting to consume from queue '{self.queue_name}'")
            
            # Start consuming
            await queue.consume(self.process_message)
            
            # Keep the consumer running
            await asyncio.Future()
            
        except Exception as e:
            logger.error(f"Error in consumer: {e}")
            raise
    
    async def close(self):
        """Close all connections."""
        if self.rabbitmq_connection:
            await self.rabbitmq_connection.close()
            logger.info("Closed RabbitMQ connection")
        
        if self.db_pool:
            await close_db_pool()
            logger.info("Closed database pool")


async def main():
    """Main entry point for the consumer."""
    consumer = PinningRequestConsumer()
    
    try:
        # Connect to services
        await consumer.connect()
        
        # Start consuming
        await consumer.start_consuming()
        
    except KeyboardInterrupt:
        logger.info("Received interrupt signal, shutting down...")
    except Exception as e:
        logger.error(f"Consumer error: {e}")
    finally:
        await consumer.close()


if __name__ == "__main__":
    asyncio.run(main()) 