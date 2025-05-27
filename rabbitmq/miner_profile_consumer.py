"""
Consumer for processing miner profiles from RabbitMQ queue.

This consumer:
1. Reads messages from the miner_profile queue
2. Fetches the profile data from IPFS using the CID
3. Parses the profile to extract file information
4. Updates the files table with miner's files
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
import httpx
from dotenv import load_dotenv

from app.db.connection import get_db_pool, init_db_pool, close_db_pool
from app.utils.config import get_ipfs_node_url
from substrate_fetcher.ipfs_profile_parser import parse_miner_profile_files

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class MinerProfileConsumer:
    """Consumer for processing miner profiles."""
    
    def __init__(self, rabbitmq_url: str = None, ipfs_gateway: str = None):
        """
        Initialize the consumer.
        
        Args:
            rabbitmq_url: URL of the RabbitMQ server
            ipfs_gateway: URL of the IPFS gateway
        """
        self.rabbitmq_url = rabbitmq_url or os.getenv('RABBITMQ_URL', 'amqp://admin:admin@localhost:5672/')
        # Use the centralized config for IPFS URL
        self.ipfs_gateway = ipfs_gateway or get_ipfs_node_url()
        # If it's a local IPFS node, we need to use the gateway endpoint
        if 'localhost' in self.ipfs_gateway or '127.0.0.1' in self.ipfs_gateway:
            # For local IPFS nodes, use port 8080 for gateway
            self.ipfs_gateway = self.ipfs_gateway.replace(':5001', ':8080')
        self.queue_name = 'miner_profile'
        
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
    
    async def fetch_from_ipfs(self, cid: str) -> Optional[bytes]:
        """
        Fetch content from IPFS using the gateway.
        
        Args:
            cid: Content ID to fetch
            
        Returns:
            Content bytes or None if failed
        """
        url = f"{self.ipfs_gateway}/ipfs/{cid}"
        
        async with httpx.AsyncClient(timeout=30.0, follow_redirects=True) as client:
            try:
                response = await client.get(url)
                if response.status_code == 200:
                    return response.content
                else:
                    logger.error(f"Failed to fetch CID {cid}: HTTP {response.status_code}")
                    return None
            except Exception as e:
                logger.error(f"Error fetching CID {cid}: {e}")
                return None
    
    async def process_miner_profile(self, node_id: str, cid: str) -> int:
        """
        Process a miner profile by fetching and parsing it.
        
        Args:
            node_id: Node ID of the miner
            cid: Content ID of the profile
            
        Returns:
            Number of files processed
        """
        logger.info(f"Processing miner profile for {node_id} with CID {cid}")
        
        # Check if this CID has already been parsed
        async with self.db_pool.acquire() as conn:
            existing = await conn.fetchrow("""
                SELECT id, file_count, parsed_at 
                FROM parsed_cids 
                WHERE cid = $1 AND profile_type = 'miner'
            """, cid)
            
            if existing:
                logger.info(f"CID {cid} already parsed at {existing['parsed_at']} with {existing['file_count']} files")
                return existing['file_count']
        
        # Fetch profile from IPFS
        profile_data = await self.fetch_from_ipfs(cid)
        if not profile_data:
            logger.error(f"Failed to fetch profile for {node_id}")
            return 0
        
        # Parse the profile
        try:
            # Parse JSON data
            profile_json = json.loads(profile_data)
            files = parse_miner_profile_files(profile_json)
            logger.info(f"Found {len(files)} files in profile for {node_id}")
        except Exception as e:
            logger.error(f"Failed to parse profile for {node_id}: {e}")
            return 0
        
        # Process each file
        processed_count = 0
        async with self.db_pool.acquire() as conn:
            for file_info in files:
                try:
                    # Extract file details
                    file_cid = file_info.get('file_hash')
                    file_size = file_info.get('file_size_in_bytes', 0)
                    owner = file_info.get('owner', 'unknown')
                    created_at = file_info.get('created_at', 0)
                    
                    if not file_cid:
                        logger.warning(f"File without CID in profile for {node_id}")
                        continue
                    
                    # Generate a file name based on CID if not available
                    file_name = f"miner_file_{file_cid[:8]}"
                    
                    # Insert into files table (skip if exists)
                    await conn.execute("""
                        INSERT INTO files (cid, name, size, created_date)
                        VALUES ($1, $2, $3, $4)
                        ON CONFLICT (cid) DO NOTHING
                    """, file_cid, file_name, file_size, datetime.utcnow())
                    
                    processed_count += 1
                    logger.debug(f"Processed file {file_cid} for miner {node_id}")
                    
                except Exception as e:
                    logger.error(f"Error processing file in profile for {node_id}: {e}")
                    continue
        
        logger.info(f"Successfully processed {processed_count} files for {node_id}")
        
        # Record that we've parsed this CID
        async with self.db_pool.acquire() as conn:
            await conn.execute("""
                INSERT INTO parsed_cids (cid, profile_type, file_count, account)
                VALUES ($1, $2, $3, $4)
                ON CONFLICT (cid, profile_type) DO UPDATE SET
                    file_count = EXCLUDED.file_count,
                    account = EXCLUDED.account,
                    parsed_at = CURRENT_TIMESTAMP
            """, cid, 'miner', processed_count, node_id)
            logger.info(f"Recorded parsed CID {cid} with {processed_count} files")
        
        return processed_count
    
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
                node_id = data.get('node_id')
                cid = data.get('cid')
                
                if not node_id or not cid:
                    logger.error(f"Invalid message format: {data}")
                    return
                
                # Process the profile
                await self.process_miner_profile(node_id, cid)
                
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
    consumer = MinerProfileConsumer()
    
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