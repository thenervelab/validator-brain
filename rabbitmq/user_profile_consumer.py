"""
Consumer for processing user profiles from RabbitMQ queue.

This consumer:
1. Reads messages from the user_profile queue
2. Fetches the profile data from IPFS using the CID
3. Parses the profile to extract file information
4. Updates the files and file_assignments tables
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
from app.utils.config import IPFS_GATEWAY
from substrate_fetcher.ipfs_profile_parser import parse_user_profile_files

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class UserProfileConsumer:
    """Consumer for processing user profiles."""
    
    def __init__(self, rabbitmq_url: str = None, ipfs_gateway: str = None):
        """
        Initialize the consumer.
        
        Args:
            rabbitmq_url: URL of the RabbitMQ server
            ipfs_gateway: URL of the IPFS gateway
        """
        self.rabbitmq_url = rabbitmq_url or os.getenv('RABBITMQ_URL', 'amqp://admin:admin@localhost:5672/')
        # Use the centralized config for IPFS URL
        self.ipfs_gateway = IPFS_GATEWAY
        self.queue_name = 'user_profile'
        
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
    
    async def process_user_profile(self, account: str, cid: str) -> int:
        """
        Process a user profile by fetching and parsing it.
        
        Args:
            account: SS58 address of the user
            cid: Content ID of the profile
            
        Returns:
            Number of files processed
        """
        logger.info(f"Processing user profile for {account} with CID {cid}")
        
        # Check if this CID has already been parsed
        async with self.db_pool.acquire() as conn:
            existing = await conn.fetchrow("""
                SELECT id, file_count, parsed_at 
                FROM parsed_cids 
                WHERE cid = $1 AND profile_type = 'user'
            """, cid)
            
            if existing:
                logger.info(f"CID {cid} already parsed at {existing['parsed_at']} with {existing['file_count']} files")
                return existing['file_count']
        
        # Fetch profile from IPFS
        profile_data = await self.fetch_from_ipfs(cid)
        if not profile_data:
            logger.error(f"Failed to fetch profile for {account}")
            return 0
        
        # Parse the profile
        try:
            # Parse JSON data
            profile_json = json.loads(profile_data)
            files = parse_user_profile_files(profile_json)
            logger.info(f"Found {len(files)} files in profile for {account}")
        except Exception as e:
            logger.error(f"Failed to parse profile for {account}: {e}")
            return 0
        
        # Process each file
        processed_count = 0
        async with self.db_pool.acquire() as conn:
            for file_info in files:
                try:
                    # Extract file details
                    file_cid = file_info.get('file_hash')  # Changed from 'cid' to 'file_hash'
                    file_name = file_info.get('file_name', 'unknown')
                    file_size = file_info.get('file_size_in_bytes', 0)  # Changed from 'size'
                    miner_ids = file_info.get('miner_ids', [])
                    
                    if not file_cid:
                        logger.warning(f"File without CID in profile for {account}")
                        continue
                    
                    # Insert into files table (skip if exists)
                    await conn.execute("""
                        INSERT INTO files (cid, name, size, created_date)
                        VALUES ($1, $2, $3, $4)
                        ON CONFLICT (cid) DO NOTHING
                    """, file_cid, file_name, file_size, datetime.utcnow())
                    
                    # Update file_assignments table
                    # Pad miner_ids to 5 elements
                    miners_padded = (miner_ids + [None] * 5)[:5]
                    
                    await conn.execute("""
                        INSERT INTO file_assignments (cid, owner, miner1, miner2, miner3, miner4, miner5)
                        VALUES ($1, $2, $3, $4, $5, $6, $7)
                        ON CONFLICT (cid) DO UPDATE SET
                            owner = EXCLUDED.owner,
                            miner1 = EXCLUDED.miner1,
                            miner2 = EXCLUDED.miner2,
                            miner3 = EXCLUDED.miner3,
                            miner4 = EXCLUDED.miner4,
                            miner5 = EXCLUDED.miner5
                    """, file_cid, account, miners_padded[0], miners_padded[1], 
                        miners_padded[2], miners_padded[3], miners_padded[4])
                    
                    processed_count += 1
                    logger.debug(f"Processed file {file_cid} for {account}")
                    
                except Exception as e:
                    logger.error(f"Error processing file in profile for {account}: {e}")
                    continue
        
        logger.info(f"Successfully processed {processed_count} files for {account}")
        
        # Record that we've parsed this CID
        async with self.db_pool.acquire() as conn:
            await conn.execute("""
                INSERT INTO parsed_cids (cid, profile_type, file_count, account)
                VALUES ($1, $2, $3, $4)
                ON CONFLICT (cid, profile_type) DO UPDATE SET
                    file_count = EXCLUDED.file_count,
                    account = EXCLUDED.account,
                    parsed_at = CURRENT_TIMESTAMP
            """, cid, 'user', processed_count, account)
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
                account = data.get('account')
                cid = data.get('cid')
                
                if not account or not cid:
                    logger.error(f"Invalid message format: {data}")
                    return
                
                # Process the profile
                await self.process_user_profile(account, cid)
                
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
    consumer = UserProfileConsumer()
    
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