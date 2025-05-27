#!/usr/bin/env python3
"""
Processor that fetches miner profiles from the database and queues them for reconstruction.
This processor reads miner profiles that have been parsed and stored in the database,
and sends them to a queue for reconstruction and publishing to IPFS.
"""

import asyncio
import json
import logging
import os
from typing import Dict, List, Any

import aio_pika
import asyncpg
from aio_pika import Message
from substrateinterface import SubstrateInterface

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class MinerProfileReconstructionProcessor:
    def __init__(self):
        self.rabbitmq_url = os.getenv('RABBITMQ_URL', 'amqp://admin:admin@localhost:5672/')
        self.database_url = os.getenv('DATABASE_URL', 'postgresql://user:password@localhost:5432/substrate_fetcher')
        self.node_url = os.getenv('NODE_URL', 'wss://rpc.hippius.network')
        self.queue_name = 'miner_profile_reconstruction'
        self.rabbitmq_connection = None
        self.rabbitmq_channel = None
        self.db_pool = None
        self.current_block = None
    
    async def connect_database(self):
        """Connect to PostgreSQL database"""
        try:
            self.db_pool = await asyncpg.create_pool(
                self.database_url,
                min_size=1,
                max_size=10
            )
            logger.info("Connected to database")
        except Exception as e:
            logger.error(f"Failed to connect to database: {e}")
            raise
    
    async def connect_rabbitmq(self):
        """Connect to RabbitMQ and declare queue"""
        try:
            self.rabbitmq_connection = await aio_pika.connect_robust(self.rabbitmq_url)
            self.rabbitmq_channel = await self.rabbitmq_connection.channel()
            
            # Declare queue
            await self.rabbitmq_channel.declare_queue(
                self.queue_name,
                durable=True
            )
            
            logger.info(f"Connected to RabbitMQ and declared queue '{self.queue_name}'")
        except Exception as e:
            logger.error(f"Failed to connect to RabbitMQ: {e}")
            raise
    
    async def fetch_current_block(self):
        """Fetch the current block number from the substrate chain"""
        try:
            substrate = SubstrateInterface(url=self.node_url)
            block_hash = substrate.get_chain_head()
            block_number = substrate.get_block_number(block_hash)
            self.current_block = block_number
            logger.info(f"Current block number: {self.current_block}")
            substrate.close()
        except Exception as e:
            logger.error(f"Failed to fetch current block: {e}")
            # Use a default block number if we can't fetch it
            self.current_block = 0
    
    async def fetch_miner_profiles_to_reconstruct(self) -> List[Dict[str, Any]]:
        """Fetch miner profiles that need to be reconstructed from file_assignments"""
        # Get batch size from environment variable (0 means no limit)
        batch_size = int(os.getenv('MINER_PROFILE_BATCH_SIZE', '100'))
        
        async with self.db_pool.acquire() as conn:
            if batch_size > 0:
                # Get limited number of miners
                miners_rows = await conn.fetch("""
                    SELECT DISTINCT miner_id as node_id
                    FROM node_metrics 
                    WHERE miner_id IS NOT NULL
                    AND miner_id NOT IN (
                        SELECT node_id FROM pending_miner_profile 
                        WHERE node_id IS NOT NULL AND status = 'published'
                    )
                    ORDER BY miner_id
                    LIMIT $1
                """, batch_size)
            else:
                # Get all miners (no limit)
                miners_rows = await conn.fetch("""
                    SELECT DISTINCT miner_id as node_id
                    FROM node_metrics 
                    WHERE miner_id IS NOT NULL
                    AND miner_id NOT IN (
                        SELECT node_id FROM pending_miner_profile 
                        WHERE node_id IS NOT NULL AND status = 'published'
                    )
                    ORDER BY miner_id
                """)
            
            return [dict(row) for row in miners_rows]
    
    async def fetch_miner_profile_files(self, node_id: str) -> List[Dict[str, Any]]:
        """Fetch all files assigned to a specific miner and convert to proper format"""
        async with self.db_pool.acquire() as conn:
            rows = await conn.fetch("""
                SELECT DISTINCT
                    f.cid,
                    f.name,
                    f.size,
                    f.created_date
                FROM files f
                JOIN file_assignments fa ON f.cid = fa.cid
                WHERE $1 IN (fa.miner1, fa.miner2, fa.miner3, fa.miner4, fa.miner5)
                ORDER BY f.created_date ASC
            """, node_id)
            
            # Convert to proper format and handle datetime serialization
            files = []
            for row in rows:
                file_data = {
                    'cid': row['cid'],
                    'name': row['name'],
                    'size': row['size']
                }
                # Convert datetime to string if present
                if row['created_date']:
                    file_data['created_date'] = row['created_date'].isoformat()
                
                files.append(file_data)
            
            return files
    
    async def send_to_queue(self, profile_data: Dict[str, Any]) -> None:
        """Send profile data to RabbitMQ queue"""
        message_body = json.dumps(profile_data)
        message = Message(
            body=message_body.encode(),
            delivery_mode=2  # Make message persistent
        )
        
        await self.rabbitmq_channel.default_exchange.publish(
            message,
            routing_key=self.queue_name
        )
        
        logger.debug(f"Sent profile to queue: {profile_data['node_id']} -> {profile_data['cid']}")
    
    async def process_profiles(self):
        """Main processing loop"""
        profiles = await self.fetch_miner_profiles_to_reconstruct()
        
        if not profiles:
            logger.info("No miner profiles to reconstruct")
            return
        
        logger.info(f"Found {len(profiles)} miner profiles to reconstruct")
        
        for profile in profiles:
            try:
                node_id = profile['node_id']
                
                # Fetch files for this miner
                files = await self.fetch_miner_profile_files(node_id)
                
                # Calculate file count and total size
                file_count = len(files)
                total_size = sum(file_data.get('size', 0) for file_data in files)
                
                # Skip miners with no files
                if file_count == 0:
                    logger.info(f"Skipping miner {node_id} - no files assigned")
                    continue
                
                # Generate a synthetic CID for the profile (we'll use the node_id as base)
                profile_cid = f"profile_{node_id}"
                
                # Prepare message data
                message_data = {
                    'cid': profile_cid,
                    'node_id': node_id,
                    'file_count': file_count,
                    'files': files,
                    'total_size': total_size,
                    'block_number': self.current_block
                }
                
                # Send to queue
                await self.send_to_queue(message_data)
                
                logger.info(f"Queued profile for miner {node_id}: {file_count} files, {total_size} bytes")
                
            except Exception as e:
                logger.error(f"Error processing profile for miner {profile['node_id']}: {e}")
                continue
        
        logger.info(f"Successfully queued {len(profiles)} profiles for reconstruction")
    
    async def close(self):
        """Close connections"""
        if self.rabbitmq_connection:
            await self.rabbitmq_connection.close()
            logger.info("Closed RabbitMQ connection")
        
        if self.db_pool:
            await self.db_pool.close()
            logger.info("Closed database connection")


async def main():
    processor = MinerProfileReconstructionProcessor()
    
    try:
        # Connect to services
        await processor.connect_database()
        await processor.connect_rabbitmq()
        
        # Fetch current block number
        await processor.fetch_current_block()
        
        # Process profiles
        await processor.process_profiles()
        
    except Exception as e:
        logger.error(f"Error in processor: {e}")
        raise
    finally:
        await processor.close()


if __name__ == "__main__":
    asyncio.run(main()) 