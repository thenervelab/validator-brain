#!/usr/bin/env python3
"""
Processor that fetches user profiles from the database and queues them for reconstruction.
This processor reads user data from file_assignments table, groups files by owner,
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


class UserProfileReconstructionProcessor:
    def __init__(self):
        self.rabbitmq_url = os.getenv('RABBITMQ_URL', 'amqp://admin:admin@localhost:5672/')
        self.database_url = os.getenv('DATABASE_URL', 'postgresql://user:password@localhost:5432/substrate_fetcher')
        self.node_url = os.getenv('NODE_URL', 'wss://rpc.hippius.network')
        self.queue_name = 'user_profile_reconstruction'
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
    
    async def fetch_user_profiles_to_reconstruct(self) -> List[Dict[str, Any]]:
        """Fetch user profiles that need to be reconstructed from file_assignments"""
        # Get batch size from environment variable (0 means no limit)
        batch_size = int(os.getenv('USER_PROFILE_BATCH_SIZE', '100'))
        
        async with self.db_pool.acquire() as conn:
            # Get users who either:
            # 1. Don't have any published profile yet, OR
            # 2. Have new files since their last profile was published
            query = """
            WITH user_file_stats AS (
                SELECT 
                    fa.owner,
                    COUNT(*) as current_file_count,
                    SUM(COALESCE(f.size, 0)) as current_total_size,
                    MAX(fa.updated_at) as latest_file_update
                FROM file_assignments fa
                LEFT JOIN files f ON f.cid = fa.cid
                WHERE fa.owner IS NOT NULL
                GROUP BY fa.owner
            ),
            latest_profiles AS (
                SELECT DISTINCT ON (owner) 
                    owner,
                    files_count,
                    files_size,
                    created_at as profile_created_at
                FROM pending_user_profile 
                WHERE status = 'published'
                ORDER BY owner, created_at DESC
            )
            SELECT DISTINCT
                ufs.owner,
                ufs.current_file_count,
                ufs.current_total_size,
                ufs.latest_file_update,
                COALESCE(lp.files_count, 0) as last_profile_file_count,
                COALESCE(lp.files_size, 0) as last_profile_total_size,
                lp.profile_created_at
            FROM user_file_stats ufs
            LEFT JOIN latest_profiles lp ON lp.owner = ufs.owner
            WHERE 
                -- No published profile yet
                lp.owner IS NULL
                OR 
                -- File count changed
                ufs.current_file_count != COALESCE(lp.files_count, 0)
                OR 
                -- Total size changed
                ufs.current_total_size != COALESCE(lp.files_size, 0)
                OR
                -- New files added since last profile (if we have timestamps)
                (lp.profile_created_at IS NOT NULL AND ufs.latest_file_update > lp.profile_created_at)
            ORDER BY ufs.owner
            """
            
            if batch_size > 0:
                query += f" LIMIT {batch_size}"
            
            users_rows = await conn.fetch(query)
            
            # Log what we found for debugging
            for row in users_rows:
                if row['last_profile_file_count'] > 0:
                    logger.info(f"User {row['owner']} needs profile update: "
                              f"files {row['last_profile_file_count']} -> {row['current_file_count']}, "
                              f"size {row['last_profile_total_size']} -> {row['current_total_size']}")
                else:
                    logger.info(f"User {row['owner']} needs initial profile: "
                              f"{row['current_file_count']} files, {row['current_total_size']} bytes")
            
            return [{'owner': row['owner']} for row in users_rows]
    
    async def fetch_user_profile_files(self, owner: str) -> List[Dict[str, Any]]:
        """Fetch all files owned by a specific user and convert to proper format"""
        async with self.db_pool.acquire() as conn:
            rows = await conn.fetch("""
                SELECT DISTINCT
                    f.cid,
                    f.name,
                    f.size,
                    f.created_date,
                    fa.miner1,
                    fa.miner2,
                    fa.miner3,
                    fa.miner4,
                    fa.miner5,
                    fa.updated_at
                FROM files f
                JOIN file_assignments fa ON f.cid = fa.cid
                WHERE fa.owner = $1
                ORDER BY f.created_date ASC
            """, owner)
            
            # Convert to proper format and handle datetime serialization
            files = []
            for row in rows:
                # Collect assigned miners (filter out None values)
                miner_ids = [
                    miner for miner in [
                        row['miner1'], row['miner2'], row['miner3'], 
                        row['miner4'], row['miner5']
                    ] if miner is not None
                ]
                
                file_data = {
                    'cid': row['cid'],
                    'name': row['name'],
                    'size': row['size'],
                    'miner_ids': miner_ids,
                    'total_replicas': len(miner_ids)
                }
                
                # Convert datetime to string if present
                if row['created_date']:
                    file_data['created_date'] = row['created_date'].isoformat()
                if row['updated_at']:
                    file_data['last_charged_at'] = row['updated_at'].isoformat()
                
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
        
        logger.debug(f"Sent profile to queue: {profile_data['owner']} -> {profile_data['cid']}")
    
    async def process_profiles(self):
        """Main processing loop"""
        profiles = await self.fetch_user_profiles_to_reconstruct()
        
        if not profiles:
            logger.info("No user profiles to reconstruct")
            return
        
        logger.info(f"Found {len(profiles)} user profiles to reconstruct")
        
        for profile in profiles:
            try:
                owner = profile['owner']
                
                # Fetch files for this user
                files = await self.fetch_user_profile_files(owner)
                
                # Calculate file count and total size
                file_count = len(files)
                total_size = sum(file_data.get('size', 0) for file_data in files)
                
                # Skip users with no files
                if file_count == 0:
                    logger.info(f"Skipping user {owner} - no files assigned")
                    continue
                
                # Generate a synthetic CID for the profile (we'll use the owner as base)
                profile_cid = f"user_profile_{owner}"
                
                # Prepare message data
                message_data = {
                    'cid': profile_cid,
                    'owner': owner,
                    'file_count': file_count,
                    'files': files,
                    'total_size': total_size,
                    'block_number': self.current_block
                }
                
                # Send to queue
                await self.send_to_queue(message_data)
                
                logger.info(f"Queued profile for user {owner}: {file_count} files, {total_size} bytes")
                
            except Exception as e:
                logger.error(f"Error processing profile for user {profile['owner']}: {e}")
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
    processor = UserProfileReconstructionProcessor()
    
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