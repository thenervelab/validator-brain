#!/usr/bin/env python3
"""
Network Self-Healing Consumer

This consumer processes file healing tasks from the queue and automatically
fixes broken file assignments by finding healthy miners to fill empty slots.

The consumer:
1. Reads healing messages from the network_self_healing queue
2. Analyzes files with broken assignments (empty miner slots)
3. Finds healthy miners to fill the empty slots
4. Updates the file_assignments table with the new assignments
5. Updates miner stats for newly assigned miners
"""

import asyncio
import json
import logging
import os
import sys
from datetime import datetime
from typing import Dict, List, Any, Optional

# Add parent directory to path for imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import aio_pika
from aio_pika import IncomingMessage
from dotenv import load_dotenv

from app.db.connection import init_db_pool, close_db_pool, get_db_pool

# Load environment variables
load_dotenv()

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class NetworkSelfHealingConsumer:
    def __init__(self):
        self.rabbitmq_url = os.getenv('RABBITMQ_URL', 'amqp://admin:admin@localhost:5672/')
        self.queue_name = 'network_self_healing'
        self.rabbitmq_connection = None
        self.rabbitmq_channel = None
        self.db_pool = None
        
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
    
    async def find_healthy_miners(self, exclude_miners: List[str], needed_count: int) -> List[str]:
        """
        Find healthy miners to fill empty assignment slots.
        
        Args:
            exclude_miners: Miners to exclude from selection (already assigned)
            needed_count: Number of miners needed
            
        Returns:
            List of miner node IDs
        """
        async with self.db_pool.acquire() as conn:
            # Get all online storage miners with stats
            all_miners = await conn.fetch("""
                SELECT 
                    r.node_id,
                    r.status,
                    COALESCE(ms.total_files_pinned, 0) as file_count,
                    COALESCE(ms.total_files_size_bytes, 0) as total_size,
                    COALESCE(ms.health_score, 0) as health_score
                FROM registration r
                LEFT JOIN miner_stats ms ON r.node_id = ms.node_id
                WHERE r.status = 'Online'
                AND r.node_type = 'StorageMiner'
                ORDER BY 
                    COALESCE(ms.health_score, 0) DESC,
                    COALESCE(ms.total_files_pinned, 0) ASC,
                    r.created_at ASC
            """)
            
            # Filter in Python for better readability
            exclude_set = set(exclude_miners)
            healthy_miners = []
            
            for miner in all_miners:
                # Skip already assigned miners
                if miner['node_id'] in exclude_set:
                    continue
                    
                # Check health threshold
                if miner['health_score'] < 20.0:
                    continue
                    
                # Check capacity limit (10GB)
                if miner['total_size'] >= 10737418240:
                    continue
                    
                healthy_miners.append(miner['node_id'])
                
                # Stop when we have enough
                if len(healthy_miners) >= needed_count:
                    break
            
            if len(healthy_miners) < needed_count:
                logger.warning(f"⚠️ Only found {len(healthy_miners)}/{needed_count} healthy miners meeting criteria")
                logger.warning(f"   Online miners: {len(all_miners)}, Health threshold: 20.0, Capacity limit: 10GB")
                logger.warning(f"   Excluded miners: {len(exclude_set)}")
                
                # Additional debugging info
                offline_count = len([m for m in all_miners if m['health_score'] < 20.0])
                overloaded_count = len([m for m in all_miners if m['total_size'] >= 10737418240])
                logger.warning(f"   Miners filtered out: {offline_count} unhealthy, {overloaded_count} overloaded")
            
            return healthy_miners
    
    async def process_file_healing(self, healing_data: Dict[str, Any]) -> bool:
        """
        Process a file healing task.
        
        Args:
            healing_data: Healing data from the queue
            
        Returns:
            True if processed successfully, False otherwise
        """
        cid = healing_data.get('cid')
        owner = healing_data.get('owner')
        filename = healing_data.get('filename', '')
        file_size_bytes = healing_data.get('file_size_bytes', 0)
        current_miners = healing_data.get('current_miners', [])
        
        if not cid or not owner:
            logger.error(f"Invalid healing data: missing cid or owner. Data: {healing_data}")
            return False
        
        logger.info(f"🛠️ Processing healing for file {filename} ({cid[:16]}...)")
        
        try:
            async with self.db_pool.acquire() as conn:
                async with conn.transaction():
                    # 1. Acquire advisory lock for this specific file (prevents parallel processing)
                    cid_hash = hash(cid) % 2147483647  # Convert CID to integer for advisory lock
                    lock_acquired = await conn.fetchval("SELECT pg_try_advisory_xact_lock($1)", cid_hash)
                    
                    if not lock_acquired:
                        logger.info(f"🔒 File {cid[:16]}... is being processed by another consumer, skipping")
                        return True  # Not an error, just skip
                    
                    # 2. Get current assignment state (now protected by advisory lock)
                    current_assignment = await conn.fetchrow("""
                        SELECT miner1, miner2, miner3, miner4, miner5, updated_at
                        FROM file_assignments
                        WHERE cid = $1
                    """, cid)
                    
                    if not current_assignment:
                        logger.error(f"❌ File assignment not found for CID {cid}")
                        return False
                    
                    # 3. Build current miner list and find empty slots
                    current_list = [
                        current_assignment['miner1'], current_assignment['miner2'],
                        current_assignment['miner3'], current_assignment['miner4'],
                        current_assignment['miner5']
                    ]
                    
                    # Find empty slots and assigned miners
                    empty_slots = []
                    assigned_miners = []
                    
                    for i, miner in enumerate(current_list):
                        if miner is None:
                            empty_slots.append(i)
                        else:
                            assigned_miners.append(miner)
                    
                    if not empty_slots:
                        logger.info(f"✅ File {cid[:16]}... already has all slots filled")
                        return True
                    
                    logger.info(f"🔍 File {cid[:16]}... has {len(empty_slots)} empty slots to fill")
                    
                    # 4. Find healthy miners to fill empty slots
                    new_miners = await self.find_healthy_miners(assigned_miners, len(empty_slots))
                    
                    if len(new_miners) < len(empty_slots):
                        logger.warning(f"⚠️ Only found {len(new_miners)} healthy miners for {len(empty_slots)} empty slots")
                    
                    # 5. Fill empty slots with new miners
                    updated_miners = current_list.copy()
                    for i, slot_index in enumerate(empty_slots):
                        if i < len(new_miners):
                            updated_miners[slot_index] = new_miners[i]
                    
                    # 6. Update file assignments (protected by advisory lock)
                    await conn.execute("""
                        UPDATE file_assignments
                        SET miner1 = $2, miner2 = $3, miner3 = $4, miner4 = $5, miner5 = $6,
                            updated_at = CURRENT_TIMESTAMP
                        WHERE cid = $1
                    """, cid, updated_miners[0], updated_miners[1], updated_miners[2], 
                        updated_miners[3], updated_miners[4])
                    
                    # 7. Update miner stats for newly assigned miners
                    for miner_id in new_miners:
                        if miner_id:  # Skip None values
                            await conn.execute("""
                                INSERT INTO miner_stats (
                                    node_id, total_files_pinned, total_files_size_bytes, updated_at
                                )
                                VALUES ($1, 1, $2, NOW())
                                ON CONFLICT (node_id) DO UPDATE SET
                                    total_files_pinned = miner_stats.total_files_pinned + 1,
                                    total_files_size_bytes = miner_stats.total_files_size_bytes + $2,
                                    updated_at = NOW()
                            """, miner_id, file_size_bytes)
                    
                    filled_count = len([m for m in new_miners if m])
                    miner_list = ', '.join([m[:20] + "..." for m in new_miners if m])
                    logger.info(f"✅ Successfully healed file {cid[:16]}... - "
                               f"filled {filled_count}/{len(empty_slots)} empty slots with miners: {miner_list}")
                    
                    # Log final assignment state for debugging
                    final_assignments = [m[:20] + "..." if m else "NULL" for m in updated_miners]
                    logger.debug(f"🔍 Final assignment state: [{', '.join(final_assignments)}]")
                    
                    # The updated_at timestamp change will automatically trigger user profile reconstruction
                    logger.info(f"📝 File assignment updated - user profile for {owner} will be reconstructed in next cycle")
                    
                    return True
                    
        except Exception as e:
            logger.error(f"❌ Error processing healing for file {cid}: {e}")
            logger.exception("Full traceback:")
            return False
    
    async def message_handler(self, message: IncomingMessage):
        """Handle incoming healing messages."""
        async with message.process():
            try:
                # Parse message
                healing_data = json.loads(message.body.decode())
                
                # Process the healing task
                success = await self.process_file_healing(healing_data)
                
                if success:
                    logger.debug(f"✅ Successfully processed healing for {healing_data.get('cid', 'unknown')}")
                else:
                    logger.error(f"❌ Failed to process healing for {healing_data.get('cid', 'unknown')}")
                
            except json.JSONDecodeError as e:
                logger.error(f"❌ Failed to decode message: {e}")
            except Exception as e:
                logger.error(f"❌ Error in message handler: {e}")
                logger.exception("Full traceback:")
    
    async def start_consuming(self):
        """Start consuming messages from the queue."""
        try:
            # Declare the queue (in case it doesn't exist)
            queue = await self.rabbitmq_channel.declare_queue(
                self.queue_name,
                durable=True
            )
            
            logger.info(f"🚀 Starting to consume from queue '{self.queue_name}'")
            
            # Start consuming
            await queue.consume(self.message_handler)
            
            # Keep the consumer running
            logger.info("🛠️ Network self-healing consumer is running. Press Ctrl+C to stop.")
            try:
                await asyncio.Future()  # Run forever
            except asyncio.CancelledError:
                logger.info("Consumer cancelled")
            
        except Exception as e:
            logger.error(f"❌ Error in consumer: {e}")
            raise
    
    async def close(self):
        """Close all connections."""
        if self.rabbitmq_connection:
            await self.rabbitmq_connection.close()
            logger.info("🔌 Closed RabbitMQ connection")


async def main():
    """Main entry point."""
    consumer = NetworkSelfHealingConsumer()
    
    try:
        # Initialize database pool
        await init_db_pool()
        consumer.db_pool = get_db_pool()
        logger.info("🗄️ Database connection pool initialized")
        
        # Connect to RabbitMQ
        await consumer.connect_rabbitmq()
        
        # Start consuming
        await consumer.start_consuming()
        
    except KeyboardInterrupt:
        logger.info("🛑 Shutting down network self-healing consumer...")
    except Exception as e:
        logger.error(f"❌ Error in consumer: {e}")
        raise
    finally:
        await consumer.close()
        await close_db_pool()


if __name__ == "__main__":
    asyncio.run(main()) 