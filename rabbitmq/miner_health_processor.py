#!/usr/bin/env python3
"""
Simple Miner Health Processor

This processor fetches active miners and queues them for basic health checks.
The health checks will test miner connectivity and remove failed miners from file assignments.
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
from aio_pika import Message
from dotenv import load_dotenv
from substrateinterface import SubstrateInterface

from app.db.connection import init_db_pool, close_db_pool, get_db_pool
from app.utils.config import NODE_URL

# Load environment variables
load_dotenv()

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class MinerHealthProcessor:
    def __init__(self):
        self.rabbitmq_url = os.getenv('RABBITMQ_URL', 'amqp://admin:admin@localhost:5672/')
        self.queue_name = 'miner_health_check'
        self.rabbitmq_connection = None
        self.rabbitmq_channel = None
        self.substrate = None
        self.db_pool = None
        
        # Configuration
        self.max_files_per_miner = int(os.getenv('HEALTH_CHECK_FILES_PER_MINER', '3'))
        
    def connect_substrate(self):
        """Connect to the substrate chain."""
        logger.info(f"Connecting to substrate at {NODE_URL}")
        self.substrate = SubstrateInterface(url=NODE_URL)
        logger.info("Connected to substrate")
        
    async def connect_rabbitmq(self):
        """Connect to RabbitMQ and declare the queue."""
        try:
            self.rabbitmq_connection = await aio_pika.connect_robust(self.rabbitmq_url)
            self.rabbitmq_channel = await self.rabbitmq_connection.channel()
            
            # Declare the queue
            await self.rabbitmq_channel.declare_queue(
                self.queue_name,
                durable=True
            )
            
            logger.info(f"Connected to RabbitMQ and declared queue '{self.queue_name}'")
        except Exception as e:
            logger.error(f"Failed to connect to RabbitMQ: {e}")
            raise
    
    def get_current_epoch(self) -> int:
        """Get the current epoch from the blockchain."""
        try:
            if not self.substrate:
                self.connect_substrate()
            
            block_number = self.substrate.get_block_number(None)
            epoch = block_number // 100  # Assuming 100 blocks per epoch
            logger.info(f"Current block: {block_number}, epoch: {epoch}")
            return epoch
        except Exception as e:
            logger.error(f"Error getting current epoch: {e}")
            return 0
    
    async def get_miners_for_health_check(self) -> List[Dict[str, Any]]:
        """Get active miners with their assigned files for health checking."""
        async with self.db_pool.acquire() as conn:
            # Get miners with their assigned files
            rows = await conn.fetch("""
                WITH miner_files AS (
                    SELECT 
                        r.node_id,
                        r.ipfs_peer_id,
                        ARRAY_AGG(DISTINCT fa.cid) FILTER (WHERE fa.cid IS NOT NULL) as assigned_files
                    FROM registration r
                    LEFT JOIN file_assignments fa ON (
                        r.node_id = fa.miner1 OR r.node_id = fa.miner2 OR 
                        r.node_id = fa.miner3 OR r.node_id = fa.miner4 OR r.node_id = fa.miner5
                    )
                    WHERE r.node_type = 'StorageMiner' 
                      AND r.status = 'active'
                      AND r.ipfs_peer_id IS NOT NULL
                    GROUP BY r.node_id, r.ipfs_peer_id
                )
                SELECT 
                    mf.node_id,
                    mf.ipfs_peer_id,
                    COALESCE(mf.assigned_files, ARRAY[]::text[]) as assigned_files,
                    COALESCE(array_length(mf.assigned_files, 1), 0) as file_count
                FROM miner_files mf
                ORDER BY mf.node_id
            """)
            
            return [dict(row) for row in rows]
    
    async def queue_health_check(self, miner_data: Dict[str, Any], epoch: int, block_number: int) -> None:
        """Queue a health check message for a miner."""
        node_id = miner_data['node_id']
        ipfs_peer_id = miner_data['ipfs_peer_id']
        assigned_files = miner_data['assigned_files'] or []
        
        # Limit files to check
        files_to_check = assigned_files[:self.max_files_per_miner] if assigned_files else []
        
        message_data = {
            'node_id': node_id,
            'ipfs_peer_id': ipfs_peer_id,
            'epoch': epoch,
            'block_number': block_number,
            'files_to_check': files_to_check,
            'total_files_assigned': len(assigned_files),
            'timestamp': datetime.utcnow().isoformat()
        }
        
        message_body = json.dumps(message_data).encode()
        
        await self.rabbitmq_channel.default_exchange.publish(
            Message(
                body=message_body,
                delivery_mode=aio_pika.DeliveryMode.PERSISTENT
            ),
            routing_key=self.queue_name
        )
        
        logger.debug(f"Queued health check for miner {node_id} with {len(files_to_check)} files to check")
    
    async def _queue_health_checks_parallel(self, miners: List[Dict[str, Any]], epoch: int, block_number: int) -> int:
        semaphore = asyncio.Semaphore(20)
        
        async def _queue_single_health_check(miner: Dict[str, Any]) -> bool:
            async with semaphore:
                try:
                    await self.queue_health_check(miner, epoch, block_number)
                    return True
                except Exception as e:
                    logger.error(f"Failed to queue health check for miner {miner['node_id']}: {e}")
                    return False
        
        tasks = [_queue_single_health_check(miner) for miner in miners]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        successful = sum(1 for r in results if r is True)
        failed = len(results) - successful
        
        if failed > 0:
            logger.warning(f"Parallel health check queuing: {successful} succeeded, {failed} failed")
        else:
            logger.info(f"Parallel health check queuing: {successful}/{len(miners)} miners queued successfully")
        
        return successful
    
    async def process_miner_health_checks(self) -> None:
        """Main processing function to queue miner health checks."""
        try:
            # Get current epoch and block
            current_epoch = self.get_current_epoch()
            current_block = self.substrate.get_block_number(None) if self.substrate else 0
            
            # Get miners for health checking
            miners = await self.get_miners_for_health_check()
            if not miners:
                logger.warning("No miners found for health checking")
                return
            
            logger.info(f"Found {len(miners)} miners for health checking in epoch {current_epoch}")
            
            # Queue health checks for all miners in parallel
            if miners:
                queued_count = await self._queue_health_checks_parallel(miners, current_epoch, current_block)
                logger.info(f"📦 Batch queued {queued_count}/{len(miners)} health checks for epoch {current_epoch}")
            
        except Exception as e:
            logger.error(f"Error in miner health check processing: {e}")
            raise
    
    async def close(self):
        """Close all connections."""
        if self.rabbitmq_connection:
            await self.rabbitmq_connection.close()
            logger.info("Closed RabbitMQ connection")
        
        if self.substrate:
            self.substrate.close()
            logger.info("Closed substrate connection")


async def main():
    """Main entry point."""
    processor = MinerHealthProcessor()
    
    try:
        # Initialize database pool
        await init_db_pool()
        processor.db_pool = get_db_pool()
        logger.info("Database connection pool initialized")
        
        # Connect to services
        processor.connect_substrate()
        await processor.connect_rabbitmq()
        
        # Process miner health checks
        await processor.process_miner_health_checks()
        
        logger.info("Completed miner health check processing")
        
    except Exception as e:
        logger.error(f"Error in processor: {e}")
        raise
    finally:
        await processor.close()
        await close_db_pool()


if __name__ == "__main__":
    asyncio.run(main()) 