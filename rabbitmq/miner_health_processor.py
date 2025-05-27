"""
Processor for queuing miners for health checks.

This processor:
1. Fetches miners from the database (from file_assignments or node_metrics)
2. Gets the current epoch from the blockchain
3. Queues miners for ping and pin health checks
4. Sends messages to the miner_health_check queue
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
from aio_pika import Message
from dotenv import load_dotenv
from substrateinterface import SubstrateInterface

from app.db.connection import get_db_pool, init_db_pool, close_db_pool
from app.utils.config import NODE_URL

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class MinerHealthProcessor:
    """Processor for queuing miners for health checks."""
    
    def __init__(self):
        self.rabbitmq_url = os.getenv('RABBITMQ_URL', 'amqp://admin:admin@localhost:5672/')
        self.queue_name = 'miner_health_check'
        self.node_url = NODE_URL
        
        self.rabbitmq_connection = None
        self.rabbitmq_channel = None
        self.db_pool = None
        self.substrate = None
        
    async def connect(self):
        """Connect to database, RabbitMQ, and substrate."""
        try:
            # Initialize database pool
            await init_db_pool()
            self.db_pool = get_db_pool()
            logger.info("Connected to database")
            
            # Connect to RabbitMQ
            self.rabbitmq_connection = await aio_pika.connect_robust(self.rabbitmq_url)
            self.rabbitmq_channel = await self.rabbitmq_connection.channel()
            
            # Declare the queue
            await self.rabbitmq_channel.declare_queue(
                self.queue_name,
                durable=True
            )
            
            logger.info("Connected to RabbitMQ")
            
            # Connect to substrate
            self.substrate = SubstrateInterface(url=self.node_url)
            logger.info(f"Connected to substrate at {self.node_url}")
            
        except Exception as e:
            logger.error(f"Failed to connect: {e}")
            raise
    
    async def disconnect(self):
        """Disconnect from all services."""
        if self.rabbitmq_connection:
            await self.rabbitmq_connection.close()
        if self.db_pool:
            await close_db_pool()
        if self.substrate:
            self.substrate.close()
    
    def get_current_epoch(self) -> int:
        """Get the current epoch from the blockchain."""
        try:
            # Get current block number (no arguments needed for latest block)
            block_number = self.substrate.get_block_number(None)
            
            # Calculate epoch (assuming 100 blocks per epoch)
            epoch = block_number // 100
            
            logger.info(f"Current block: {block_number}, Current epoch: {epoch}")
            return epoch
            
        except Exception as e:
            logger.error(f"Error getting current epoch: {e}")
            # Fallback to a default epoch
            return 0
    
    async def fetch_miners_for_health_check(self) -> List[Dict[str, Any]]:
        """
        Fetch miners that need health checks.
        
        We'll get miners from multiple sources:
        1. Miners with files assigned in file_assignments
        2. Miners from node_metrics (active miners)
        """
        miners = []
        
        async with self.db_pool.acquire() as conn:
            # Get miners from file_assignments (miners with assigned files)
            file_miners = await conn.fetch("""
                SELECT DISTINCT 
                    COALESCE(miner1, miner2, miner3, miner4, miner5) as node_id
                FROM file_assignments
                WHERE COALESCE(miner1, miner2, miner3, miner4, miner5) IS NOT NULL
                UNION
                SELECT DISTINCT miner1 as node_id FROM file_assignments WHERE miner1 IS NOT NULL
                UNION
                SELECT DISTINCT miner2 as node_id FROM file_assignments WHERE miner2 IS NOT NULL
                UNION
                SELECT DISTINCT miner3 as node_id FROM file_assignments WHERE miner3 IS NOT NULL
                UNION
                SELECT DISTINCT miner4 as node_id FROM file_assignments WHERE miner4 IS NOT NULL
                UNION
                SELECT DISTINCT miner5 as node_id FROM file_assignments WHERE miner5 IS NOT NULL
            """)
            
            # Get miners from node_metrics (active miners)
            metrics_miners = await conn.fetch("""
                SELECT DISTINCT miner_id as node_id
                FROM node_metrics
                WHERE miner_id IS NOT NULL
            """)
            
            # Combine and deduplicate
            all_node_ids = set()
            for row in file_miners:
                if row['node_id']:
                    all_node_ids.add(row['node_id'])
            
            for row in metrics_miners:
                if row['node_id']:
                    all_node_ids.add(row['node_id'])
            
            # For each miner, try to get their IPFS peer ID
            for node_id in all_node_ids:
                # Try to get IPFS peer ID from registration table first
                reg_row = await conn.fetchrow("""
                    SELECT ipfs_peer_id FROM registration WHERE node_id = $1
                """, node_id)
                
                ipfs_peer_id = None
                if reg_row and reg_row['ipfs_peer_id']:
                    ipfs_peer_id = reg_row['ipfs_peer_id']
                else:
                    # Fallback: use node_id as ipfs_peer_id (common pattern)
                    ipfs_peer_id = node_id
                
                miners.append({
                    'node_id': node_id,
                    'ipfs_peer_id': ipfs_peer_id
                })
        
        logger.info(f"Found {len(miners)} miners for health checks")
        return miners
    
    async def get_miner_files(self, node_id: str) -> List[str]:
        """Get all files assigned to a specific miner."""
        async with self.db_pool.acquire() as conn:
            rows = await conn.fetch("""
                SELECT DISTINCT f.cid, f.created_date
                FROM files f
                JOIN file_assignments fa ON f.cid = fa.cid
                WHERE $1 IN (fa.miner1, fa.miner2, fa.miner3, fa.miner4, fa.miner5)
                ORDER BY f.created_date ASC
            """, node_id)
            
            return [row['cid'] for row in rows]
    
    async def send_to_queue(self, health_check_data: Dict[str, Any]) -> None:
        """Send health check data to RabbitMQ queue."""
        message_body = json.dumps(health_check_data)
        message = Message(
            body=message_body.encode(),
            delivery_mode=2  # Make message persistent
        )
        
        await self.rabbitmq_channel.default_exchange.publish(
            message,
            routing_key=self.queue_name
        )
        
        logger.debug(f"Sent health check task to queue: {health_check_data['node_id']}")
    
    async def process_miners(self) -> None:
        """Main processing function."""
        try:
            # Get current epoch
            current_epoch = self.get_current_epoch()
            
            # Fetch miners for health checks
            miners = await self.fetch_miners_for_health_check()
            
            if not miners:
                logger.warning("No miners found for health checks")
                return
            
            # Process each miner
            for miner in miners:
                node_id = miner['node_id']
                ipfs_peer_id = miner['ipfs_peer_id']
                
                # Get files assigned to this miner
                files = await self.get_miner_files(node_id)
                
                # Prepare health check data
                health_check_data = {
                    'node_id': node_id,
                    'ipfs_peer_id': ipfs_peer_id,
                    'epoch': current_epoch,
                    'files': files,
                    'timestamp': datetime.utcnow().isoformat(),
                    'block_number': self.substrate.get_block_number(None) if self.substrate else None
                }
                
                # Send to queue
                await self.send_to_queue(health_check_data)
            
            logger.info(f"Successfully queued {len(miners)} miners for health checks in epoch {current_epoch}")
            
        except Exception as e:
            logger.error(f"Error processing miners for health checks: {e}")
            raise


async def main():
    """Main function."""
    processor = MinerHealthProcessor()
    
    try:
        await processor.connect()
        await processor.process_miners()
        
    except Exception as e:
        logger.error(f"Error in main: {e}")
        raise
    finally:
        await processor.disconnect()


if __name__ == "__main__":
    asyncio.run(main()) 