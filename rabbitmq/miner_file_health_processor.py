"""
Processor for queuing individual file health checks for miners.

This processor:
1. Fetches miners and their assigned files from the database
2. Gets the current epoch from the blockchain
3. Queues individual file health check messages (one per file per miner)
4. Sends messages to the miner_file_health_check queue
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

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class MinerFileHealthProcessor:
    """Processor for queuing individual file health checks."""
    
    def __init__(self):
        self.rabbitmq_url = os.getenv('RABBITMQ_URL', 'amqp://admin:admin@localhost:5672/')
        self.queue_name = 'miner_file_health_check'
        self.node_url = os.getenv('NODE_URL', 'wss://rpc.hippius.network')
        
        self.rabbitmq_connection = None
        self.rabbitmq_channel = None
        self.db_pool = None
        self.substrate = None
        
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
    
    async def connect(self):
        """Connect to all services."""
        try:
            # Initialize database pool
            await init_db_pool()
            self.db_pool = get_db_pool()
            logger.info("Database connection pool initialized")
            
            # Connect to substrate
            self.substrate = SubstrateInterface(url=self.node_url)
            logger.info(f"Connected to substrate node: {self.node_url}")
            
            # Connect to RabbitMQ
            self.rabbitmq_connection = await aio_pika.connect_robust(self.rabbitmq_url)
            self.rabbitmq_channel = await self.rabbitmq_connection.channel()
            logger.info("Connected to RabbitMQ")
            
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
    
    async def get_miners_with_files(self) -> List[Dict[str, Any]]:
        """Get all miners with their assigned files."""
        async with self.db_pool.acquire() as conn:
            # Get miners from node_metrics (most recent data)
            miners = await conn.fetch("""
                SELECT DISTINCT nm.miner_id as node_id, nm.miner_id as ipfs_peer_id
                FROM node_metrics nm
                WHERE nm.miner_id IS NOT NULL
                ORDER BY nm.miner_id
            """)
            
            result = []
            for miner in miners:
                node_id = miner['node_id']
                ipfs_peer_id = miner['ipfs_peer_id']
                
                # Get all files assigned to this miner
                files = await conn.fetch("""
                    SELECT DISTINCT f.cid, f.created_date
                    FROM files f
                    JOIN file_assignments fa ON f.cid = fa.cid
                    WHERE $1 IN (fa.miner1, fa.miner2, fa.miner3, fa.miner4, fa.miner5)
                    ORDER BY f.created_date ASC
                """, node_id)
                
                file_cids = [row['cid'] for row in files]
                
                if file_cids:  # Only include miners that have files
                    result.append({
                        'node_id': node_id,
                        'ipfs_peer_id': ipfs_peer_id,
                        'files': file_cids
                    })
            
            return result
    
    async def queue_file_health_checks(self):
        """Queue individual file health check messages."""
        try:
            # Get current epoch
            current_epoch = self.get_current_epoch()
            
            # Get miners with their files
            miners_with_files = await self.get_miners_with_files()
            logger.info(f"Found {len(miners_with_files)} miners with assigned files")
            
            # Declare the queue
            queue = await self.rabbitmq_channel.declare_queue(
                self.queue_name,
                durable=True
            )
            
            total_messages = 0
            
            for miner in miners_with_files:
                node_id = miner['node_id']
                ipfs_peer_id = miner['ipfs_peer_id']
                files = miner['files']
                
                logger.info(f"Queuing {len(files)} file health checks for miner {node_id}")
                
                # Queue individual messages for each file
                for file_cid in files:
                    # Prepare file health check data
                    file_health_check_data = {
                        'node_id': node_id,
                        'ipfs_peer_id': ipfs_peer_id,
                        'file_cid': file_cid,
                        'epoch': current_epoch,
                        'timestamp': datetime.utcnow().isoformat(),
                        'block_number': self.substrate.get_block_number(None) if self.substrate else None
                    }
                    
                    # Send message to queue
                    message = Message(
                        json.dumps(file_health_check_data).encode(),
                        delivery_mode=2  # Make message persistent
                    )
                    
                    await self.rabbitmq_channel.default_exchange.publish(
                        message,
                        routing_key=self.queue_name
                    )
                    
                    total_messages += 1
                
                logger.info(f"Queued {len(files)} file health checks for miner {node_id}")
            
            logger.info(f"Successfully queued {total_messages} file health check messages for epoch {current_epoch}")
            
        except Exception as e:
            logger.error(f"Error queuing file health checks: {e}")
            raise
    
    async def run(self):
        """Main run method."""
        try:
            await self.connect()
            await self.queue_file_health_checks()
        finally:
            await self.disconnect()


async def main():
    """Main function."""
    processor = MinerFileHealthProcessor()
    
    try:
        await processor.run()
        logger.info("File health check processor completed successfully")
    except Exception as e:
        logger.error(f"Error in main: {e}")
        raise


if __name__ == "__main__":
    asyncio.run(main()) 