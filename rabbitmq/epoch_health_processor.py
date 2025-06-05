"""
Epoch Health Processor for comprehensive miner health checks.

This processor:
1. Runs at the start of each validator epoch
2. Clears/resets the miner_epoch_health table for the new epoch
3. Fetches ALL miners from registration table
4. Gets ALL files assigned to each miner
5. Queues comprehensive health check messages for each miner
6. Sends messages to the epoch_health_check queue
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


class EpochHealthProcessor:
    """Processor for comprehensive epoch-based health checks."""
    
    def __init__(self):
        self.rabbitmq_url = os.getenv('RABBITMQ_URL', 'amqp://admin:admin@localhost:5672/')
        self.queue_name = 'epoch_health_check'
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
    
    async def clear_epoch_health_data(self, current_epoch: int) -> None:
        """Preserve health data from the PREVIOUS epoch and mark it as stale."""
        if current_epoch == 0:
            logger.info("Epoch 0, no previous epoch data to mark as stale.")
            return

        previous_epoch = current_epoch - 1
        async with self.db_pool.acquire() as conn:
            try:
                # CONSERVATIVE APPROACH: Preserve health data, just mark as stale
                # Instead of DELETE, UPDATE existing records to mark them as archived/stale
                result = await conn.execute("""
                    UPDATE miner_epoch_health 
                    SET 
                        is_stale = true,
                        archived_at = NOW()
                    WHERE epoch = $1 AND (is_stale IS NOT true OR is_stale IS NULL)
                """, previous_epoch)
                
                # Add is_stale column if it doesn't exist (migration-safe)
                try:
                    await conn.execute("""
                        ALTER TABLE miner_epoch_health 
                        ADD COLUMN IF NOT EXISTS is_stale BOOLEAN DEFAULT false,
                        ADD COLUMN IF NOT EXISTS archived_at TIMESTAMP DEFAULT NULL
                    """)
                except Exception as alter_e:
                    logger.debug(f"Columns may already exist: {alter_e}")
                
                updated_count = 0
                if result and 'UPDATE' in result:
                    try:
                        updated_count = int(result.split(' ')[-1])
                    except ValueError:
                        logger.warning(f"Could not parse update count from: {result}")

                if updated_count > 0:
                    logger.info(f"✅ PRESERVED health data for PREVIOUS epoch {previous_epoch} ({updated_count} records marked as stale)")
                else:
                    logger.info(f"ℹ️ No health data from PREVIOUS epoch {previous_epoch} found to mark as stale, or already marked.")
                logger.info(f"   This maintains historical health data for trend analysis and debugging")
                
            except Exception as e:
                logger.error(f"Error preserving epoch health data for epoch {previous_epoch}: {e}")
                # Do not raise here, allow the processor to continue for the current epoch
                # raise
    
    async def fetch_all_miners(self) -> List[Dict[str, Any]]:
        """
        Fetch ALL active miners from registration table.
        """
        miners = []
        
        async with self.db_pool.acquire() as conn:
            # Get all active storage miners from registration
            rows = await conn.fetch("""
                SELECT 
                    node_id,
                    ipfs_peer_id,
                    node_type,
                    status,
                    registered_at
                FROM registration
                WHERE node_type = 'StorageMiner' 
                  AND status = 'active'
                ORDER BY registered_at ASC
            """)
            
            for row in rows:
                miners.append({
                    'node_id': row['node_id'],
                    'ipfs_peer_id': row['ipfs_peer_id'],
                    'node_type': row['node_type'],
                    'status': row['status']
                })
        
        logger.info(f"Found {len(miners)} active storage miners for epoch health checks")
        return miners
    
    async def get_all_miner_files(self, node_id: str) -> List[str]:
        """Get ALL files assigned to a specific miner."""
        async with self.db_pool.acquire() as conn:
            # Get all files from file_assignments where this miner is assigned
            rows = await conn.fetch("""
                SELECT DISTINCT fa.cid
                FROM file_assignments fa
                WHERE $1 IN (fa.miner1, fa.miner2, fa.miner3, fa.miner4, fa.miner5)
                ORDER BY fa.cid
            """, node_id)
            
            files = [row['cid'] for row in rows]
            
            # Also check miner_profile for additional files
            profile_rows = await conn.fetch("""
                SELECT DISTINCT file_hash as cid
                FROM miner_profile
                WHERE miner_node_id = $1
                ORDER BY file_hash
            """, node_id)
            
            # Combine and deduplicate
            profile_files = [row['cid'] for row in profile_rows]
            all_files = list(set(files + profile_files))
            
            return all_files
    
    async def send_to_queue(self, health_check_data: Dict[str, Any]) -> None:
        """Send epoch health check data to RabbitMQ queue."""
        message_body = json.dumps(health_check_data)
        message = Message(
            body=message_body.encode(),
            delivery_mode=2  # Make message persistent
        )
        
        await self.rabbitmq_channel.default_exchange.publish(
            message,
            routing_key=self.queue_name
        )
        
        logger.debug(f"Sent epoch health check task to queue: {health_check_data['node_id']}")
    
    async def process_epoch_health_checks(self) -> None:
        """Main processing function for epoch health checks."""
        try:
            # Get current epoch
            current_epoch = self.get_current_epoch()
            current_block = self.substrate.get_block_number(None) if self.substrate else None
            
            logger.info(f"Starting comprehensive epoch health checks for epoch {current_epoch}")
            
            # Clear existing health data for this epoch
            await self.clear_epoch_health_data(current_epoch)
            
            # Fetch ALL active miners
            miners = await self.fetch_all_miners()
            
            if not miners:
                logger.warning("No active miners found for epoch health checks")
                return
            
            total_files = 0
            queued_miners = 0
            
            # Process each miner
            for miner in miners:
                node_id = miner['node_id']
                ipfs_peer_id = miner['ipfs_peer_id']
                
                # Get ALL files assigned to this miner
                files = await self.get_all_miner_files(node_id)
                total_files += len(files)
                
                logger.info(f"Miner {node_id} has {len(files)} files assigned")
                
                # Prepare comprehensive health check data
                health_check_data = {
                    'node_id': node_id,
                    'ipfs_peer_id': ipfs_peer_id,
                    'epoch': current_epoch,
                    'files': files,
                    'total_files': len(files),
                    'timestamp': datetime.utcnow().isoformat(),
                    'block_number': current_block,
                    'check_type': 'epoch_comprehensive'
                }
                
                # Send to queue
                await self.send_to_queue(health_check_data)
                queued_miners += 1
            
            logger.info(f"Successfully queued {queued_miners} miners with {total_files} total files for comprehensive epoch health checks in epoch {current_epoch}")
            
        except Exception as e:
            logger.error(f"Error processing epoch health checks: {e}")
            raise


async def main():
    """Main function."""
    processor = EpochHealthProcessor()
    
    try:
        await processor.connect()
        await processor.process_epoch_health_checks()
        
    except Exception as e:
        logger.error(f"Error in main: {e}")
        raise
    finally:
        await processor.disconnect()


if __name__ == "__main__":
    asyncio.run(main()) 