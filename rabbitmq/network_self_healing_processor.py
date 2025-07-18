#!/usr/bin/env python3
"""
Network Self-Healing Processor

This processor identifies files with broken assignments (failed miners, empty slots)
and queues them for self-healing via RabbitMQ.
"""

import asyncio
import json
import logging
import os
import sys
from datetime import datetime, timedelta
from typing import Dict, List, Any

# Add parent directory to path for imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import aio_pika
from aio_pika import Message
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


class NetworkSelfHealingProcessor:
    def __init__(self):
        self.rabbitmq_url = os.getenv('RABBITMQ_URL', 'amqp://admin:admin@localhost:5672/')
        self.queue_name = 'network_self_healing'
        self.db_pool = None
        
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
    
    async def find_broken_assignments(self) -> List[Dict[str, Any]]:
        """Find files with broken assignments that need healing."""
        async with self.db_pool.acquire() as conn:
            # Find files with empty assignments
            broken_files = await conn.fetch("""
                SELECT 
                    fa.cid,
                    fa.owner, 
                    f.name as filename,
                    f.size as file_size_bytes,
                    fa.miner1, fa.miner2, fa.miner3, fa.miner4, fa.miner5
                FROM file_assignments fa
                JOIN files f ON fa.cid = f.cid
                WHERE fa.miner1 IS NULL OR fa.miner2 IS NULL OR fa.miner3 IS NULL 
                   OR fa.miner4 IS NULL OR fa.miner5 IS NULL
                ORDER BY fa.updated_at ASC
                LIMIT 100
            """)
            
            return [dict(row) for row in broken_files]
    
    async def queue_healing_task(self, file_data: Dict[str, Any]):
        """Queue a self-healing task for a broken file."""
        task_data = {
            'type': 'file_healing',
            'cid': file_data['cid'],
            'owner': file_data['owner'],
            'filename': file_data['filename'],
            'file_size_bytes': file_data['file_size_bytes'],
            'current_miners': [
                file_data['miner1'], file_data['miner2'], file_data['miner3'],
                file_data['miner4'], file_data['miner5']
            ],
            'timestamp': datetime.now().isoformat()
        }
        
        message_body = json.dumps(task_data).encode()
        
        await self.rabbitmq_channel.default_exchange.publish(
            Message(
                body=message_body,
                delivery_mode=aio_pika.DeliveryMode.PERSISTENT
            ),
            routing_key=self.queue_name
        )
        
        logger.debug(f"Queued healing task for file {file_data['cid'][:16]}...")
    
    async def process_self_healing(self):
        """Main processing method."""
        logger.info("🛠️ Starting network self-healing processing")
        
        try:
            # Initialize database connection
            await init_db_pool()
            self.db_pool = get_db_pool()
            
            # Connect to RabbitMQ
            await self.connect_rabbitmq()
            
            # Find broken assignments
            broken_files = await self.find_broken_assignments()
            
            if not broken_files:
                logger.info("✅ No broken assignments found - network is healthy")
                return True
            
            logger.info(f"🔍 Found {len(broken_files)} files with broken assignments")
            
            # Queue healing tasks
            healed_count = 0
            for file_data in broken_files:
                try:
                    await self.queue_healing_task(file_data)
                    healed_count += 1
                except Exception as e:
                    logger.error(f"❌ Failed to queue healing task for {file_data['cid']}: {e}")
            
            logger.info(f"✅ Queued {healed_count} self-healing tasks")
            
            # Close RabbitMQ connection
            if hasattr(self, 'rabbitmq_connection'):
                await self.rabbitmq_connection.close()
            
            return True
            
        except Exception as e:
            logger.error(f"❌ Error during self-healing processing: {e}")
            return False
        finally:
            if self.db_pool:
                await close_db_pool()


async def main():
    """Main entry point."""
    processor = NetworkSelfHealingProcessor()
    success = await processor.process_self_healing()
    
    if success:
        logger.info("✅ Network self-healing processing completed successfully")
        return 0
    else:
        logger.error("❌ Network self-healing processing failed")
        return 1


if __name__ == "__main__":
    exit_code = asyncio.run(main())
    sys.exit(exit_code) 