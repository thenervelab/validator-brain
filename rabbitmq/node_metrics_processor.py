"""
RabbitMQ processor for fetching node metrics from the latest block.

This processor:
1. Connects to the substrate chain
2. Fetches ExecutionUnit.nodeMetrics storage from the latest block only
3. Parses the node metrics data
4. Sends individual metrics to RabbitMQ queue
5. Shuts down after processing
"""

import asyncio
import json
import logging
import os
import sys
from typing import Dict, Any, Optional

# Add parent directory to path to import app modules
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import aio_pika
from dotenv import load_dotenv
from substrateinterface import SubstrateInterface

from app.utils.config import NODE_URL

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class NodeMetricsProcessor:
    """Processor for fetching and queuing node metrics."""
    
    def __init__(self):
        """Initialize the processor."""
        self.substrate = None
        self.rabbitmq_connection = None
        self.rabbitmq_channel = None
        self.queue_name = 'node_metrics_latest'
        
    def connect_substrate(self):
        """Connect to the substrate chain."""
        logger.info(f"Connecting to substrate at {NODE_URL}")
        self.substrate = SubstrateInterface(url=NODE_URL)
        logger.info("Connected to substrate")
        
    async def connect_rabbitmq(self):
        """Connect to RabbitMQ and declare the queue."""
        rabbitmq_url = os.getenv('RABBITMQ_URL', 'amqp://admin:admin@localhost:5672/')
        
        self.rabbitmq_connection = await aio_pika.connect_robust(rabbitmq_url)
        self.rabbitmq_channel = await self.rabbitmq_connection.channel()
        
        # Declare the queue
        await self.rabbitmq_channel.declare_queue(
            self.queue_name,
            durable=True
        )
        
        logger.info(f"Connected to RabbitMQ and declared queue '{self.queue_name}'")
    
    def parse_node_metrics(self, miner_id: str, metrics_data: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """
        Parse node metrics data from substrate.
        
        Args:
            miner_id: The miner's peer ID
            metrics_data: Raw metrics data from substrate
            
        Returns:
            Parsed metrics dictionary or None if data is invalid
        """
        try:
            # AGGRESSIVE DEBUGGING: Log the raw metrics data to see its structure
            logger.info(f"DEBUG: Raw metrics data for {miner_id}: {json.dumps(metrics_data, indent=2)}")

            # Handle both camelCase and snake_case field names
            ipfs_repo_size = metrics_data.get('ipfs_repo_size', metrics_data.get('ipfsRepoSize'))
            
            # Try various keys for storage max
            ipfs_storage_max = (
                metrics_data.get('ipfs_storage_max') or
                metrics_data.get('ipfsStorageMax') or
                metrics_data.get('storage_capacity') or
                metrics_data.get('storageCapacity') or
                metrics_data.get('max_storage') or
                metrics_data.get('maxStorage')
            )
            
            # Default to 0 if any required field is missing
            if ipfs_repo_size is None:
                logger.warning(f"Missing ipfs_repo_size for miner {miner_id}, defaulting to 0")
                ipfs_repo_size = 0
            
            if ipfs_storage_max is None:
                logger.warning(f"Missing ipfs_storage_max for miner {miner_id}, defaulting to 0")
                ipfs_storage_max = 0
            
            return {
                'miner_id': miner_id,
                'ipfs_repo_size': int(ipfs_repo_size),
                'ipfs_storage_max': int(ipfs_storage_max),
                'timestamp': asyncio.get_event_loop().time()
            }
            
        except Exception as e:
            logger.error(f"Error parsing metrics for miner {miner_id}: {e}")
            return None
    
    async def fetch_and_queue_metrics(self):
        """
        Fetch node metrics from substrate at the latest block and queue them.
        """
        try:
            # Always fetch from the latest block
            block_hash = None
            block_number = self.substrate.get_block_number(block_hash)
            logger.info(f"Fetching metrics at latest block {block_number}")
            
            # Query all node metrics
            result = self.substrate.query_map(
                module='ExecutionUnit',
                storage_function='NodeMetrics',
                block_hash=block_hash
            )
            
            metrics_count = 0
            
            for key, value in result:
                if value is None or (hasattr(value, 'value') and value.value is None):
                    continue
                
                # The key is the peer ID - it's either a string or a byte array
                if hasattr(key, '__str__') and str(key).startswith('12D3Koo'):
                    # Key is already a string representation of the peer ID
                    miner_id = str(key)
                elif hasattr(key, '__iter__'):
                    # Key is a byte array - convert ASCII values to string
                    try:
                        miner_id = ''.join(chr(b) for b in key)
                    except:
                        # Fallback to string representation
                        miner_id = str(key)
                else:
                    miner_id = str(key)
                
                # Get the actual metrics data
                if hasattr(value, 'value'):
                    metrics_data = value.value
                else:
                    metrics_data = value
                
                # The metrics data may also contain minerId field
                # Use it if the key extraction didn't work or is empty
                if isinstance(metrics_data, dict):
                    data_miner_id = metrics_data.get('miner_id', metrics_data.get('minerId'))
                    if data_miner_id and data_miner_id.strip():
                        # Use the miner_id from data if it's not empty
                        miner_id = str(data_miner_id)
                
                # Parse the metrics
                parsed_metrics = self.parse_node_metrics(miner_id, metrics_data)
                
                if parsed_metrics:
                    # Add block number to the message
                    parsed_metrics['block_number'] = block_number
                    
                    # Send to queue
                    message_body = json.dumps(parsed_metrics).encode()
                    
                    await self.rabbitmq_channel.default_exchange.publish(
                        aio_pika.Message(
                            body=message_body,
                            delivery_mode=aio_pika.DeliveryMode.PERSISTENT
                        ),
                        routing_key=self.queue_name
                    )
                    
                    metrics_count += 1
                    logger.debug(f"Sent metrics for miner {miner_id}")
            
            logger.info(f"Successfully processed {metrics_count} node metrics at block {block_number}")
            
        except Exception as e:
            logger.error(f"Error fetching/queuing node metrics: {e}")
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
    processor = NodeMetricsProcessor()
    
    try:
        # Connect to services
        processor.connect_substrate()
        await processor.connect_rabbitmq()
        
        # Fetch and queue metrics from latest block only
        await processor.fetch_and_queue_metrics()
        
        logger.info("Completed processing latest block metrics")
        
    except Exception as e:
        logger.error(f"Error in processor: {e}")
        raise
    finally:
        await processor.close()


if __name__ == "__main__":
    asyncio.run(main()) 