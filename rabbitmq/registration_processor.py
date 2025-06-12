"""
RabbitMQ processor for fetching node registration data from the latest block.

This processor:
1. Connects to the substrate chain
2. Clears the registration table before processing
3. Fetches registration.coldkeyNodeRegistration and registration.nodeRegistration storage from the latest block
4. Parses the registration data
5. Sends individual registration records to RabbitMQ queue
6. Shuts down after processing
"""

import asyncio
import json
import logging
import os
import sys
from typing import Dict, Any, Optional, List

# Add parent directory to path to import app modules
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import aio_pika
from dotenv import load_dotenv
from substrateinterface import SubstrateInterface

from app.utils.config import NODE_URL
from app.db.connection import get_db_pool, init_db_pool, close_db_pool

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class RegistrationProcessor:
    """Processor for fetching and queuing node registration data."""
    
    def __init__(self):
        """Initialize the processor."""
        self.substrate = None
        self.rabbitmq_connection = None
        self.rabbitmq_channel = None
        self.queue_name = 'registration_latest'
        self.db_pool = None
        
    def connect_substrate(self):
        """Connect to the substrate chain."""
        logger.info(f"Connecting to substrate at {NODE_URL}")
        self.substrate = SubstrateInterface(url=NODE_URL)
        logger.info("Connected to substrate")
        
    async def init_database(self):
        """Initialize database connection pool."""
        await init_db_pool()
        self.db_pool = get_db_pool()
        logger.info("Database connection pool initialized")
        
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
    
    async def clear_registration_table(self):
        """Clear all records from the registration table before processing."""
        try:
            async with self.db_pool.acquire() as conn:
                deleted_count = await conn.execute("DELETE FROM registration")
                logger.info(f"Cleared {deleted_count} records from registration table")
        except Exception as e:
            logger.error(f"Error clearing registration table: {e}")
            raise
    
    def parse_registration_data(self, node_id: str, registration_data: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """
        Parse registration data from substrate.
        
        Args:
            node_id: The node's peer ID (key from the storage map)
            registration_data: Raw registration data from substrate
            
        Returns:
            Parsed registration dictionary or None if data is invalid
        """
        try:
            # Handle both camelCase and snake_case field names
            ipfs_node_id = registration_data.get('ipfsNodeId', registration_data.get('ipfs_node_id'))
            node_type = registration_data.get('nodeType', registration_data.get('node_type'))
            owner = registration_data.get('owner', registration_data.get('owner_account'))
            registered_at = registration_data.get('registeredAt', registration_data.get('registered_at'))
            status = registration_data.get('status', 'Online')  # Default to Online if not specified
            
            # Use the nodeId from the data if available, otherwise use the key
            actual_node_id = registration_data.get('nodeId', registration_data.get('node_id', node_id))
            
            if not all([actual_node_id, ipfs_node_id, node_type, owner, registered_at]):
                logger.warning(f"Missing required fields for node {node_id}: {registration_data}")
                return None
            
            return {
                'node_id': str(actual_node_id),
                'ipfs_peer_id': str(ipfs_node_id),
                'node_type': str(node_type),
                'owner_account': str(owner),
                'registered_at': int(registered_at),
                'status': 'active' if str(status).lower() == 'online' else 'inactive'
            }
            
        except Exception as e:
            logger.error(f"Error parsing registration data for node {node_id}: {e}")
            return None
    
    async def fetch_and_queue_registrations(self):
        """
        Fetch registration data from substrate at the latest block and queue them.
        """
        try:
            # Always fetch from the latest block
            block_hash = None
            block_number = self.substrate.get_block_number(block_hash)
            logger.info(f"Fetching registrations at latest block {block_number}")
            
            # Clear the registration table first
            # await self.clear_registration_table() # <<< MODIFIED: Commented out, consumer will handle upsert
            
            registration_count = 0
            
            # Fetch coldkeyNodeRegistration
            logger.info("Fetching registration.coldkeyNodeRegistration...")
            coldkey_result = self.substrate.query_map(
                module='Registration',
                storage_function='ColdkeyNodeRegistration',
                block_hash=block_hash
            )
            
            for key, value in coldkey_result:
                if value is None or (hasattr(value, 'value') and value.value is None):
                    continue
                
                # Extract node ID from key
                node_id = self._extract_node_id(key)
                if not node_id:
                    continue
                
                # Get the actual registration data
                registration_data = value.value if hasattr(value, 'value') else value
                
                # Parse the registration
                parsed_registration = self.parse_registration_data(node_id, registration_data)
                
                if parsed_registration:
                    # Add block number to the message
                    parsed_registration['block_number'] = block_number
                    parsed_registration['source'] = 'coldkey'
                    
                    # Send to queue
                    message_body = json.dumps(parsed_registration).encode()
                    
                    await self.rabbitmq_channel.default_exchange.publish(
                        aio_pika.Message(
                            body=message_body,
                            delivery_mode=aio_pika.DeliveryMode.PERSISTENT
                        ),
                        routing_key=self.queue_name
                    )
                    
                    registration_count += 1
                    logger.debug(f"Sent coldkey registration for node {node_id}")
            
            # Fetch nodeRegistration
            logger.info("Fetching registration.nodeRegistration...")
            node_result = self.substrate.query_map(
                module='Registration',
                storage_function='NodeRegistration',
                block_hash=block_hash
            )
            
            for key, value in node_result:
                if value is None or (hasattr(value, 'value') and value.value is None):
                    continue
                
                # Extract node ID from key
                node_id = self._extract_node_id(key)
                if not node_id:
                    continue
                
                # Get the actual registration data
                registration_data = value.value if hasattr(value, 'value') else value
                
                # Parse the registration
                parsed_registration = self.parse_registration_data(node_id, registration_data)
                
                if parsed_registration:
                    # Add block number to the message
                    parsed_registration['block_number'] = block_number
                    parsed_registration['source'] = 'node'
                    
                    # Send to queue
                    message_body = json.dumps(parsed_registration).encode()
                    
                    await self.rabbitmq_channel.default_exchange.publish(
                        aio_pika.Message(
                            body=message_body,
                            delivery_mode=aio_pika.DeliveryMode.PERSISTENT
                        ),
                        routing_key=self.queue_name
                    )
                    
                    registration_count += 1
                    logger.debug(f"Sent node registration for node {node_id}")
            
            logger.info(f"Successfully processed {registration_count} registrations at block {block_number}")
            
        except Exception as e:
            logger.error(f"Error fetching/queuing registrations: {e}")
            raise
    
    def _extract_node_id(self, key) -> Optional[str]:
        """
        Extract node ID from storage key.
        
        Args:
            key: Storage key from substrate
            
        Returns:
            Node ID string or None if extraction fails
        """
        try:
            # The key is the peer ID - it's either a string or a byte array
            if hasattr(key, '__str__') and str(key).startswith('12D3Koo'):
                # Key is already a string representation of the peer ID
                return str(key)
            elif hasattr(key, '__iter__'):
                # Key is a byte array - convert ASCII values to string
                try:
                    return ''.join(chr(b) for b in key)
                except:
                    # Fallback to string representation
                    return str(key)
            else:
                return str(key)
        except Exception as e:
            logger.error(f"Error extracting node ID from key {key}: {e}")
            return None
    
    async def close(self):
        """Close all connections."""
        if self.rabbitmq_connection:
            await self.rabbitmq_connection.close()
            logger.info("Closed RabbitMQ connection")
        
        if self.substrate:
            self.substrate.close()
            logger.info("Closed substrate connection")
        
        if self.db_pool:
            await close_db_pool()
            logger.info("Closed database connection pool")


async def main():
    """Main entry point."""
    processor = RegistrationProcessor()
    
    try:
        # Connect to services
        await processor.init_database()
        processor.connect_substrate()
        await processor.connect_rabbitmq()
        
        # Fetch and queue registrations from latest block only
        await processor.fetch_and_queue_registrations()
        
        logger.info("Completed processing latest block registrations")
        
    except Exception as e:
        logger.error(f"Error in processor: {e}")
        raise
    finally:
        await processor.close()


if __name__ == "__main__":
    asyncio.run(main()) 