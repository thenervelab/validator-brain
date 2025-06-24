#!/usr/bin/env python3
"""
Consumer that reconstructs miner profiles as JSON and publishes them to a remote IPFS node.
This consumer processes messages from the reconstruction queue, builds the complete profile JSON,
and pins it to the remote IPFS node at store.hippius.network.
"""

import asyncio
import json
import logging
import os
import sys
from datetime import datetime
from typing import Dict, Any, Optional

import aio_pika
import httpx
from aio_pika import IncomingMessage

# Add parent directory to path for imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app.db.connection import init_db_pool, close_db_pool
from app.db.models.pending_miner_profile import PendingMinerProfile

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class MinerProfileReconstructionConsumer:
    def __init__(self):
        self.rabbitmq_url = os.getenv('RABBITMQ_URL', 'amqp://admin:admin@localhost:5672/')
        self.queue_name = 'miner_profile_reconstruction'
        self.remote_ipfs_url = os.getenv('REMOTE_IPFS_URL', 'https://store.hippius.network')
        self.rabbitmq_connection = None
        self.rabbitmq_channel = None
        self.http_client = None
    
    async def init_http_client(self):
        """Initialize HTTP client for IPFS requests"""
        self.http_client = httpx.AsyncClient(timeout=30.0)
    
    async def get_file_owner(self, cid: str) -> Optional[str]:
        """Get the owner of a file from file_assignments table"""
        try:
            from app.db.connection import get_db_pool
            pool = get_db_pool()
            async with pool.acquire() as conn:
                row = await conn.fetchrow(
                    "SELECT owner FROM file_assignments WHERE cid = $1",
                    cid
                )
                if row and row['owner']:
                    return row['owner']
                else:
                    logger.warning(f"No owner found for CID {cid} in file_assignments table")
                    return None
        except Exception as e:
            logger.error(f"Could not fetch owner for CID {cid}: {e}")
            return None
    
    async def connect_rabbitmq(self):
        """Connect to RabbitMQ"""
        try:
            self.rabbitmq_connection = await aio_pika.connect_robust(self.rabbitmq_url)
            self.rabbitmq_channel = await self.rabbitmq_connection.channel()
            
            # Set prefetch count to process one message at a time
            await self.rabbitmq_channel.set_qos(prefetch_count=1)
            
            logger.info("Connected to RabbitMQ")
        except Exception as e:
            logger.error(f"Failed to connect to RabbitMQ: {e}")
            raise
    
    async def reconstruct_profile_json(self, message_data: Dict[str, Any]) -> list:
        """Reconstruct the miner profile as JSON in the original substrate format"""
        node_id = message_data['node_id']
        block_number = message_data.get('block_number', 0)
        
        # Get validator address from environment - REQUIRED
        selected_validator = os.getenv('VALIDATOR_ACCOUNT_ID')
        if not selected_validator:
            raise ValueError("VALIDATOR_ACCOUNT_ID environment variable is required but not set")
        
        # Build the profile as an array of file objects
        profile_files = []
        
        # Add files to the profile in the original format
        for file_data in message_data.get('files', []):
            # Convert CID back to hex-encoded byte array
            cid = file_data['cid']
            file_hash = list(cid.encode('utf-8'))
            
            # Get the actual owner from file_assignments table
            owner = await self.get_file_owner(cid)
            
            # Skip files without owners to prevent incorrect charging
            if not owner:
                logger.warning(f"Skipping file {cid} - no owner found, cannot include in miner profile")
                continue
            
            file_entry = {
                "created_at": block_number,
                "file_hash": file_hash,
                "file_size_in_bytes": file_data['size'],
                "miner_node_id": node_id,
                "owner": owner,
                "selected_validator": selected_validator
            }
            
            profile_files.append(file_entry)
        
        return profile_files
    
    async def publish_to_ipfs(self, profile_json: list) -> Optional[str]:
        """Publish the profile JSON to the remote IPFS node"""
        try:
            # Convert profile to JSON string
            json_data = json.dumps(profile_json, indent=2)
            
            # Prepare the request
            files = {
                'file': ('profile.json', json_data, 'application/json')
            }
            
            # Send to IPFS API
            response = await self.http_client.post(
                f"{self.remote_ipfs_url}/api/v0/add",
                files=files,
                params={'pin': 'true'}
            )
            
            if response.status_code == 200:
                result = response.json()
                cid = result.get('Hash')
                logger.info(f"Successfully published profile to IPFS: {cid}")
                return cid
            else:
                logger.error(f"Failed to publish to IPFS: {response.status_code} - {response.text}")
                return None
                
        except Exception as e:
            logger.error(f"Error publishing to IPFS: {e}")
            return None
    
    async def process_message(self, message: IncomingMessage) -> None:
        """Process a single message from the queue"""
        async with message.process():
            try:
                # Parse message
                message_data = json.loads(message.body.decode())
                node_id = message_data['node_id']
                profile_cid = message_data['cid']
                
                logger.info(f"Processing miner profile: {node_id} -> {profile_cid}")
                
                # Check if already processed by node_id
                existing = await PendingMinerProfile.get_by_node_id(node_id)
                if existing and existing.status == 'published':
                    logger.info(f"Profile for miner {node_id} already published (CID: {existing.cid}), skipping")
                    return
                
                # Reconstruct the profile JSON
                profile_json = await self.reconstruct_profile_json(message_data)
                
                # Publish to IPFS
                published_cid = await self.publish_to_ipfs(profile_json)
                
                if published_cid:
                    # Update or create the pending profile record with the actual IPFS CID
                    if existing:
                        # Update the existing record with the actual IPFS CID
                        from app.db.connection import get_db_pool
                        pool = get_db_pool()
                        async with pool.acquire() as conn:
                            await conn.execute(
                                "UPDATE pending_miner_profile SET cid = $1 WHERE id = $2",
                                published_cid, existing.id
                            )
                        existing.cid = published_cid
                        await existing.mark_published()
                    else:
                        files_count = message_data.get('file_count', 0)
                        total_size = message_data.get('total_size', 0)
                        block_number = message_data.get('block_number', 0)
                        profile_record = await PendingMinerProfile.create(
                            cid=published_cid,  # Use the actual IPFS CID
                            node_id=node_id,
                            files_count=files_count,
                            files_size=total_size,
                            block_number=block_number
                        )
                        await profile_record.mark_published()
                    
                    logger.info(f"Successfully processed profile {profile_cid} -> {published_cid}")
                else:
                    # Mark as failed
                    error_msg = "Failed to publish to IPFS"
                    if existing:
                        await existing.mark_failed(error_msg)
                    else:
                        files_count = message_data.get('file_count', 0)
                        total_size = message_data.get('total_size', 0)
                        block_number = message_data.get('block_number', 0)
                        profile_record = await PendingMinerProfile.create(
                            cid=profile_cid,
                            node_id=node_id,
                            files_count=files_count,
                            files_size=total_size,
                            block_number=block_number
                        )
                        await profile_record.mark_failed(error_msg)
                    
                    logger.error(f"Failed to process profile {profile_cid}")
                
            except Exception as e:
                logger.error(f"Error processing message: {e}")
                # Don't acknowledge the message, let it be requeued
                raise
    
    async def start_consuming(self):
        """Start consuming messages from the queue"""
        queue = await self.rabbitmq_channel.declare_queue(
            self.queue_name,
            durable=True
        )
        
        logger.info(f"Starting to consume from queue '{self.queue_name}'")
        
        # Start consuming
        await queue.consume(self.process_message)
        
        # Keep the consumer running
        await asyncio.Future()
    
    async def close(self):
        """Close connections"""
        if self.http_client:
            await self.http_client.aclose()
        
        if self.rabbitmq_connection:
            await self.rabbitmq_connection.close()
            logger.info("Closed RabbitMQ connection")


async def main():
    consumer = MinerProfileReconstructionConsumer()
    
    try:
        # Initialize database pool
        await init_db_pool()
        logger.info("Database connection pool initialized")
        
        # Initialize HTTP client
        await consumer.init_http_client()
        
        # Connect to RabbitMQ
        await consumer.connect_rabbitmq()
        
        # Start consuming
        await consumer.start_consuming()
        
    except KeyboardInterrupt:
        logger.info("Received interrupt signal, shutting down...")
    except Exception as e:
        logger.error(f"Error in consumer: {e}")
        raise
    finally:
        await consumer.close()
        await close_db_pool()


if __name__ == "__main__":
    asyncio.run(main()) 