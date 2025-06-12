#!/usr/bin/env python3
"""
Processor that fetches pinning request files from IPFS and queues individual files for processing.

This processor:
1. Reads pinning requests from the pinning_requests table
2. Fetches the file content from IPFS using the file_hash
3. Parses the JSON content to extract individual files
4. Queues each file for size processing
"""

import asyncio
import json
import logging
import os
import sys
from typing import Dict, List, Any, Optional

# Add parent directory to path for imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import aio_pika
import httpx
from aio_pika import Message

from app.db.connection import init_db_pool, close_db_pool, get_db_pool
from app.utils.config import IPFS_GATEWAY, get_ipfs_timeout

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class PinningFileProcessor:
    def __init__(self):
        self.rabbitmq_url = os.getenv('RABBITMQ_URL', 'amqp://admin:admin@localhost:5672/')
        self.queue_name = 'pinning_file_processing'
        self.rabbitmq_connection = None
        self.rabbitmq_channel = None
        self.http_client = None
        
    async def connect_rabbitmq(self):
        """Connect to RabbitMQ"""
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
    
    async def init_http_client(self):
        """Initialize HTTP client for IPFS requests"""
        timeout = get_ipfs_timeout("fetch")
        self.http_client = httpx.AsyncClient(timeout=timeout)
    
    async def fetch_pinning_requests(self) -> List[Dict[str, Any]]:
        """Fetch unprocessed pinning requests from the database"""
        pool = get_db_pool()
        async with pool.acquire() as conn:
            rows = await conn.fetch("""
                SELECT pr.request_hash, pr.owner, pr.file_hash, pr.file_name
                FROM pinning_requests pr
                WHERE pr.file_hash IS NOT NULL 
                AND pr.file_hash != ''
                AND NOT EXISTS (
                    SELECT 1 FROM pending_assignment_file paf 
                    WHERE paf.owner = pr.owner 
                    AND paf.cid = pr.file_hash
                )
                ORDER BY pr.created_at ASC
            """)
            
            return [dict(row) for row in rows]
    
    async def fetch_file_content(self, file_hash: str) -> Optional[List[Dict[str, Any]]]:
        """
        Fetch file content from IPFS and parse as JSON.
        
        Args:
            file_hash: The IPFS CID of the file to fetch
            
        Returns:
            List of file dictionaries or None if failed
        """
        try:
            gateway_url = IPFS_GATEWAY
            file_url = f"{gateway_url}/ipfs/{file_hash}"
            
            logger.info(f"Fetching file content from: {file_url}")
            
            response = await self.http_client.get(file_url)
            response.raise_for_status()
            
            # Parse JSON content
            content = response.json()
            
            # Validate that it's a list of file objects
            if not isinstance(content, list):
                logger.error(f"Expected list, got {type(content)} for file {file_hash}")
                return None
            
            # Validate each file object has required fields
            valid_files = []
            for file_obj in content:
                if isinstance(file_obj, dict) and 'cid' in file_obj:
                    valid_files.append(file_obj)
                else:
                    logger.warning(f"Invalid file object in {file_hash}: {file_obj}")
            
            logger.info(f"Successfully fetched {len(valid_files)} files from {file_hash}")
            return valid_files
            
        except httpx.HTTPStatusError as e:
            logger.error(f"HTTP error fetching file {file_hash}: {e.response.status_code}")
            return None
        except json.JSONDecodeError as e:
            logger.error(f"JSON decode error for file {file_hash}: {e}")
            return None
        except Exception as e:
            logger.error(f"Error fetching file {file_hash}: {e}")
            return None
    
    async def queue_file_for_processing(self, file_data: Dict[str, Any], owner: str) -> None:
        """Queue a file for size processing"""
        message_data = {
            'cid': file_data['cid'],
            'filename': file_data.get('filename', ''),
            'owner': owner
        }
        
        message_body = json.dumps(message_data).encode()
        
        await self.rabbitmq_channel.default_exchange.publish(
            Message(
                body=message_body,
                delivery_mode=aio_pika.DeliveryMode.PERSISTENT
            ),
            routing_key=self.queue_name
        )
        
        logger.debug(f"Queued file for processing: {file_data['cid']}")
    
    async def process_pinning_requests(self) -> None:
        """Process all unprocessed pinning requests"""
        requests = await self.fetch_pinning_requests()
        
        if not requests:
            logger.info("No unprocessed pinning requests found")
            return
        
        logger.info(f"Processing {len(requests)} pinning requests")
        
        total_files_queued = 0
        
        for request in requests:
            try:
                file_hash = request['file_hash']
                owner = request['owner']
                
                logger.info(f"Processing request for {owner} -> {file_hash[:16]}...")
                
                # Fetch and parse the file content
                files = await self.fetch_file_content(file_hash)
                
                if files is None:
                    logger.error(f"Failed to fetch file content for {file_hash}")
                    continue
                
                # Queue each file for processing
                for file_data in files:
                    await self.queue_file_for_processing(file_data, owner)
                    total_files_queued += 1
                
                logger.info(f"Queued {len(files)} files from request {file_hash[:16]}...")
                
            except Exception as e:
                logger.error(f"Error processing request {request.get('request_hash', 'unknown')}: {e}")
                continue
        
        logger.info(f"Successfully queued {total_files_queued} files for processing")
    
    async def close(self):
        """Close all connections"""
        if self.http_client:
            await self.http_client.aclose()
        
        if self.rabbitmq_connection:
            await self.rabbitmq_connection.close()
            logger.info("Closed RabbitMQ connection")


async def main():
    processor = PinningFileProcessor()
    
    try:
        # Initialize database pool
        await init_db_pool()
        logger.info("Database connection pool initialized")
        
        # Initialize HTTP client
        await processor.init_http_client()
        
        # Connect to RabbitMQ
        await processor.connect_rabbitmq()
        
        # Process pinning requests
        await processor.process_pinning_requests()
        
        logger.info("Completed processing pinning requests")
        
    except Exception as e:
        logger.error(f"Error in processor: {e}")
        raise
    finally:
        await processor.close()
        await close_db_pool()


if __name__ == "__main__":
    asyncio.run(main()) 