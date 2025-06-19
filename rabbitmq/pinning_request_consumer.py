"""
Consumer for processing pinning requests from RabbitMQ queue.

This consumer:
1. Reads messages from the pinning_request queue
2. Checks if the request has already been processed
3. Decodes the file hash from hex to CID
4. Updates the pinning_requests table
"""

import asyncio
import json
import logging
import os
import sys
from datetime import datetime
from typing import Dict, List, Any, Optional

import httpx  # Add httpx for IPFS gateway requests

# Add parent directory to path to import app modules
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import aio_pika
from dotenv import load_dotenv

from app.db.connection import get_db_pool, init_db_pool, close_db_pool

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def hex_to_string(hex_string: str) -> str:
    """
    Convert a hex string to its ASCII representation.
    
    Args:
        hex_string: Hex string to convert
        
    Returns:
        ASCII string
    """
    try:
        # Remove '0x' prefix if present
        if hex_string.startswith('0x'):
            hex_string = hex_string[2:]
        
        # Convert hex to bytes then to string
        return bytes.fromhex(hex_string).decode('utf-8')
    except Exception as e:
        logger.error(f"Error converting hex to string: {e}")
        return hex_string


async def fetch_ipfs_content(cid: str, ipfs_node_url: str = None) -> Optional[bytes]:
    """Fetch content from the local IPFS node with external gateway fallback."""
    if not cid:
        return None
    
    # Use local IPFS service by default
    if ipfs_node_url is None:
        ipfs_node_url = os.getenv("IPFS_NODE_URL", "http://ipfs-service:5001")
    
    # Try local IPFS node first
    url = f"{ipfs_node_url}/api/v0/cat?arg={cid}"
    
    async with httpx.AsyncClient() as client:
        try:
            response = await client.post(url, timeout=10.0)
            response.raise_for_status()
            logger.info(f"✅ Successfully fetched content for CID {cid[:16]}... from local IPFS")
            return response.content
        except httpx.HTTPStatusError as e:
            logger.warning(f"IPFS node returned error for CID {cid}: {e}")
        except httpx.RequestError as e:
            logger.warning(f"Error fetching CID {cid} from local IPFS: {e}")
    
    # Fallback to external gateway
    logger.info(f"🌐 Trying external gateway for CID {cid[:16]}...")
    gateway_url = f"https://get.hippius.network/ipfs/{cid}"
    
    async with httpx.AsyncClient() as client:
        try:
            response = await client.get(gateway_url, timeout=20.0)
            response.raise_for_status()
            logger.info(f"✅ Successfully fetched content for CID {cid[:16]}... from external gateway")
            return response.content
        except httpx.HTTPStatusError as e:
            logger.error(f"External gateway returned error for CID {cid}: {e}")
        except httpx.RequestError as e:
            logger.error(f"Error fetching CID {cid} from external gateway: {e}")
    
    logger.error(f"❌ Failed to fetch content for CID {cid} from both local IPFS and external gateway")
    return None


async def fetch_ipfs_file_size(cid: str, ipfs_node_url: str = None) -> Optional[int]:
    """Fetch file size using the local IPFS node's files/stat API."""
    if not cid:
        return None
    
    # Use local IPFS service by default, fall back to environment variable
    if ipfs_node_url is None:
        ipfs_node_url = os.getenv("IPFS_NODE_URL", "http://ipfs-service:5001")
    
    # Use the local IPFS node's /files/stat endpoint
    stat_url = f"{ipfs_node_url}/api/v0/files/stat"
    params = {"arg": f"/ipfs/{cid}"}
    
    async with httpx.AsyncClient() as client:
        try:
            response = await client.post(stat_url, params=params, timeout=10.0)
            response.raise_for_status()
            stats = response.json()
            size = stats.get("Size") # Use 'Size' from files/stat
            if size is not None:
                logger.info(f"✅ Fetched size for CID {cid[:16]}...: {size:,} bytes (from local IPFS)")
                return int(size)
            else:
                logger.warning(f"Could not determine size from files/stat for CID {cid}. Stats: {stats}")
                # Try fallback with block/stat
                return await _fetch_ipfs_file_size_fallback_local(cid, ipfs_node_url)
        except (httpx.RequestError, httpx.HTTPStatusError, json.JSONDecodeError) as e:
            logger.error(f"Error fetching file size for CID {cid} via local files/stat: {e}")
            # Try fallback with block/stat
            return await _fetch_ipfs_file_size_fallback_local(cid, ipfs_node_url)


async def _fetch_ipfs_file_size_fallback_local(cid: str, ipfs_node_url: str) -> Optional[int]:
    """Fallback method using block/stat for local IPFS service."""
    block_url = f"{ipfs_node_url}/api/v0/block/stat"
    params = {"arg": cid}
    
    async with httpx.AsyncClient() as client:
        try:
            response = await client.post(block_url, params=params, timeout=10.0)
            response.raise_for_status()
            stats = response.json()
            size = stats.get("Size")
            if size is not None:
                logger.info(f"✅ Fetched block size (fallback) for CID {cid[:16]}...: {size:,} bytes (from local IPFS)")
                return int(size)
            else:
                logger.error(f"Could not determine size from block/stat fallback for CID {cid}. Stats: {stats}")
                return 0
        except Exception as e:
            logger.error(f"Local IPFS fallback method also failed for CID {cid}: {e}")
            return 0


class PinningRequestConsumer:
    """Consumer for processing pinning requests."""
    
    def __init__(self, rabbitmq_url: str = None):
        """
        Initialize the consumer.
        
        Args:
            rabbitmq_url: URL of the RabbitMQ server
        """
        self.rabbitmq_url = rabbitmq_url or os.getenv('RABBITMQ_URL', 'amqp://admin:admin@localhost:5672/')
        self.queue_name = 'pinning_request'
        
        self.rabbitmq_connection = None
        self.rabbitmq_channel = None
        self.db_pool = None
        
    async def connect(self):
        """Connect to RabbitMQ and database."""
        try:
            # Initialize database pool
            await init_db_pool()
            self.db_pool = get_db_pool()
            logger.info("Connected to database")
            
            # Connect to RabbitMQ
            self.rabbitmq_connection = await aio_pika.connect_robust(self.rabbitmq_url)
            self.rabbitmq_channel = await self.rabbitmq_connection.channel()
            
            # Set prefetch count to process one message at a time
            await self.rabbitmq_channel.set_qos(prefetch_count=1)
            
            logger.info(f"Connected to RabbitMQ")
            
        except Exception as e:
            logger.error(f"Failed to connect: {e}")
            raise
    
    async def _assign_file_to_owner(self, conn, cid: str, owner: str, filename: Optional[str] = None) -> None:
        """Helper to create a file_assignments entry for a single CID."""
        if not cid or not owner:
            return
        
        account = owner[:16] + "..."
        cid_short = cid[:20] + "..."
        
        # --- FETCH FILE SIZE ---
        logger.info(f"📌 FILE_ASSIGNMENT: account={account} CID={cid_short} - fetching file size")
        file_size = await fetch_ipfs_file_size(cid)
        if file_size is None:
            logger.warning(f"📌 FILE_ASSIGNMENT: account={account} CID={cid_short} - could not fetch size, using 0")
            file_size = 0
        else:
            logger.info(f"📌 FILE_ASSIGNMENT: account={account} CID={cid_short} - size={file_size:,} bytes")
            
        # Ensure the file exists in the files table first
        file_name_to_use = filename or f"file_{cid[:8]}"
        await conn.execute("""
            INSERT INTO files (cid, name, size)
            VALUES ($1, $2, $3)
            ON CONFLICT (cid) DO UPDATE SET
                name = EXCLUDED.name,
                size = EXCLUDED.size
        """, cid, file_name_to_use, file_size)
        
        # Create the file_assignments entry with NULL miners
        await conn.execute("""
            INSERT INTO file_assignments (cid, owner, miner1, miner2, miner3, miner4, miner5)
            VALUES ($1, $2, NULL, NULL, NULL, NULL, NULL)
            ON CONFLICT (cid) DO UPDATE SET
                owner = EXCLUDED.owner,
                updated_at = CURRENT_TIMESTAMP
        """, cid, owner)
        logger.info(f"✅ FILE_ASSIGNMENT_COMPLETE: account={account} CID={cid_short} - added to file_assignments table")
    
    async def _process_manifest_files_parallel(self, manifest_data: List, owner: str) -> int:
        if not manifest_data:
            return 0
            
        file_assignments = []
        
        for i, file_info in enumerate(manifest_data):
            if isinstance(file_info, dict):
                file_cid = file_info.get('cid')
                file_name = file_info.get('filename') or file_info.get('name') or f"file_{i+1}.bin"
                
                if file_cid:
                    file_assignments.append({
                        'cid': file_cid,
                        'owner': owner,
                        'filename': file_name,
                        'index': i + 1
                    })
                else:
                    logger.warning(f"Skipping manifest entry {i+1}: missing 'cid' field")
                    
            elif isinstance(file_info, str):
                file_name = f"file_{i+1}.bin"
                file_assignments.append({
                    'cid': file_info,
                    'owner': owner,
                    'filename': file_name,
                    'index': i + 1
                })
            else:
                logger.warning(f"Skipping invalid manifest entry {i+1}: {file_info}")
        
        if not file_assignments:
            logger.warning("No valid file assignments found in manifest")
            return 0
        
        return await self._batch_process_file_assignments(file_assignments)
    
    async def _batch_process_file_assignments(self, file_assignments: List[Dict]) -> int:
        # Create semaphore for parallel file size fetching (limit to 20 concurrent)
        semaphore = asyncio.Semaphore(20)
        
        async def fetch_file_size_with_semaphore(assignment: Dict) -> tuple:
            async with semaphore:
                file_size = await fetch_ipfs_file_size(assignment['cid'])
                return (
                    assignment['cid'],
                    assignment['filename'],
                    file_size if file_size is not None else 0,
                    assignment['owner']
                )
        
        # Fetch all file sizes in parallel
        logger.info(f"📏 Fetching file sizes for {len(file_assignments)} files in parallel (max 20 concurrent)")
        tasks = [fetch_file_size_with_semaphore(assignment) for assignment in file_assignments]
        results = await asyncio.gather(*tasks)
        
        async with self.db_pool.acquire() as conn:
            async with conn.transaction():
                files_data = []
                assignments_data = []
                
                for cid, filename, file_size, owner in results:
                    files_data.append((cid, filename, file_size))
                    assignments_data.append((cid, owner))
                
                await conn.executemany("""
                    INSERT INTO files (cid, name, size)
                    VALUES ($1, $2, $3)
                    ON CONFLICT (cid) DO UPDATE SET
                        name = EXCLUDED.name,
                        size = EXCLUDED.size
                """, files_data)
                
                await conn.executemany("""
                    INSERT INTO file_assignments (cid, owner, miner1, miner2, miner3, miner4, miner5)
                    VALUES ($1, $2, NULL, NULL, NULL, NULL, NULL)
                    ON CONFLICT (cid) DO UPDATE SET
                        owner = EXCLUDED.owner,
                        updated_at = CURRENT_TIMESTAMP
                """, assignments_data)
                
                return len(file_assignments)
    
    async def process_pinning_request(self, request_data: Dict[str, Any]) -> bool:
        """
        Process a pinning request, handling manifest CIDs from blockchain storage requests.
        """
        request_hash = request_data.get('request_hash')
        owner = request_data.get('owner')
        
        if not request_hash or not owner:
            logger.error(f"Invalid request data: missing request_hash or owner. Data: {request_data}")
            return False
        
        # ===== STORAGE REQUEST TRACING - STEP 5: CONSUMED FROM QUEUE =====
        account = owner[:16] + "..."
        request_hash_short = request_hash[:16] + "..."
        logger.info(f"📬 QUEUE_CONSUME: Processing storage request account={account} request_hash={request_hash_short}")
        
        # Extract and log CID information early for tracing
        file_hash_hex = request_data.get('file_hash', '')
        cid = "N/A"
        try:
            if file_hash_hex:
                if isinstance(file_hash_hex, str):
                    if file_hash_hex.startswith('0x'):
                        cid = bytes.fromhex(file_hash_hex[2:]).decode('utf-8')
                    else:
                        cid = bytes.fromhex(file_hash_hex).decode('utf-8')
                    logger.info(f"📬 QUEUE_CONSUME: account={account} CID={cid[:20]}... request_hash={request_hash_short}")
                else:
                    logger.warning(f"📬 QUEUE_CONSUME: account={account} file_hash is not string: {type(file_hash_hex)}")
        except Exception as e:
            logger.warning(f"📬 QUEUE_CONSUME: account={account} could not parse CID from file_hash: {e}")
        
        async with self.db_pool.acquire() as conn:
            # Check if this request has already been processed to avoid re-work
            existing = await conn.fetchrow("SELECT id FROM processed_pinning_requests WHERE request_hash = $1", request_hash)
            if existing:
                logger.info(f"🔄 ALREADY_PROCESSED: account={account} CID={cid[:20] if cid != 'N/A' else 'N/A'}... request_hash={request_hash_short} - skipping")
                return True
        
        try:
            file_hash_hex = request_data.get('file_hash', '')
            manifest_cid = hex_to_string(file_hash_hex) if file_hash_hex else ''
            
            if not manifest_cid:
                logger.error(f"❌ PIN_REQUEST_ERROR: account={account} request_hash={request_hash_short} - No file_hash found")
                return False
            
            files_processed = 0
            
            # ===== STORAGE REQUEST TRACING - STEP 6: PROCESSING PIN REQUEST =====
            logger.info(f"📌 PIN_REQUEST_START: account={account} CID={manifest_cid[:20]}... request_hash={request_hash_short}")
            
            # Try to fetch and parse the manifest
            logger.info(f"📌 PIN_REQUEST_FETCH: account={account} CID={manifest_cid[:20]}... - fetching manifest content")
            manifest_content = await fetch_ipfs_content(manifest_cid)
            
            if manifest_content:
                # Successfully fetched manifest content
                logger.info(f"📌 PIN_REQUEST_FETCH_SUCCESS: account={account} CID={manifest_cid[:20]}... - manifest fetched, parsing content")
                try:
                    manifest_data = json.loads(manifest_content)
                    
                    if isinstance(manifest_data, list):
                        # Valid manifest format - list of files
                        logger.info(f"📌 PIN_REQUEST_MANIFEST: account={account} CID={manifest_cid[:20]}... - manifest contains {len(manifest_data)} files")
                        
                        # Process all files in parallel - MAJOR PERFORMANCE BOOST!
                        files_processed = await self._process_manifest_files_parallel(manifest_data, owner)
                        logger.info(f"📌 PIN_REQUEST_FILES: account={account} CID={manifest_cid[:20]}... - processed {files_processed} files from manifest")
                        
                    elif isinstance(manifest_data, dict):
                        # Single file object format
                        logger.info(f"📌 PIN_REQUEST_SINGLE: account={account} CID={manifest_cid[:20]}... - manifest contains single file object")
                        file_cid = manifest_data.get('cid')
                        file_name = manifest_data.get('filename') or manifest_data.get('name') or 'manifest_file.bin'
                        
                        if file_cid:
                            logger.info(f"📌 PIN_REQUEST_SINGLE: account={account} manifest_CID={manifest_cid[:20]}... file_CID={file_cid[:20]}...")
                            async with self.db_pool.acquire() as conn:
                                await self._assign_file_to_owner(conn, file_cid, owner, file_name)
                            files_processed = 1
                        else:
                            # No CID in manifest, treat manifest itself as the file
                            logger.info(f"📌 PIN_REQUEST_SINGLE: account={account} CID={manifest_cid[:20]}... - no file CID, treating manifest as file")
                            async with self.db_pool.acquire() as conn:
                                await self._assign_file_to_owner(conn, manifest_cid, owner, request_data.get('file_name') or 'manifest.json')
                            files_processed = 1
                    else:
                        # Not a JSON object/array, treat as raw file
                        logger.info(f"📌 PIN_REQUEST_RAW: account={account} CID={manifest_cid[:20]}... - content is not JSON, treating as raw file")
                        async with self.db_pool.acquire() as conn:
                            await self._assign_file_to_owner(conn, manifest_cid, owner, request_data.get('file_name') or 'data.bin')
                        files_processed = 1
                        
                except json.JSONDecodeError:
                    # Not a JSON file, treat manifest CID as a single file
                    logger.info(f"📌 PIN_REQUEST_BINARY: account={account} CID={manifest_cid[:20]}... - not JSON, treating as binary file")
                    async with self.db_pool.acquire() as conn:
                        await self._assign_file_to_owner(conn, manifest_cid, owner, request_data.get('file_name') or 'data.bin')
                    files_processed = 1
                    
            else:
                # Could not fetch manifest content from both local IPFS and external gateway
                logger.error(f"❌ PIN_REQUEST_FETCH_FAILED: account={account} CID={manifest_cid[:20]}... - could not fetch from IPFS or gateway")
                logger.error(f"❌ PIN_REQUEST_ERROR: account={account} request_hash={request_hash_short} - manifest not accessible, skipping")
                return False
            
            # Record that we've processed this storage request
            async with self.db_pool.acquire() as conn:
                # ===== STORAGE REQUEST TRACING - STEP 8: STORING request_hash IN DATABASE =====
                logger.info(f"💾 REQUEST_HASH_STORE: Storing request data in pinning_requests table account={account} request_hash={request_hash_short}")
                logger.info(f"💾 REQUEST_HASH_STORE: Data being stored - owner={owner} file_hash_hex={file_hash_hex[:20]}... file_name={request_data.get('file_name', 'N/A')}")
                
                # Check if this request_hash already exists in pinning_requests
                existing_pinning = await conn.fetchrow("SELECT id FROM pinning_requests WHERE request_hash = $1", request_hash)
                if not existing_pinning:
                    # Store the request in pinning_requests table for tracking
                    await conn.execute("""
                        INSERT INTO pinning_requests (request_hash, owner, file_hash, file_name)
                        VALUES ($1, $2, $3, $4)
                    """, request_hash, owner, file_hash_hex, request_data.get('file_name', ''))
                    
                    logger.info(f"💾 REQUEST_HASH_STORE: Successfully stored NEW request_hash={request_hash_short} in pinning_requests table")
                    logger.info(f"💾 REQUEST_HASH_STORE: Full request_hash={request_hash}")
                else:
                    logger.info(f"💾 REQUEST_HASH_STORE: request_hash={request_hash_short} already exists in pinning_requests table (id={existing_pinning['id']})")
                
                # Record that we've processed this storage request  
                await conn.execute("""
                    INSERT INTO processed_pinning_requests (request_hash, miner_count)
                    VALUES ($1, $2) ON CONFLICT DO NOTHING
                """, request_hash, files_processed)
            
            # ===== STORAGE REQUEST TRACING - STEP 7: PROCESSING COMPLETE =====
            logger.info(f"✅ PIN_REQUEST_COMPLETE: account={account} CID={manifest_cid[:20] if manifest_cid else 'N/A'}... request_hash={request_hash_short} files_processed={files_processed}")
            return True
                
        except Exception as e:
            account = owner[:20] + "..." if owner else "unknown"
            request_hash_short = request_hash[:16] + "..." if request_hash else "unknown"
            logger.error(f"❌ PIN_REQUEST_ERROR: account={account} request_hash={request_hash_short} error={str(e)}")
            logger.exception("Full traceback:")
            return False
    
    async def process_message(self, message: aio_pika.IncomingMessage):
        """
        Process a single message from the queue.
        
        Args:
            message: The message to process
        """
        async with message.process():
            try:
                # Parse message body
                data = json.loads(message.body.decode())
                
                # Extract account and request hash for tracing
                account = data.get('owner', 'unknown')[:16] + "..."
                request_hash = data.get('request_hash', 'unknown')[:16] + "..."
                
                logger.info(f"📬 MESSAGE_RECEIVED: account={account} request_hash={request_hash} - processing message")
                logger.debug(f"Full message data: {json.dumps(data, indent=2)}")
                
                # Process the pinning request
                success = await self.process_pinning_request(data)
                
                if not success:
                    # Reject and requeue if processing failed
                    logger.error(f"📬 MESSAGE_FAILED: account={account} request_hash={request_hash} - processing failed, will requeue")
                    raise Exception(f"Failed to process pinning request for account {account}, request_hash {request_hash}")
                else:
                    logger.info(f"📬 MESSAGE_SUCCESS: account={account} request_hash={request_hash} - processing completed successfully")
                
            except json.JSONDecodeError as e:
                logger.error(f"📬 MESSAGE_JSON_ERROR: Invalid JSON in message - {e}")
                # Don't requeue invalid JSON messages
                return
            except Exception as e:
                logger.error(f"📬 MESSAGE_ERROR: Error processing message - {e}")
                # Message will be requeued due to the exception
                raise
    
    async def start_consuming(self):
        """Start consuming messages from the queue."""
        try:
            # Declare the queue
            queue = await self.rabbitmq_channel.declare_queue(
                self.queue_name,
                durable=True
            )
            
            logger.info(f"Starting to consume from queue '{self.queue_name}'")
            
            # Start consuming
            await queue.consume(self.process_message)
            
            # Keep the consumer running
            await asyncio.Future()
            
        except Exception as e:
            logger.error(f"Error in consumer: {e}")
            raise
    
    async def close(self):
        """Close all connections."""
        if self.rabbitmq_connection:
            await self.rabbitmq_connection.close()
            logger.info("Closed RabbitMQ connection")
        
        if self.db_pool:
            await close_db_pool()
            logger.info("Closed database pool")


async def main():
    """Main entry point for the consumer."""
    consumer = PinningRequestConsumer()
    
    try:
        # Connect to services
        await consumer.connect()
        
        # Start consuming
        await consumer.start_consuming()
        
    except KeyboardInterrupt:
        logger.info("Received interrupt signal, shutting down...")
    except Exception as e:
        logger.error(f"Consumer error: {e}")
    finally:
        await consumer.close()


if __name__ == "__main__":
    asyncio.run(main()) 