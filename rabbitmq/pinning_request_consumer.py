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
from typing import Any, Optional

import httpx  # Add httpx for IPFS gateway requests

# Add parent directory to path to import app modules
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import aio_pika
from dotenv import load_dotenv

from app.db.connection import close_db_pool, get_db_pool, init_db_pool
from app.utils.config import get_ipfs_node_url

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
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
        if hex_string.startswith("0x"):
            hex_string = hex_string[2:]

        # Convert hex to bytes then to string
        return bytes.fromhex(hex_string).decode("utf-8")
    except Exception as e:
        logger.error(f"Error converting hex to string: {e}")
        return hex_string


async def fetch_ipfs_content(cid: str, ipfs_node_url: str = None) -> Optional[bytes]:
    """Fetch content from the local IPFS node with external gateway fallback."""
    if not cid:
        return None
    cid = str(cid)

    # Use local IPFS service by default
    if ipfs_node_url is None:
        ipfs_node_url = get_ipfs_node_url()

    # Try local IPFS node first
    url = f"{ipfs_node_url}/api/v0/cat?arg={cid}"

    async with httpx.AsyncClient() as client:
        try:
            response = await client.post(url, timeout=7)
            response.raise_for_status()
            logger.info(f"✅ Successfully fetched content for {cid=} from IPFS {url=}")
            return response.content
        except httpx.HTTPStatusError as e:
            logger.warning(f"IPFS node returned error for {cid=} {url=}: {e}")
        except httpx.RequestError as e:
            logger.warning(f"Error fetching {cid=} {url=}: {e}")
    return None


async def fetch_ipfs_file_size(cid: str) -> Optional[int]:
    """Fetch file size using the local IPFS node's files/stat API."""
    if not cid:
        return None

    # Use centralized IPFS node URL function
    from app.utils.config import get_ipfs_node_url

    ipfs_node_url = get_ipfs_node_url()
    stat_url = f"{ipfs_node_url}/api/v0/files/stat"
    params = {"arg": f"/ipfs/{cid}"}

    async with httpx.AsyncClient() as client:
        try:
            logger.info(f"Stating {cid=} {stat_url=} {params=}")
            response = await client.post(stat_url, params=params, timeout=10)
            response.raise_for_status()
            stats = response.json()
            # files/stat returns CumulativeSize for total size of the file
            size = stats.get("CumulativeSize")

            if size is not None:
                logger.info(f"✅ Fetched size for {cid=} {size:,} bytes")
                return int(size)
            else:
                logger.warning(f"Could not determine size from files/stat for CID {cid}. Stats: {stats}")
                return 0
        except (httpx.RequestError, httpx.HTTPStatusError, json.JSONDecodeError):
            logger.exception(f"Error fetching file size for CID {cid} via local files/stat")
            return None


class PinningRequestConsumer:
    """Consumer for processing pinning requests."""

    def __init__(self, rabbitmq_url: str = None):
        """
        Initialize the consumer.

        Args:
            rabbitmq_url: URL of the RabbitMQ server
        """
        self.rabbitmq_url = rabbitmq_url or os.getenv("RABBITMQ_URL", "amqp://admin:admin@localhost:5672/")
        self.queue_name = "pinning_request"

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
            logger.info("Connected to RabbitMQ")

        except Exception as e:
            logger.error(f"Failed to connect: {e}")
            raise

    async def _process_manifest_files_parallel(self, manifest_data: list, owner: str) -> int:
        if not manifest_data:
            return 0

        file_assignments = []

        for i, file_info in enumerate(manifest_data):
            if isinstance(file_info, dict):
                file_cid = file_info.get("cid")
                file_name = file_info.get("filename") or file_info.get("name") or f"file_{i + 1}.bin"

                if file_cid:
                    file_assignments.append(
                        {
                            "cid": file_cid,
                            "owner": owner,
                            "filename": file_name,
                            "index": i + 1,
                        }
                    )
                else:
                    logger.warning(f"Skipping manifest entry {i + 1}: missing 'cid' field")

            elif isinstance(file_info, str):
                file_name = f"file_{i + 1}.bin"
                file_assignments.append(
                    {
                        "cid": file_info,
                        "owner": owner,
                        "filename": file_name,
                        "index": i + 1,
                    }
                )
            else:
                logger.warning(f"Skipping invalid manifest entry {i + 1}: {file_info}")

        if not file_assignments:
            logger.warning("No valid file assignments found in manifest")
            return 0

        return await self._batch_process_file_assignments(file_assignments)

    async def _batch_process_file_assignments(self, file_assignments: list[dict]) -> int:
        async with self.db_pool.acquire() as conn:
            async with conn.transaction():
                files_data = []
                assignments_data = []

                for item in file_assignments:
                    cid = item["cid"]
                    filename = item["filename"]
                    logger.info(f"Processing file assignment {cid=} {item['owner']}")

                    # Fetch actual file size from IPFS instead of hardcoding 0
                    file_size = await fetch_ipfs_file_size(cid)
                    if file_size is None:
                        logger.warning(f"Could not fetch size for {cid}, defaulting to 0")
                        file_size = 0

                    files_data.append((cid, filename, file_size))
                    assignments_data.append((cid, item["owner"]))

                if files_data:
                    await conn.executemany(
                        """
                        INSERT INTO files (cid, name, size)
                        VALUES ($1, $2, $3)
                        ON CONFLICT (cid) DO UPDATE SET
                            name = EXCLUDED.name,
                            size = EXCLUDED.size
                    """,
                        files_data,
                    )

                    # Write to pending_assignment_file instead of file_assignments
                    # This ensures new storage request files trigger profile reconstruction
                    pending_data = []
                    for i, (cid, filename, file_size) in enumerate(files_data):
                        owner = assignments_data[i][1]  # Get corresponding owner
                        pending_data.append((cid, owner, filename, file_size))

                    await conn.executemany(
                        """
                        INSERT INTO pending_assignment_file (cid, owner, filename, file_size_bytes, status)
                        VALUES ($1, $2, $3, $4, 'processed')
                        ON CONFLICT (cid, owner) DO UPDATE SET
                            filename = EXCLUDED.filename,
                            file_size_bytes = EXCLUDED.file_size_bytes,
                            status = 'processed',
                            updated_at = CURRENT_TIMESTAMP
                    """,
                        pending_data,
                    )

                return len(file_assignments)

    async def process_pinning_request(self, request_data: dict[str, Any]) -> bool:
        """
        Process a pinning request, handling manifest CIDs from blockchain storage requests.
        """
        request_hash = request_data.get("request_hash")
        owner = request_data.get("owner")

        if not request_hash or not owner:
            logger.error(f"Invalid request data: missing request_hash or owner. Data: {request_data}")
            return False

        async with self.db_pool.acquire() as conn:
            # Check if this request has already been processed to avoid re-work
            existing = await conn.fetchrow(
                "SELECT id FROM pinning_requests WHERE request_hash = $1",
                request_hash,
            )
            if existing:
                return True

        try:
            file_hash_hex = request_data.get("file_hash", "")
            manifest_cid = hex_to_string(file_hash_hex) if file_hash_hex else ""

            if not manifest_cid:
                return False

            manifest_content = await fetch_ipfs_content(manifest_cid)
            manifest_data = json.loads(manifest_content)
            files_processed = await self._process_manifest_files_parallel(manifest_data, owner)

            # Record that we've processed this storage request
            async with self.db_pool.acquire() as conn:
                # Check if this request_hash already exists in pinning_requests
                existing_pinning = await conn.fetchrow(
                    "SELECT id FROM pinning_requests WHERE request_hash = $1",
                    request_hash,
                )
                if not existing_pinning:
                    # Store the request in pinning_requests table for tracking
                    await conn.execute(
                        """
                        INSERT INTO pinning_requests (request_hash, owner, file_hash, file_name)
                        VALUES ($1, $2, $3, $4)
                    """,
                        request_hash,
                        owner,
                        file_hash_hex,
                        request_data.get("file_name", ""),
                    )

                # The request is now recorded in pinning_requests table above
                logger.info(f"Successfully processed storage request {request_hash} with {files_processed} files")

            return True

        except Exception:
            logger.exception(f"Failed to process {request_data=}")
            return False

    async def process_message(self, message: aio_pika.IncomingMessage):
        """
        Process a single message from the queue.

        Args:
            message: The message to process
        """
        async with message.process():
            data = json.loads(message.body.decode())
            await self.process_pinning_request(data)

    async def start_consuming(self):
        """Start consuming messages from the queue."""
        try:
            # Declare the queue
            queue = await self.rabbitmq_channel.declare_queue(self.queue_name, durable=True)

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
