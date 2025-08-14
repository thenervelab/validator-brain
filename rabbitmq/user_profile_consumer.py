"""
Consumer for processing user profiles from RabbitMQ queue.

This consumer:
1. Reads messages from the user_profile queue
2. Fetches the profile data from IPFS using the CID
3. Parses the profile to extract file information
4. Updates the files and file_assignments tables
"""

import asyncio
import json
import logging
import os
import sys
from datetime import datetime
from typing import Optional

# Add parent directory to path to import app modules
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import aio_pika
import httpx
from dotenv import load_dotenv

from app.db.connection import close_db_pool, get_db_pool, init_db_pool
from app.utils.config import get_ipfs_node_url
from substrate_fetcher.ipfs_profile_parser import (
    bytes_to_ipfs_cid,
    parse_user_profile_files,
)

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

# Maximum file size limit (100 GB in bytes)
MAX_FILE_SIZE = 100 * 1024 * 1024 * 1024  # 107,374,182,400 bytes


class UserProfileConsumer:
    """Consumer for processing user profiles."""

    def __init__(self, rabbitmq_url: str = None, ipfs_gateway: str = None):
        """
        Initialize the consumer.

        Args:
            rabbitmq_url: URL of the RabbitMQ server
            ipfs_gateway: URL of the IPFS gateway
        """
        self.rabbitmq_url = rabbitmq_url or os.getenv("RABBITMQ_URL", "amqp://admin:admin@localhost:5672/")
        # Use the centralized config for IPFS URL
        self.ipfs_gateway = get_ipfs_node_url()
        self.queue_name = "user_profile"

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

    async def fetch_from_ipfs(self, cid: str) -> Optional[bytes]:
        """
        Fetch content from IPFS using the API endpoint.

        Args:
            cid: Content ID to fetch

        Returns:
            Content bytes or None if failed
        """
        # Use IPFS API endpoint for fetching content
        api_url = f"{self.ipfs_gateway}/api/v0/cat?arg={cid}"

        async with httpx.AsyncClient(timeout=30.0, follow_redirects=True) as client:
            try:
                response = await client.post(api_url)
                if response.status_code == 200:
                    return response.content
                else:
                    logger.error(f"Failed to fetch CID {cid}: HTTP {response.status_code}")
                    return None
            except Exception as e:
                logger.error(f"Error fetching CID {cid}: {e}")
                return None

    async def process_user_profile(self, account: str, cid: str) -> int:
        """
        Process a user profile by fetching and parsing it.

        Args:
            account: SS58 address of the user
            cid: Content ID of the profile

        Returns:
            Number of files processed
        """
        logger.info(f"Processing user profile for {account} with CID {cid}")

        # Fetch profile from IPFS
        profile_data = await self.fetch_from_ipfs(cid)
        if not profile_data:
            logger.error(f"Failed to fetch profile for {account}")
            return 0

        # Parse the profile
        try:
            # Parse JSON data
            profile_json = json.loads(profile_data)
            files = parse_user_profile_files(profile_json)
            logger.info(f"Found {len(files)} files in profile for {account}")
        except Exception as e:
            logger.error(f"Failed to parse profile for {account}: {e}")
            return 0

        # Process each file
        processed_count = 0
        async with self.db_pool.acquire() as conn:
            for file_info in files:
                try:
                    # Extract file details
                    file_hash_bytes = file_info.get("file_hash")  # This is a byte array
                    file_name = file_info.get("file_name", "unknown")
                    file_size = file_info.get("file_size_in_bytes", 0)
                    miner_ids = file_info.get("miner_ids", [])

                    if not file_hash_bytes:
                        logger.warning(f"File without file_hash in profile for {account}: {file_info}")
                        continue

                    # Convert byte array to CID string, handle both byte arrays and strings
                    if isinstance(file_hash_bytes, list):
                        # It's a byte array, convert it
                        file_cid = bytes_to_ipfs_cid(file_hash_bytes)
                    elif isinstance(file_hash_bytes, str):
                        # It's already a string CID
                        file_cid = file_hash_bytes
                    else:
                        logger.warning(f"Unknown file_hash type {type(file_hash_bytes)} for {account}")
                        continue

                    if not file_cid:
                        logger.warning(f"Failed to convert file_hash to CID for {account}")
                        continue

                    # Check if file exceeds maximum size limit and purge if needed
                    if file_size > MAX_FILE_SIZE:
                        logger.warning(
                            f"File {file_cid} exceeds maximum size ({file_size:,} bytes > {MAX_FILE_SIZE:,} bytes), purging from database..."
                        )

                        # # Delete from files table
                        # await conn.execute("DELETE FROM files WHERE cid = $1", file_cid)
                        #
                        # # Delete from file_assignments table
                        # await conn.execute("DELETE FROM file_assignments WHERE cid = $1", file_cid)

                        logger.info(f"Purged oversized file {file_cid} from files and file_assignments tables")
                        continue

                    # Track this file as active from chain
                    await conn.execute(
                        """
                        INSERT INTO active_files_from_chain (cid, last_seen)
                        VALUES ($1, CURRENT_TIMESTAMP)
                        ON CONFLICT (cid) DO UPDATE SET last_seen = CURRENT_TIMESTAMP
                        """,
                        file_cid,
                    )

                    # Insert into files table (skip if exists)
                    await conn.execute(
                        """
                        INSERT INTO files (cid, name, size, created_at)
                        VALUES ($1, $2, $3, $4)
                        ON CONFLICT (cid) DO NOTHING
                    """,
                        file_cid,
                        file_name,
                        file_size,
                        datetime.utcnow(),
                    )

                    # Filter out miners that don't exist in registration table
                    valid_miners = []
                    for miner_id in miner_ids:
                        if miner_id:
                            # Check if miner exists in registration table
                            exists = await conn.fetchval(
                                """
                                SELECT 1 FROM registration WHERE node_id = $1 LIMIT 1
                            """,
                                miner_id,
                            )
                            if exists:
                                valid_miners.append(miner_id)

                    # Deduplicate miners while preserving order to prevent same miner in multiple slots
                    valid_miners_unique = []
                    for miner in valid_miners:
                        if miner not in valid_miners_unique:
                            valid_miners_unique.append(miner)

                    # Update file_assignments table with only valid miners (empty slots will be reassigned later)
                    # Pad valid_miners to 5 elements
                    miners_padded = (valid_miners_unique + [None] * 5)[:5]

                    await conn.execute(
                        """
                        INSERT INTO file_assignments (cid, owner, miner1, miner2, miner3, miner4, miner5)
                        VALUES ($1, $2, $3, $4, $5, $6, $7)
                        ON CONFLICT (cid) DO UPDATE SET
                            owner = EXCLUDED.owner,
                            miner1 = EXCLUDED.miner1,
                            miner2 = EXCLUDED.miner2,
                            miner3 = EXCLUDED.miner3,
                            miner4 = EXCLUDED.miner4,
                            miner5 = EXCLUDED.miner5
                    """,
                        file_cid,
                        account,
                        miners_padded[0],
                        miners_padded[1],
                        miners_padded[2],
                        miners_padded[3],
                        miners_padded[4],
                    )

                    processed_count += 1

                except Exception:
                    logger.exception(f"Error processing file in profile for {account=} {file_info=}")
                    continue

        logger.info(f"Successfully processed {processed_count} files for {account}")

        return processed_count

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
                account = data.get("account")
                cid = data.get("cid")

                if not account or not cid:
                    logger.error(f"Invalid message format: {data}")
                    return

                # Process the profile
                await self.process_user_profile(account, cid)

            except Exception as e:
                logger.error(f"Error processing message: {e}")  # Log error and continue processing

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

    async def cleanup_orphaned_files(self):
        """
        Clean up orphaned files that are no longer referenced in any user profiles from chain.
        This removes files from both 'files' and 'file_assignments' tables that are not
        present in the active_files_from_chain tracking table.
        """
        async with self.db_pool.acquire() as conn:
            async with conn.transaction():
                # Count files before cleanup
                files_count_before = await conn.fetchval("SELECT COUNT(*) FROM files")
                assignments_count_before = await conn.fetchval("SELECT COUNT(*) FROM file_assignments")

                # Delete orphaned files using efficient NOT EXISTS query
                deleted_files = await conn.fetchval("""
                    SELECT FROM files f
                    WHERE NOT EXISTS (
                        SELECT 1 FROM active_files_from_chain a 
                        WHERE a.cid = f.cid
                    )
                    """)

                for row in deleted_files:
                    logger.warning(f"About to delete orphan CID from files {row}")

                # Delete orphaned file assignments using efficient NOT EXISTS query
                deleted_assignments = await conn.fetchval("""
                    SELECT FROM file_assignments fa
                    WHERE NOT EXISTS (
                        SELECT 1 FROM active_files_from_chain a 
                        WHERE a.cid = fa.cid
                    )
                    """)

                for row in deleted_assignments:
                    logger.warning(f"About to delete orphan CID from deleted_assignments {row}")

                # Count files after cleanup
                files_count_after = await conn.fetchval("SELECT COUNT(*) FROM files")
                assignments_count_after = await conn.fetchval("SELECT COUNT(*) FROM file_assignments")

                files_removed = files_count_before - files_count_after
                assignments_removed = assignments_count_before - assignments_count_after

                logger.info(
                    f"Orphaned file cleanup completed: "
                    f"removed {files_removed} files and {assignments_removed} assignments"
                )

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
    consumer = UserProfileConsumer()

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
