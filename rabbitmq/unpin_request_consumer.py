import asyncio
import json
import logging
import os
from json import JSONDecodeError
from typing import Any

import aio_pika

from app.db.connection import close_db_pool, get_db_pool, init_db_pool
from rabbitmq.pinning_request_consumer import fetch_ipfs_content

logger = logging.getLogger(__name__)


def get_cid_version(cid: str) -> int:
    if cid.startswith("Qm") and len(cid) == 46:
        return True
    elif cid.startswith(("b", "z", "f")):  # f for base16
        return True
    else:
        return False


def hex_to_string(hex_string: str) -> str:
    """
    Convert a hex string to its ASCII representation.

    Args:
        hex_string: Hex string to convert

    Returns:
        ASCII string
    """
    try:
        cid = bytes.fromhex(hex_string).decode("utf-8")
        if not get_cid_version(cid):  # double encoded
            return str(bytes.fromhex(cid))
        else:
            return cid
    except Exception as e:
        logger.error(f"Error converting hex to string: {e}")
        return hex_string


class UnpinRequestConsumer:
    """Consumer for processing unpin requests."""

    def __init__(self, rabbitmq_url: str = None):
        """
        Initialize the consumer.

        Args:
            rabbitmq_url: URL of the RabbitMQ server
        """
        self.rabbitmq_url = rabbitmq_url or os.getenv("RABBITMQ_URL", "amqp://admin:admin@localhost:5672/")
        self.queue_name = "unpin_request"

        self.rabbitmq_connection = None
        self.rabbitmq_channel = None
        self.db_pool = None
        self.unpin_confirmations = []  # Collect confirmations for batch submission

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

    async def _unpin_single_file(self, cid: str, owner: str, conn):
        """Helper to unpin a single CID and clean up database entries."""
        logger.info(f"UNPINNING FROM DATABASE {cid=} {owner=}")
        # Check if file exists and belongs to this owner
        file_record = await conn.fetchrow(
            """
            SELECT fa.cid, fa.owner, f.size, fa.miner1, fa.miner2, fa.miner3, fa.miner4, fa.miner5
            FROM file_assignments fa
            JOIN files f ON fa.cid = f.cid
            WHERE fa.cid = $1 AND fa.owner = $2
        """,
            cid,
            owner,
        )

        if not file_record:
            logger.warning(f"Did not find {cid=} {owner=} to delete from database")
            return False

        async with conn.transaction():
            # Delete from files table (CASCADE will handle file_assignments)
            await conn.execute("DELETE FROM files WHERE cid = $1", cid)

            # Clean up profile entries
            await conn.execute("DELETE FROM user_profile WHERE file_hash = $1 AND owner = $2", cid, owner)
            await conn.execute("DELETE FROM miner_profile WHERE file_hash = $1", cid)

            # Clean up pending entries
            await conn.execute("DELETE FROM pending_assignment_file WHERE cid = $1", cid)
            await conn.execute("DELETE FROM pending_user_profile WHERE cid = $1", cid)
            await conn.execute("DELETE FROM pending_miner_profile WHERE cid = $1", cid)

            # Clean up monitoring data
            await conn.execute("DELETE FROM file_failures WHERE cid = $1", cid)

            # Clean up storage requests if still pending
            await conn.execute("DELETE FROM storage_requests WHERE file_hash = $1 AND owner = $2", cid, owner)

        logger.info(f"Successfully deleted {cid=} {owner=} from all tables")

    async def _process_manifest_files_parallel(self, manifest_data: list, owner: str, conn):
        """Process manifest files for unpinning in parallel."""
        if not manifest_data:
            return

        # Process all files in parallel with semaphore
        semaphore = asyncio.Semaphore(3)

        async def unpin_with_semaphore(cid, owner, conn):
            async with semaphore:
                await self._unpin_single_file(cid, owner, conn)

        logger.info(f"🗑️ Unpinning {len(manifest_data)} files from {owner=}")

        tasks = [
            unpin_with_semaphore(
                item["cid"],
                item["owner"],
                conn,
            )
            for item in manifest_data
        ]
        await asyncio.gather(*tasks, return_exceptions=True)

    async def process_unpin_request(self, request_data: dict[str, Any]):
        """
        Process an unpin request, handling manifest CIDs from blockchain unpin requests.
        """
        owner = request_data.get("owner")
        file_hash_hex = request_data["file_hash"]
        request_id = f"{owner}_{file_hash_hex}"
        cid = hex_to_string(file_hash_hex)

        async with self.db_pool.acquire() as conn:
            # Check if this request has already been processed
            if await conn.fetchrow("SELECT id FROM processed_unpin_requests WHERE request_id = $1", request_id):
                logger.info(f"🔄 ALREADY_PROCESSED: request_id={request_id}... - skipping")
                return True

            await conn.execute(
                "INSERT INTO processed_unpin_requests (request_id, owner, file_hash, status) VALUES ($1, $2, $3, $4)",
                request_id,
                owner,
                file_hash_hex,
                "unprocessed",
            )

            manifest_data = await fetch_ipfs_content(cid)

            if not manifest_data:
                logger.warning(f"Could not fetch manifest data for cid={cid} - treating as already processed")
                return True

            try:
                logger.info(f"Deserializing {manifest_data[:32]=}")
                manifest_data = json.loads(manifest_data)
            except (UnicodeDecodeError, JSONDecodeError) as e:  # it's probably not a manifest, just the cid to unpin
                logger.error(
                    f"Failed to decode unpin manifest JSON for request_id={request_id}, cid={cid}: {str(e)[:20]}"
                )
                manifest_data = [
                    {
                        "cid": cid,
                        "owner": owner,
                    }
                ]

            try:
                await self._process_manifest_files_parallel(
                    manifest_data,
                    owner,
                    conn,
                )
            except Exception as e:
                logger.exception(f"Failed to process manifest for cid={cid}: {e}")
                return True

        return True

    async def process_message(self, message: aio_pika.IncomingMessage):
        """
        Process a single message from the queue.

        Args:
            message: The message to process
        """
        logger.info("🔍 UNPIN_DEBUG: Consumer received message, starting process_message")
        async with message.process():
            try:
                data = json.loads(message.body.decode())

                logger.info(f"🔍 UNPIN_DEBUG: About to call process_unpin_request for {data}")
                success = await self.process_unpin_request(data)

                if not success:
                    # Reject and requeue if processing failed
                    raise Exception(f"Failed to process unpin request for account {data}")
                else:
                    logger.info(f"MESSAGE_SUCCESS: account={data} - processing completed successfully")

            except Exception:
                logger.exception("MESSAGE_ERROR: Error processing message")
                raise

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

        except Exception:
            logger.exception("Error in consumer:")
            raise

    async def close(self):
        """Close all connections."""
        # Submit any remaining confirmations before shutdown
        if self.rabbitmq_connection:
            await self.rabbitmq_connection.close()
            logger.info("Closed RabbitMQ connection")

        if self.db_pool:
            await close_db_pool()
            logger.info("Closed database pool")


async def main():
    """Main entry point for the consumer."""
    logger.info("🔍 UNPIN_DEBUG: Starting unpin_request_consumer main()")
    consumer = UnpinRequestConsumer()

    try:
        # Connect to services
        logger.info("🔍 UNPIN_DEBUG: Consumer connecting to services")
        await consumer.connect()

        # Start consuming
        logger.info("🔍 UNPIN_DEBUG: Consumer starting to consume messages")
        await consumer.start_consuming()

    except KeyboardInterrupt:
        logger.info("Received interrupt signal, shutting down...")
        logger.info("🔍 UNPIN_DEBUG: Consumer received interrupt signal")
    except Exception as e:
        logger.error(f"Consumer error: {e}")
        logger.info(f"🔍 UNPIN_DEBUG: Consumer exception: {e}")
    finally:
        logger.info("🔍 UNPIN_DEBUG: Consumer closing connections")
        await consumer.close()
        logger.info("🔍 UNPIN_DEBUG: unpin_request_consumer main() completed")


if __name__ == "__main__":
    asyncio.run(main())
