"""
RabbitMQ consumer for processing node registration messages.

This consumer:
1. Listens to the registration_latest queue
2. Processes each message containing node registration data
3. Stores the registration data in the registration table
4. Uses upsert logic to handle duplicates from both storage sources
"""

import asyncio
import json
import logging
import os
import sys
from typing import Any

# Add parent directory to path to import app modules
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import aio_pika
from dotenv import load_dotenv

from app.db.connection import close_db_pool, get_db_pool, init_db_pool

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


class RegistrationConsumer:
    """Consumer for processing node registration messages."""

    def __init__(self):
        """Initialize the consumer."""
        self.rabbitmq_connection = None
        self.rabbitmq_channel = None
        self.queue_name = "registration_latest"
        self.db_pool = None

    async def connect_rabbitmq(self):
        """Connect to RabbitMQ."""
        rabbitmq_url = os.getenv("RABBITMQ_URL", "amqp://admin:admin@localhost:5672/")

        self.rabbitmq_connection = await aio_pika.connect_robust(rabbitmq_url)
        self.rabbitmq_channel = await self.rabbitmq_connection.channel()

        # Set prefetch count to process one message at a time
        await self.rabbitmq_channel.set_qos(prefetch_count=1)

        logger.info("Connected to RabbitMQ")

    async def init_database(self):
        """Initialize database connection pool."""
        await init_db_pool()
        self.db_pool = get_db_pool()
        logger.info("Database connection pool initialized")

    async def store_registration(self, registration: dict[str, Any]) -> bool:
        """
        Store registration data in the database.

        Args:
            registration: Dictionary containing registration data

        Returns:
            True if successful, False otherwise
        """
        # Validate node_id length to prevent "value too long" errors
        node_id = registration.get("node_id", "")
        if len(node_id) > 100:
            logger.warning(f"Node ID too long ({len(node_id)} chars), skipping registration for node {node_id[:50]}...")
            return False

        # Validate other VARCHAR fields
        ipfs_peer_id = registration.get("ipfs_peer_id", "")
        if len(ipfs_peer_id) > 100:
            logger.warning(
                f"IPFS peer ID too long ({len(ipfs_peer_id)} chars), skipping registration for node {node_id}"
            )
            return False

        owner_account = registration.get("owner_account", "")
        if len(owner_account) > 100:
            logger.warning(
                f"Owner account too long ({len(owner_account)} chars), skipping registration for node {node_id}"
            )
            return False

        node_type = registration.get("node_type", "")
        if len(node_type) > 50:
            logger.warning(f"Node type too long ({len(node_type)} chars), skipping registration for node {node_id}")
            return False

        status = registration.get("status", "")
        if len(status) > 20:
            logger.warning(f"Status too long ({len(status)} chars), skipping registration for node {node_id}")
            return False

        try:
            async with self.db_pool.acquire() as conn:
                # Use upsert logic to handle duplicates from both storage sources
                await conn.execute(
                    """
                    INSERT INTO registration (
                        node_id, ipfs_peer_id, node_type, owner_account, 
                        registered_at, status, node_hierarchy, updated_at
                    ) VALUES ($1, $2, $3, $4, $5, $6, $7, NOW())
                    ON CONFLICT (node_id)
                    DO UPDATE SET
                        ipfs_peer_id = EXCLUDED.ipfs_peer_id,
                        node_type = EXCLUDED.node_type,
                        owner_account = EXCLUDED.owner_account,
                        registered_at = EXCLUDED.registered_at,
                        status = EXCLUDED.status,
                        node_hierarchy = EXCLUDED.node_hierarchy,
                        updated_at = NOW()
                """,
                    node_id,
                    ipfs_peer_id,
                    node_type,
                    owner_account,
                    registration["registered_at"],
                    status,
                    registration["node_hierarchy"],
                )

                logger.info(f"Stored registration for node {node_id} (source: {registration.get('source', 'unknown')})")
                return True

        except Exception as e:
            logger.error(f"Error storing registration for node {node_id}: {e}")
            return False

    async def process_message(self, message: aio_pika.IncomingMessage):
        """
        Process a single message from the queue.

        Args:
            message: The message to process
        """
        async with message.process():
            try:
                # Parse the message body
                registration_data = json.loads(message.body.decode())
                logger.info(
                    f"Processing registration for node {registration_data.get('node_id', 'unknown')} from {registration_data.get('source', 'unknown')} source"
                )

                # Store the registration
                success = await self.store_registration(registration_data)

                if not success:
                    # Log error for failed storage but don't requeue
                    logger.error(f"Failed to store registration for node {registration_data}, discarding message")

            except json.JSONDecodeError as e:
                logger.error(f"Invalid JSON in message: {e}")
                # Log error and continue processing
            except Exception as e:
                logger.error(f"Error processing message: {e}")
                # Log error and continue processing

    async def start_consuming(self):
        """Start consuming messages from the queue."""
        # Declare the queue (in case it doesn't exist)
        queue = await self.rabbitmq_channel.declare_queue(self.queue_name, durable=True)

        logger.info(f"Starting to consume from queue '{self.queue_name}'")

        # Start consuming
        await queue.consume(self.process_message)

        # Keep the consumer running
        try:
            await asyncio.Future()
        except asyncio.CancelledError:
            logger.info("Consumer cancelled")

    async def close(self):
        """Close all connections."""
        if self.rabbitmq_connection:
            await self.rabbitmq_connection.close()
            logger.info("Closed RabbitMQ connection")

        if self.db_pool:
            await close_db_pool()
            logger.info("Closed database connection pool")


async def main():
    """Main entry point."""
    consumer = RegistrationConsumer()

    try:
        # Initialize connections
        await consumer.init_database()
        await consumer.connect_rabbitmq()

        # Start consuming
        await consumer.start_consuming()

    except KeyboardInterrupt:
        logger.info("Received interrupt signal")
    except Exception as e:
        logger.error(f"Error in consumer: {e}")
        raise
    finally:
        await consumer.close()


if __name__ == "__main__":
    asyncio.run(main())
