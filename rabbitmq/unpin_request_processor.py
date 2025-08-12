import asyncio
import json
import logging
import os
from typing import Any

import aio_pika

from app.services.substrate_client import substrate_client
from app.utils.config import NODE_URL

logger = logging.getLogger(__name__)


class UnpinRequestProcessor:
    """Processor for fetching and queuing user unpin requests."""

    def __init__(self):
        """Initialize the processor."""
        self.substrate = None
        self.rabbitmq_connection = None
        self.rabbitmq_channel = None
        self.queue_name = "unpin_request"

    def connect_substrate(self):
        """Connect to the substrate chain."""
        logger.info(f"Connecting to substrate at {NODE_URL}")
        self.substrate = substrate_client

    async def connect_rabbitmq(self):
        """Connect to RabbitMQ and declare the queue."""
        rabbitmq_url = os.getenv("RABBITMQ_URL", "amqp://admin:admin@localhost:5672/")

        self.rabbitmq_connection = await aio_pika.connect_robust(rabbitmq_url)
        self.rabbitmq_channel = await self.rabbitmq_connection.channel()

        # Declare the queue
        await self.rabbitmq_channel.declare_queue(self.queue_name, durable=True)

        logger.info(f"Connected to RabbitMQ and declared queue '{self.queue_name}'")

    async def fetch_and_queue_requests(self):
        """
        Fetch unprocessed unpin requests from user_unpin_requests table and queue them.
        """
        unpin_requests = await self.substrate.query_storage_map(
            module="IpfsPallet",
            function="UserUnpinRequests",
        )
        logger.info(f"Found {len(unpin_requests)} unpin requests on substrate...")
        batch = unpin_requests[:50]

        if batch:
            await self._publish_requests_parallel(batch)

        logger.info(f"Successfully processed {len(batch)} unpin requests")

    async def _publish_requests_parallel(self, requests: list[dict[str, Any]]) -> None:
        semaphore = asyncio.Semaphore(50)

        async def _publish_single_request(request: dict[str, Any]) -> None:
            async with semaphore:
                message_body = json.dumps(request).encode()
                await self.rabbitmq_channel.default_exchange.publish(
                    aio_pika.Message(
                        body=message_body,
                        delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
                    ),
                    routing_key=self.queue_name,
                )

        tasks = [
            _publish_single_request(
                request,
            )
            for request in requests
        ]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        successful = sum(1 for r in results if not isinstance(r, Exception))
        failed = len(results) - successful

        if failed > 0:
            logger.warning(f"Parallel publishing: {successful} succeeded, {failed} failed")
        else:
            logger.info(f"Parallel publishing: {successful}/{len(requests)} requests published successfully")

    async def close(self):
        """Close all connections."""
        if self.rabbitmq_connection:
            await self.rabbitmq_connection.close()
            logger.info("Closed RabbitMQ connection")

        if self.substrate:
            await self.substrate.disconnect()
            logger.info("Closed substrate connection")


async def main():
    """Main entry point."""
    processor = UnpinRequestProcessor()

    try:
        # Connect to substrate and RabbitMQ
        processor.connect_substrate()  # This is synchronous, not async
        await processor.connect_rabbitmq()

        # Fetch and queue requests from database
        await processor.fetch_and_queue_requests()

    except Exception as e:
        logger.error(f"Error in processor: {e}")
        raise
    finally:
        await processor.close()


if __name__ == "__main__":
    asyncio.run(main())
