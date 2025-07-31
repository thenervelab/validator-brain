import asyncio
import json
import logging
import os
from typing import Any

import aio_pika
from substrateinterface import SubstrateInterface

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
        self.substrate = SubstrateInterface(url=NODE_URL)
        logger.info("Connected to substrate")

    async def connect_rabbitmq(self):
        """Connect to RabbitMQ and declare the queue."""
        rabbitmq_url = os.getenv("RABBITMQ_URL", "amqp://admin:admin@localhost:5672/")

        self.rabbitmq_connection = await aio_pika.connect_robust(rabbitmq_url)
        self.rabbitmq_channel = await self.rabbitmq_connection.channel()

        # Declare the queue
        await self.rabbitmq_channel.declare_queue(self.queue_name, durable=True)

        logger.info(f"Connected to RabbitMQ and declared queue '{self.queue_name}'")

    def parse_unpin_request_data(self, storage_data: list[tuple[Any, Any]]) -> list[dict[str, Any]]:
        """
        Parse the raw unpin data from substrate into structured format.

        Args:
            storage_data: Raw data from substrate storage query

        Returns:
            List of parsed unpin request dictionaries
        """
        parsed_requests = []

        for item in storage_data:
            # Extract key and value
            key_data = item[0]
            value_data = item[1]
            owner = str(key_data[0])

            request = {
                "owner": owner,
                "file_hash": value_data["fileHash"],
                "selected_validator": str(value_data["selectedValidator"]),
                "timestamp": asyncio.get_event_loop().time(),
            }

            parsed_requests.append(request)
            logger.debug(f"Parsed unpin request: {owner} -> {request['file_hash'][:20]}...")

        return parsed_requests

    async def fetch_and_queue_requests(self):
        """
        Fetch unprocessed unpin requests from user_unpin_requests table and queue them.
        """
        logger.info("🔍 UNPIN_DEBUG: Starting fetch_and_queue_requests from user_unpin_requests table")

        # Connect to database

        try:
            raw_unpin_requests = self.substrate.query_map(
                module="IpfsPallet",
                storage_function="UserUnpinRequests",
            )
            unpin_rows = self.parse_unpin_request_data(raw_unpin_requests)
            logger.info(f"Parsed {len(unpin_rows)} unpin requests from substrate...")
            parsed_requests = []
            request_ids = []

            for row in unpin_rows:
                try:
                    request = {
                        "id": row["id"],
                        "owner": row["owner"],
                        "file_hash": row["file_hash"],
                        "selected_validator": row["selected_validator"],
                        "epoch": row["epoch"],
                        "timestamp": asyncio.get_event_loop().time(),
                    }
                    parsed_requests.append(request)
                    request_ids.append(row["id"])

                except Exception:
                    logger.exception("Error processing request row")
                    continue

            if parsed_requests:
                logger.info("🔍 UNPIN_DEBUG: Publishing requests to RabbitMQ queue")
                await self._publish_requests_parallel(parsed_requests)
            else:
                logger.info("🔍 UNPIN_DEBUG: No unprocessed unpin requests to publish")

            logger.info(f"Successfully processed {len(parsed_requests)} unpin requests")

        except Exception as e:
            logger.error(f"🔍 UNPIN_DEBUG: Error fetching from database: {e}")
            raise

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
            self.substrate.close()
            logger.info("Closed substrate connection")


async def main():
    """Main entry point."""
    processor = UnpinRequestProcessor()

    try:
        # Connect to RabbitMQ only (no substrate connection needed)
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
