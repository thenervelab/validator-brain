#!/usr/bin/env python3
"""
Network Self-Healing Processor

This processor identifies files with broken assignments (failed miners, empty slots)
and queues them for self-healing via RabbitMQ.
"""

import asyncio
import logging
import os
import sys
from typing import Dict, List, Any

from substrate_fetcher.registration import get_deregistered_coldkeys

# Add parent directory to path for imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import aio_pika
from dotenv import load_dotenv
from substrateinterface import SubstrateInterface

from app.db.connection import init_db_pool, close_db_pool, get_db_pool

# Load environment variables
load_dotenv()

# Setup logging

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


def submit_deregistration_report(substrate, keypair, node_ids):
    """Submit deregistration report transaction."""
    formatted_node_ids = [node_id.encode() for node_id in node_ids]

    call = substrate.compose_call(
        call_module="Registration",
        call_function="submit_deregistration_report",
        call_params={"node_ids": formatted_node_ids},
    )

    extrinsic = substrate.create_signed_extrinsic(call=call, keypair=keypair)

    receipt = substrate.submit_extrinsic(extrinsic, wait_for_inclusion=True)

    if receipt.is_success:
        logger.info(f"Hippius deregistration successful: Hash {receipt.extrinsic_hash}")
    else:
        logger.error(f"Hippius deregistration failed: {receipt.error_message}")

    return receipt


async def grace(node_ids) -> None:
    """
    Grace period check for deregistered node_ids.
    Increments unsuccessful_registration_checks counter and removes nodes
    that have been checked less than 10 times from the processing list.
    """
    if not node_ids:
        return

    db_pool = get_db_pool()
    nodes_to_remove = []

    async with db_pool.acquire() as conn:
        for node_id in list(node_ids):
            # Insert or update the deregistered node record
            result = await conn.fetchrow(
                """
                INSERT INTO deregistered_node_ids (node_id, unsuccessful_registration_checks, updated_at)
                VALUES ($1, 1, CURRENT_TIMESTAMP)
                ON CONFLICT (node_id) 
                DO UPDATE SET 
                    unsuccessful_registration_checks = deregistered_node_ids.unsuccessful_registration_checks + 1,
                    updated_at = CURRENT_TIMESTAMP
                RETURNING unsuccessful_registration_checks
                """,
                node_id,
            )

            check_count = result["unsuccessful_registration_checks"]

            # If less than 10 checks, remove from processing list (grace period)
            if check_count < 3:
                nodes_to_remove.append(node_id)
                logger.info(f"🕐 Gracing node_id {node_id} (check #{check_count}/10)")

    # Remove graced nodes from the processing list
    for node_id in nodes_to_remove:
        node_ids.discard(node_id)

    if nodes_to_remove:
        logger.info(f"🕐 Graced {len(nodes_to_remove)} nodes, {len(node_ids)} remaining for processing")


class NetworkSelfHealingProcessor:
    def __init__(self):
        self.rabbitmq_url = os.getenv("RABBITMQ_URL", "amqp://admin:admin@localhost:5672/")
        self.queue_name = "network_self_healing"
        self.db_pool = None

    async def connect_rabbitmq(self):
        """Connect to RabbitMQ and declare the queue."""
        try:
            self.rabbitmq_connection = await aio_pika.connect_robust(self.rabbitmq_url)
            self.rabbitmq_channel = await self.rabbitmq_connection.channel()

            # Declare the queue
            await self.rabbitmq_channel.declare_queue(self.queue_name, durable=True)

            logger.info(f"Connected to RabbitMQ and declared queue '{self.queue_name}'")
        except Exception as e:
            logger.error(f"Failed to connect to RabbitMQ: {e}")
            raise

    async def find_broken_assignments(self) -> List[Dict[str, Any]]:
        """Find files with broken assignments that need healing."""
        async with self.db_pool.acquire() as conn:
            # Find files with empty assignments
            broken_files = await conn.fetch(
                """
                SELECT 
                    fa.cid,
                    fa.owner, 
                    f.name as filename,
                    f.size as file_size_bytes,
                    fa.miner1, fa.miner2, fa.miner3, fa.miner4, fa.miner5
                FROM file_assignments fa
                JOIN files f ON fa.cid = f.cid
                WHERE fa.miner1 IS NULL OR fa.miner2 IS NULL OR fa.miner3 IS NULL 
                   OR fa.miner4 IS NULL OR fa.miner5 IS NULL
                ORDER BY fa.updated_at ASC
                LIMIT 100
            """
            )

            return [dict(row) for row in broken_files]

    async def get_known_miners(self) -> Dict:
        """Get all miner node_ids we're currently tracking."""
        async with self.db_pool.acquire() as conn:
            miners = await conn.fetch(
                """
                SELECT node_id, owner_account
                FROM registration 
                WHERE node_id IS NOT NULL
            """
            )
        logger.info("Found {} known miners".format(len(miners)))
        return {row["owner_account"]: row["node_id"] for row in miners}

    async def cleanup_deregistered_miners(self, deregistered_miners: List[str]) -> int:
        """Delete deregistered miners and cleanup orphaned records."""
        if not deregistered_miners:
            return 0

        async with self.db_pool.acquire() as conn:
            total_cleaned = 0

            for miner_node_id in deregistered_miners:
                # Clean up orphaned monitoring records first
                await conn.execute("DELETE FROM node_metrics WHERE miner_id = $1", miner_node_id)
                await conn.execute("DELETE FROM file_failures WHERE miner_id = $1", miner_node_id)
                await conn.execute("DELETE FROM miner_availability WHERE miner_id = $1", miner_node_id)

                # Remove miner from file assignments (set miner columns to NULL)
                await conn.execute(
                    """
                    UPDATE file_assignments SET
                        miner1 = CASE WHEN miner1 = $1 THEN NULL ELSE miner1 END,
                        miner2 = CASE WHEN miner2 = $1 THEN NULL ELSE miner2 END,
                        miner3 = CASE WHEN miner3 = $1 THEN NULL ELSE miner3 END,
                        miner4 = CASE WHEN miner4 = $1 THEN NULL ELSE miner4 END,
                        miner5 = CASE WHEN miner5 = $1 THEN NULL ELSE miner5 END
                    WHERE miner1 = $1 OR miner2 = $1 OR miner3 = $1 OR miner4 = $1 OR miner5 = $1
                    """,
                    miner_node_id,
                )

                # Delete from registration table
                result = await conn.execute("DELETE FROM registration WHERE node_id = $1", miner_node_id)
                if result == "DELETE 1":
                    total_cleaned += 1
                    logger.info(f"Cleaned up deregistered miner: {miner_node_id}")

            return total_cleaned

    async def process_self_healing(self):
        """Main processing method."""
        logger.info("🛠️ Starting network self-healing processing")

        try:
            # Initialize database connection
            await init_db_pool()
            self.db_pool = get_db_pool()

            # Connect to RabbitMQ
            await self.connect_rabbitmq()

            bt_client = SubstrateInterface(
                url="wss://entrypoint-finney.opentensor.ai:443",
                ss58_format=42,
            )

            # Connect to registration network (from NODE_URL env var)
            registration_url = os.getenv("NODE_URL", "wss://rpc.hippius.network")
            registration_substrate = SubstrateInterface(
                url=registration_url,
                use_remote_preset=True,
            )

            # Check for deregistered miners and clean them up
            dereged_coldkeys = get_deregistered_coldkeys(bt_client, registration_substrate)

            if dereged_coldkeys:
                dereged_node_ids = set(sum(dereged_coldkeys.values(), []))

                logger.info(f"🚨 Found {len(dereged_coldkeys)=} and {len(dereged_node_ids)=}")
                await grace(dereged_node_ids)
                logger.info(f"{len(dereged_node_ids)=} after grace applied...")

                cleaned_count = await self.cleanup_deregistered_miners(list(dereged_node_ids))
                logger.info(f"🧹 Cleaned up {cleaned_count} deregistered miners")

                # Submit deregistration report to Hippius blockchain
                validator_seed = os.getenv("VALIDATOR_SEED")
                # keypair = Keypair.create_from_seed(validator_seed, ss58_format=42)
                # logger.info(
                #     f"DRYRUN: Submitting deregistration report to Hippius using account: {keypair.ss58_address}"
                # )
                # hippius_substrate = connect_to_node(os.getenv("NODE_URL"))
                # receipt = submit_deregistration_report(
                #     hippius_substrate, keypair, dereged_node_ids
                # )
                # if receipt and receipt.is_success:
                #     logger.info(
                #         "✅ Hippius deregistration report submitted successfully"
                #     )
                # else:
                #     logger.error("❌ Failed to submit Hippius deregistration report")
            else:
                logger.info("✅ All miners are still registered on Bittensor")

            # Close RabbitMQ connection
            if hasattr(self, "rabbitmq_connection"):
                await self.rabbitmq_connection.close()

            return True

        except Exception as e:
            logger.exception(f"❌ Error during self-healing processing")
            return False
        finally:
            if self.db_pool:
                await close_db_pool()


async def main():
    """Main entry point."""
    processor = NetworkSelfHealingProcessor()
    success = await processor.process_self_healing()

    if success:
        logger.info("✅ Network self-healing processing completed successfully")
        return 0
    else:
        logger.error("❌ Network self-healing processing failed")
        return 1


if __name__ == "__main__":
    exit_code = asyncio.run(main())
    sys.exit(exit_code)
