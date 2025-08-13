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
from typing import Any

from substrate_fetcher.registration import compute_deregistration_report

# Add parent directory to path for imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import aio_pika
from dotenv import load_dotenv
from substrateinterface import Keypair, SubstrateInterface

from app.db.connection import close_db_pool, get_db_pool, init_db_pool

# Load environment variables
load_dotenv()


def string_to_hex(s: str) -> str:
    """Convert string to hex-encoded string for substrate submission."""
    # return s.encode("utf-8").hex()
    # we don't need to hex them
    return s


# Setup logging

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


def connect_to_node(ws_url):
    """Establish connection to a Substrate node."""
    try:
        logger.info(f"Connecting to {ws_url}...")
        substrate = SubstrateInterface(
            url=ws_url,
            ss58_format=42,
        )
        logger.info(f"Connected to chain: {substrate.chain}")
        logger.info(f"Runtime version: {substrate.runtime_version}")
        return substrate
    except Exception as e:
        logger.error(f"Connection failed to {ws_url}: {str(e)}")
        raise


async def submit_deregistration_report(substrate, keypair, node_ids):
    """Submit deregistration report transaction."""
    logger.info(f"deregistration node_ids={node_ids}")

    call = substrate.compose_call(
        call_module="Registration",
        call_function="submit_deregistration_report",
        call_params={
            "node_ids": node_ids,
        },
    )
    extrinsic = substrate.create_signed_extrinsic(
        call=call,
        keypair=keypair,
    )

    receipt = substrate.submit_extrinsic(
        extrinsic,
        wait_for_inclusion=True,
    )

    if receipt.is_success:
        logger.info(f"Hippius deregistration successful: Hash {receipt.extrinsic_hash}")
    else:
        logger.error(f"Hippius deregistration failed: {receipt.error_message}")

    return receipt.is_success


async def batch_submit(
    substrate,
    keypair,
    node_ids,
    batch_size=150,
):
    """Submit deregistration reports in batches."""
    successful_batches = 0
    failed_batches = 0
    total_batches = (len(node_ids) + batch_size - 1) // batch_size

    for i in range(0, len(node_ids), batch_size):
        batch = node_ids[i : i + batch_size]
        batch_num = (i // batch_size) + 1

        logger.info(f"📦 Processing batch {batch_num}/{total_batches} with {len(batch)} node IDs")

        is_success = await submit_deregistration_report(substrate, keypair, batch)

        if is_success:
            successful_batches += 1
            logger.info(f"✅ Batch {batch_num}/{total_batches} submitted successfully")
        else:
            failed_batches += 1
            logger.error(f"❌ Batch {batch_num}/{total_batches} failed to submit")

    logger.info(
        f"📊 Batch processing complete: {successful_batches} successful, {failed_batches} failed out of {total_batches} total batches"
    )
    return successful_batches, failed_batches


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

    async def find_broken_assignments(self) -> list[dict[str, Any]]:
        """Find files with broken assignments that need healing."""
        async with self.db_pool.acquire() as conn:
            # Find files with empty assignments
            broken_files = await conn.fetch("""
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
            """)

            return [dict(row) for row in broken_files]

    async def get_known_miners(self) -> dict:
        """Get all miner node_ids we're currently tracking."""
        async with self.db_pool.acquire() as conn:
            miners = await conn.fetch("""
                SELECT node_id, owner_account
                FROM registration 
                WHERE node_id IS NOT NULL
            """)
        logger.info(f"Found {len(miners)} known miners")
        return {row["owner_account"]: row["node_id"] for row in miners}

    async def cleanup_deregistered_miners(self, deregistered_miners: list[str]) -> int:
        """Delete deregistered miners and cleanup orphaned records."""
        if not deregistered_miners:
            return 0
        logger.info(f"Cleaning up {len(deregistered_miners)} from the assigned files and database")
        async with self.db_pool.acquire() as conn:
            total_cleaned = 0

            for miner_node_id in deregistered_miners:
                # Clean up orphaned monitoring records first
                await conn.execute("DELETE FROM node_metrics WHERE miner_id = $1", miner_node_id)
                await conn.execute("DELETE FROM file_failures WHERE miner_id = $1", miner_node_id)
                await conn.execute("DELETE FROM miner_availability WHERE miner_id = $1", miner_node_id)

                # Remove miner from file assignments (set miner columns to NULL)
                files_unassigned = await conn.execute(
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

            # Check for deregistered miners and clean them up
            deregistration = await compute_deregistration_report()

            if deregistration.coldkeys:
                primary_node_ids = [miner.id for miner in deregistration.primary_nodes]
                primary_ipfs_ids = [miner.ipfs_peer_id for miner in deregistration.primary_nodes]
                secondary_ipfs_ids = [miner.ipfs_peer_id for miner in deregistration.linked_nodes]
                all_ipfs_ids = set(primary_ipfs_ids + secondary_ipfs_ids)
                cleaned_count = await self.cleanup_deregistered_miners(list(all_ipfs_ids))
                logger.info(f"🧹 Removed {cleaned_count} file assignments")

                # Submit deregistration report to Hippius blockchain
                validator_seed = os.getenv("VALIDATOR_SEED")
                keypair = Keypair.create_from_mnemonic(validator_seed, ss58_format=42)

                # Clean and decode node IDs
                all_decoded_ids = [n.decode() if isinstance(n, bytes) else n for n in primary_node_ids]
                clean_node_ids = [n for n in all_decoded_ids if n.startswith("12D3Koo")]
                ignored_node_ids = [n for n in all_decoded_ids if not n.startswith("12D3Koo")]

                if ignored_node_ids:
                    logger.warning(f"Ignoring {len(ignored_node_ids)} invalid node IDs: {ignored_node_ids}")

                logger.info(f"Valid node IDs to deregister: {len(clean_node_ids)} out of {len(primary_node_ids)} total")

                if clean_node_ids:
                    logger.info(
                        f"Submitting deregistration report to Hippius using account: {keypair.ss58_address} for {len(clean_node_ids)} node ids"
                    )

                    hippius_substrate = connect_to_node(os.getenv("NODE_URL"))
                    # Convert node IDs to hex-encoded strings for substrate submission
                    hex_encoded_node_ids = [string_to_hex(node_id) for node_id in set(clean_node_ids)]
                    successful_batches, failed_batches = await batch_submit(
                        hippius_substrate,
                        keypair,
                        hex_encoded_node_ids,
                    )

                    if successful_batches > 0:
                        logger.info("✅ Hippius deregistration reports submitted successfully")
                    if failed_batches > 0:
                        logger.error("❌ Some Hippius deregistration reports failed to submit")
                else:
                    logger.info("No node ids to de-register. All contenders graced this epoch.")
            else:
                logger.info("✅ All miners are still registered on Bittensor")

            # Close RabbitMQ connection
            if hasattr(self, "rabbitmq_connection"):
                await self.rabbitmq_connection.close()

            return True

        except Exception:
            logger.exception("❌ Error during self-healing processing")
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
