#!/usr/bin/env python3
"""
Network Self-Healing Processor

This processor identifies files with broken assignments (failed miners, empty slots)
and queues them for self-healing via RabbitMQ.
"""

import asyncio
import json
import logging
import os
import sys
from datetime import datetime
from typing import Dict, List, Any

# Add parent directory to path for imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import aio_pika
from aio_pika import Message
from dotenv import load_dotenv
from substrateinterface import SubstrateInterface, Keypair

from app.db.connection import init_db_pool, close_db_pool, get_db_pool

# Load environment variables
load_dotenv()

# Setup logging

logger = logging.getLogger(__name__)


def connect_to_node(ws_url):
    """Establish connection to a Substrate node."""
    logger.info(f"Connecting to {ws_url}...")
    substrate = SubstrateInterface(
        url=ws_url,
        ss58_format=42,
    )
    logger.info(f"Connected to chain: {substrate.chain}")
    logger.info(f"Runtime version: {substrate.runtime_version}")
    return substrate


def query_storage_double_map(substrate, module, storage_function, netuid):
    """Query all entries in a storage double map for a specific netuid."""
    result = substrate.query_map(module=module, storage_function=storage_function, params=[netuid])
    # Format as { hotkey: uid }
    return {entry[0].value: entry[1].value for entry in result}


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

    async def find_deregistered_miners(self) -> List[str]:
        """Find miners that are no longer registered on Bittensor."""
        substrate = connect_to_node("wss://entrypoint-finney.opentensor.ai:443")
        current_uids = query_storage_double_map(substrate, "SubtensorModule", "Uids", 75)

        logger.info("Found {} miners registered on BTS".format(len(current_uids)))

        known_miners = await self.get_known_miners()
        deregistered_miners = []

        for owner_account, miner_node_id in known_miners.items():
            if miner_node_id not in current_uids:
                deregistered_miners.append(miner_node_id)

        return deregistered_miners

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

                # Delete from registration table (triggers CASCADE deletion and SET NULL on file_assignments)
                result = await conn.execute("DELETE FROM registration WHERE node_id = $1", miner_node_id)
                if result == "DELETE 1":
                    total_cleaned += 1
                    logger.info(f"Cleaned up deregistered miner: {miner_node_id}")

            return total_cleaned

    async def queue_healing_task(self, file_data: Dict[str, Any]):
        """Queue a self-healing task for a broken file."""
        task_data = {
            "type": "file_healing",
            "cid": file_data["cid"],
            "owner": file_data["owner"],
            "filename": file_data["filename"],
            "file_size_bytes": file_data["file_size_bytes"],
            "current_miners": [
                file_data["miner1"],
                file_data["miner2"],
                file_data["miner3"],
                file_data["miner4"],
                file_data["miner5"],
            ],
            "timestamp": datetime.now().isoformat(),
        }

        message_body = json.dumps(task_data).encode()

        await self.rabbitmq_channel.default_exchange.publish(
            Message(body=message_body, delivery_mode=aio_pika.DeliveryMode.PERSISTENT),
            routing_key=self.queue_name,
        )

        logger.debug(f"Queued healing task for file {file_data['cid'][:16]}...")

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
            deregistered_miners = await self.find_deregistered_miners()
            if deregistered_miners:
                logger.info(f"🚨 Found {len(deregistered_miners)} deregistered miners")

                # will add back once confirmed fixed
                # cleaned_count = await self.cleanup_deregistered_miners(
                #     deregistered_miners
                # )
                # logger.info(f"🧹 Cleaned up {cleaned_count} deregistered miners")

                # Submit deregistration report to Hippius blockchain
                validator_seed = os.getenv("VALIDATOR_SEED")
                keypair = Keypair.create_from_mnemonic(validator_seed, ss58_format=42)
                logger.info(
                    f"DRYRUN: Submitting deregistration report to Hippius using account: {keypair.ss58_address}"
                )
                # hippius_substrate = connect_to_node(os.getenv("NODE_URL"))
                # receipt = submit_deregistration_report(
                #     hippius_substrate, keypair, deregistered_miners
                # )
                # if receipt and receipt.is_success:
                #     logger.info(
                #         "✅ Hippius deregistration report submitted successfully"
                #     )
                # else:
                #     logger.error("❌ Failed to submit Hippius deregistration report")
            else:
                logger.info("✅ All miners are still registered on Bittensor")

            # Find broken assignments (now includes files from deregistered miners)
            broken_files = await self.find_broken_assignments()

            if not broken_files:
                logger.info("✅ No broken assignments found - network is healthy")
                return True

            logger.info(f"🔍 Found {len(broken_files)} files with broken assignments")

            # Queue healing tasks
            healed_count = 0
            for file_data in broken_files:
                try:
                    await self.queue_healing_task(file_data)
                    healed_count += 1
                except Exception as e:
                    logger.error(f"❌ Failed to queue healing task for {file_data['cid']}: {e}")

            logger.info(f"✅ Queued {healed_count} self-healing tasks")

            # Close RabbitMQ connection
            if hasattr(self, "rabbitmq_connection"):
                await self.rabbitmq_connection.close()

            return True

        except Exception as e:
            logger.error(f"❌ Error during self-healing processing: {e}")
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
