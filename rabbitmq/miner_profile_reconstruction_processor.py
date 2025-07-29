#!/usr/bin/env python3
"""
Processor that fetches miner profiles from the database and queues them for reconstruction.
This processor reads miner profiles that have been parsed and stored in the database,
and sends them to a queue for reconstruction and publishing to IPFS.
"""

import asyncio
import json
import logging
import os
from typing import Any

import aio_pika
import asyncpg
from aio_pika import Message
from substrateinterface import SubstrateInterface

# Setup logging

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


class MinerProfileReconstructionProcessor:
    def __init__(self):
        self.rabbitmq_url = os.getenv("RABBITMQ_URL", "amqp://admin:admin@localhost:5672/")
        self.database_url = os.getenv(
            "DATABASE_URL",
            "postgresql://user:password@localhost:5432/substrate_fetcher",
        )
        self.node_url = os.getenv("NODE_URL", "wss://rpc.hippius.network")
        self.queue_name = "miner_profile_reconstruction"
        self.rabbitmq_connection = None
        self.rabbitmq_channel = None
        self.db_pool = None
        self.current_block = None

    async def connect_database(self):
        """Connect to PostgreSQL database"""
        try:
            self.db_pool = await asyncpg.create_pool(self.database_url, min_size=1, max_size=3)
            logger.info("Connected to database")
        except Exception as e:
            logger.error(f"Failed to connect to database: {e}")
            raise

    async def connect_rabbitmq(self):
        """Connect to RabbitMQ and declare queue"""
        try:
            self.rabbitmq_connection = await aio_pika.connect_robust(self.rabbitmq_url)
            self.rabbitmq_channel = await self.rabbitmq_connection.channel()

            # Declare queue
            await self.rabbitmq_channel.declare_queue(self.queue_name, durable=True)

            logger.info(f"Connected to RabbitMQ and declared queue '{self.queue_name}'")
        except Exception as e:
            logger.error(f"Failed to connect to RabbitMQ: {e}")
            raise

    async def fetch_current_block(self):
        """Fetch the current block number from the substrate chain"""
        try:
            substrate = SubstrateInterface(url=self.node_url)
            block_hash = substrate.get_chain_head()
            block_number = substrate.get_block_number(block_hash)
            self.current_block = block_number
            logger.info(f"Current block number: {self.current_block}")
            substrate.close()
        except Exception as e:
            logger.error(f"Failed to fetch current block: {e}")
            # Use a default block number if we can't fetch it
            self.current_block = 0

    async def fetch_miner_profiles_to_reconstruct(self) -> list[dict[str, Any]]:
        """Fetch miner profiles that need to be reconstructed from pending_miner_profile table"""
        async with self.db_pool.acquire() as conn:
            # Only get miners explicitly flagged for reconstruction
            miners_rows = await conn.fetch(
                """
                SELECT DISTINCT node_id
                FROM pending_miner_profile
                WHERE node_id IS NOT NULL
                ORDER BY node_id
            """
            )

            return [dict(row) for row in miners_rows]

    async def fetch_miner_profile_files(self, node_id: str) -> list[dict[str, Any]]:
        """Fetch all files assigned to a specific miner and convert to proper format"""
        async with self.db_pool.acquire() as conn:
            rows = await conn.fetch(
                """
                SELECT DISTINCT
                    f.cid,
                    f.name,
                    f.size,
                    f.created_at
                FROM files f
                JOIN file_assignments fa ON f.cid = fa.cid
                WHERE $1 IN (fa.miner1, fa.miner2, fa.miner3, fa.miner4, fa.miner5)
                ORDER BY f.created_at ASC
            """,
                node_id,
            )

            # Convert to proper format and handle datetime serialization
            files = []
            for row in rows:
                file_data = {
                    "cid": row["cid"],
                    "name": row["name"],
                    "size": row["size"],
                }
                # Convert datetime to string if present
                if row["created_at"]:
                    file_data["created_at"] = row["created_at"].isoformat()

                files.append(file_data)

            return files

    async def send_to_queue(self, profile_data: dict[str, Any]) -> None:
        """Send profile data to RabbitMQ queue"""
        message_body = json.dumps(profile_data)
        message = Message(body=message_body.encode(), delivery_mode=2)  # Make message persistent

        await self.rabbitmq_channel.default_exchange.publish(message, routing_key=self.queue_name)

        logger.debug(f"Sent profile to queue: {profile_data['node_id']} -> {profile_data['cid']}")

    async def process_single_profile_parallel(self, profile: dict[str, Any]) -> dict[str, str]:
        """Process a single profile in parallel"""
        try:
            node_id = profile["node_id"]
            logger.debug(f"Processing miner profile for {node_id}")

            # Fetch files for this miner
            files = await self.fetch_miner_profile_files(node_id)
            logger.debug(f"Fetched {len(files)} files for miner {node_id}")

            # Calculate file count and total size
            file_count = len(files)
            total_size = sum(file_data.get("size", 0) for file_data in files)

            # Skip miners with no files
            if file_count == 0:
                logger.info(f"Skipping miner {node_id} - no files assigned")
                return {"status": "skipped", "node_id": node_id, "reason": "no_files"}

            # Generate a synthetic CID for the profile (we'll use the node_id as base)
            profile_cid = f"profile_{node_id}"

            # Prepare message data
            message_data = {
                "cid": profile_cid,
                "node_id": node_id,
                "file_count": file_count,
                "files": files,
                "total_size": total_size,
                "block_number": self.current_block,
            }

            # Send to queue
            await self.send_to_queue(message_data)

            logger.info(f"✅ Queued profile for miner {node_id}: {file_count} files, {total_size} bytes")
            return {
                "status": "success",
                "node_id": node_id,
                "file_count": file_count,
                "total_size": total_size,
            }

        except Exception as e:
            logger.exception(f"❌ Error processing profile for miner {profile['node_id']}")
            return {"status": "failed", "node_id": profile["node_id"], "error": str(e)}

    async def process_profiles(self):
        """Main processing loop - now with parallel processing"""
        profiles = await self.fetch_miner_profiles_to_reconstruct()

        if not profiles:
            logger.info("No miner profiles to reconstruct")
            return

        logger.info(f"Found {len(profiles)} miner profiles to reconstruct")

        # Process profiles in parallel with concurrency limit
        semaphore = asyncio.Semaphore(20)  # Limit concurrent profile processing

        async def process_with_semaphore(profile):
            async with semaphore:
                return await self.process_single_profile_parallel(profile)

        # Execute all profile processing in parallel
        logger.info("🚀 Processing profiles in parallel...")
        results = await asyncio.gather(*[process_with_semaphore(profile) for profile in profiles])

        # Count results
        successful_profiles = sum(1 for r in results if r["status"] == "success")
        failed_profiles = sum(1 for r in results if r["status"] == "failed")
        skipped_profiles = sum(1 for r in results if r["status"] == "skipped")

        # Enhanced summary logging
        logger.info("📊 Profile reconstruction summary:")
        logger.info(f"   ✅ Successfully queued: {successful_profiles}")
        logger.info(f"   ❌ Failed: {failed_profiles}")
        logger.info(f"   ⏭️ Skipped (no files): {skipped_profiles}")
        logger.info(f"   📋 Total processed: {len(profiles)}")

        # Log warning if no profiles were successfully queued
        if successful_profiles == 0:
            if failed_profiles > 0:
                logger.error(f"🚨 CRITICAL: All {failed_profiles} profile(s) failed to process!")
                logger.error("   This indicates a systematic issue (database, query, or data problems)")
            elif skipped_profiles > 0:
                logger.warning(f"⚠️ All {skipped_profiles} miner(s) have no assigned files")
                logger.warning("   This may indicate file assignment issues")
            else:
                logger.warning("⚠️ No miner profiles found to process")

        logger.info(f"Successfully queued {successful_profiles} profiles for reconstruction")

        # Return success count for main function to check
        return successful_profiles

    async def close(self):
        """Close connections"""
        if self.rabbitmq_connection:
            await self.rabbitmq_connection.close()
            logger.info("Closed RabbitMQ connection")

        if self.db_pool:
            await self.db_pool.close()
            logger.info("Closed database connection")


async def main():
    processor = MinerProfileReconstructionProcessor()

    try:
        logger.info("🚀 Starting Miner Profile Reconstruction Processor")

        # Connect to services
        logger.info("🔌 Connecting to database and RabbitMQ...")
        await processor.connect_database()
        await processor.connect_rabbitmq()

        # Fetch current block number
        logger.info("📡 Fetching current block number...")
        await processor.fetch_current_block()

        # Process profiles
        logger.info("⛏️ Processing miner profiles...")
        successful_count = await processor.process_profiles()

        # Check if processing was successful
        if successful_count == 0:
            logger.error("🚨 PROCESSOR FAILED: No profiles were successfully queued!")
            logger.error("   This indicates a systematic issue that needs investigation")
            raise RuntimeError("Miner profile reconstruction processor completed but queued 0 profiles")

        logger.info(f"✅ Processor completed successfully - queued {successful_count} profiles")

    except Exception as e:
        logger.error(f"❌ Error in processor: {e}")
        logger.exception("Full processor error traceback:")
        raise
    finally:
        logger.info("🔌 Closing connections...")
        await processor.close()
        logger.info("✅ Processor shutdown complete")


if __name__ == "__main__":
    asyncio.run(main())
