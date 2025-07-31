import asyncio
import json
import logging
import os
from typing import Any

import aio_pika
from substrateinterface import SubstrateInterface

from app.db.connection import close_db_pool, get_db_pool, init_db_pool
from app.utils.blockchain_submission import load_validator_keypair, string_to_bounded_vec
from rabbitmq.pinning_request_consumer import fetch_ipfs_content

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
        return bytes.fromhex(hex_string).decode("utf-8")
    except Exception as e:
        logger.error(f"Error converting hex to string: {e}")
        return hex_string


async def call_update_unpin_and_storage_requests(requests: list[dict[str, Any]]) -> None:
    """Calls the update_unpin_and_storage_requests extrinsic on the Substrate node.

    Args:
        requests (List[Dict[str, Any]]): List of unpin request updates. Each dict should contain:
            - storage_request_owner: str (SS58 address)
            - storage_request_file_hash: str (IPFS CID)
            - file_size: int
            - user_profile_cid: str (IPFS CID)
            - miner_pin_requests: List[Dict[str, Any]] with fields:
                - miner_node_id: str (node ID)
                - cid: str (IPFS CID)
                - files_count: int
                - files_size: int

    Raises:
        Exception: If the extrinsic submission fails
    """
    substrate = None

    try:
        # Initialize Substrate interface
        node_url = os.getenv("NODE_URL", "wss://rpc.hippius.network")
        substrate = SubstrateInterface(url=node_url, use_remote_preset=True)
        logger.info(f"Connected to Substrate node at {node_url}")

        # Check if IpfsPallet exists in metadata
        metadata = substrate.get_metadata()
        if "IpfsPallet" not in [p.name for p in metadata.pallets]:
            raise Exception("IpfsPallet not found in chain metadata!")

        # Load the validator keypair for signing
        keypair = load_validator_keypair()
        if not keypair:
            raise Exception("No validator keypair available for signing")

        # Create keypair from mnemonic
        logger.info(f"Using account {keypair.ss58_address} for signing unpin confirmation")

        # Format the requests to match the StorageUnpinUpdateRequest structure
        formatted_requests = []
        for req in requests:
            formatted_req = {
                "storage_request_owner": req["storage_request_owner"],
                "storage_request_file_hash": string_to_bounded_vec(req["storage_request_file_hash"]),
                "file_size": int(req["file_size"]),
                "user_profile_cid": string_to_bounded_vec(req["user_profile_cid"]),
            }
            formatted_requests.append(formatted_req)

        logger.info(f"Formatted {len(formatted_requests)} unpin request(s)")

        # Compose the call
        call = substrate.compose_call(
            call_module="IpfsPallet",
            call_function="update_unpin_and_storage_requests",
            call_params={"requests": formatted_requests},
        )

        # Create and sign the extrinsic
        extrinsic = substrate.create_signed_extrinsic(call, keypair)
        logger.debug(f"Created extrinsic: {extrinsic}")

        # Submit the extrinsic and wait for finalization
        receipt = substrate.submit_extrinsic(extrinsic, wait_for_inclusion=True, wait_for_finalization=True)

        if receipt.is_success:
            logger.info(f"✅ Unpin confirmation extrinsic successful in block {receipt.block_hash}")
        else:
            raise Exception(f"Unpin confirmation extrinsic failed: {receipt.error_message}")

    finally:
        if substrate:
            substrate.close()


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

    async def _unpin_single_file(self, conn, cid: str, owner: str) -> bool:
        """Helper to unpin a single CID and clean up database entries."""
        logger.info(f"🗑️ UNPIN_FILE: account={account} CID={cid_short} - starting cleanup")

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
            logger.warning(f"🗑️ UNPIN_FILE: account={account} CID={cid_short} - file not found or not owned by user")
            return False

        file_size = file_record["size"] or 0
        assigned_miners = [file_record[f"miner{i}"] for i in range(1, 6) if file_record[f"miner{i}"]]

        logger.info(
            f"🗑️ UNPIN_FILE: account={account} CID={cid_short} - file size={file_size:,} bytes, assigned_miners={len(assigned_miners)}"
        )

        # Clean up database entries
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

            # Update miner statistics for affected miners
            for miner_id in assigned_miners:
                if miner_id:
                    # Decrement file count and size in miner_stats
                    await conn.execute(
                        """
                        UPDATE miner_stats 
                        SET 
                            files_count = GREATEST(0, files_count - 1),
                            total_size = GREATEST(0, total_size - $1),
                            updated_at = NOW()
                        WHERE node_id = $2
                    """,
                        file_size,
                        miner_id,
                    )

                    logger.debug(f"🗑️ UNPIN_FILE: Updated stats for miner {miner_id[:16]}...")

            # Clean up parsed_cids if this was part of a profile
            await conn.execute("DELETE FROM parsed_cids WHERE cid = $1", cid)

        logger.info(f"✅ UNPIN_FILE_COMPLETE: account={account} CID={cid_short} - file unpinned and cleaned up")
        return True

    async def _process_manifest_files_parallel(self, manifest_data: list, owner: str) -> int:
        """Process manifest files for unpinning in parallel."""
        if not manifest_data:
            return 0

        file_cids = []

        for i, file_info in enumerate(manifest_data):
            if isinstance(file_info, dict):
                file_cid = file_info.get("cid")
                if file_cid:
                    file_cids.append(file_cid)
                else:
                    logger.warning(f"Skipping manifest entry {i + 1}: missing 'cid' field")

            elif isinstance(file_info, str):
                file_cids.append(file_info)
            else:
                logger.warning(f"Skipping invalid manifest entry {i + 1}: {file_info}")

        if not file_cids:
            logger.warning("No valid file CIDs found in manifest")
            return 0

        # Process all files in parallel with semaphore
        semaphore = asyncio.Semaphore(20)
        unpinned_count = 0

        async def unpin_with_semaphore(file_cid: str) -> bool:
            async with semaphore:
                async with self.db_pool.acquire() as conn:
                    return await self._unpin_single_file(conn, file_cid, owner)

        logger.info(f"🗑️ Unpinning {len(file_cids)} files from manifest in parallel (max 20 concurrent)")
        tasks = [unpin_with_semaphore(file_cid) for file_cid in file_cids]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        for result in results:
            if isinstance(result, bool) and result:
                unpinned_count += 1
            elif isinstance(result, Exception):
                logger.error(f"Error unpinning file: {result}")

        return unpinned_count

    async def process_unpin_request(self, request_data: dict[str, Any]) -> bool:
        """
        Process an unpin request, handling manifest CIDs from blockchain unpin requests.
        """
        owner = request_data.get("owner")
        file_hash_hex = request_data.get("file_hash", "")
        account = owner[:16]  # for logging purposes
        request_id = f"{owner}_{file_hash_hex}"

        async with self.db_pool.acquire() as conn:
            # Check if this request has already been processed
            existing = await conn.fetchrow("SELECT id FROM processed_unpin_requests WHERE request_id = $1", request_id)
            if existing:
                logger.info(f"🔄 ALREADY_PROCESSED: request_id={request_id}... - skipping")
                return True

        manifest_cid = hex_to_string(file_hash_hex)
        manifest_id = manifest_cid[:20]

        if not manifest_cid:
            logger.error(f"❌ UNPIN_REQUEST_ERROR: account={account} - No file_hash found")
            return False

        files_unpinned = 0

        # ===== UNPIN REQUEST TRACING - STEP 5: PROCESSING UNPIN REQUEST =====
        logger.info(f"🗑️ UNPIN_REQUEST_START: account={account} CID={manifest_id}...")

        # Try to fetch and parse the manifest to handle multi-file unpinning
        logger.info(f"🗑️ UNPIN_REQUEST_FETCH: account={account} CID={manifest_id}... - fetching manifest content")
        manifest_content = await fetch_ipfs_content(manifest_cid)

        if manifest_content:
            # Successfully fetched manifest content
            logger.info(
                f"🗑️ UNPIN_REQUEST_FETCH_SUCCESS: account={account} CID={manifest_id}... - manifest fetched, parsing content"
            )
            try:
                manifest_data = json.loads(manifest_content)

                if isinstance(manifest_data, list):
                    # Valid manifest format - list of files
                    logger.info(
                        f"🗑️ UNPIN_REQUEST_MANIFEST: account={account} CID={manifest_id}... - manifest contains {len(manifest_data)} files"
                    )

                    # Process all files in parallel
                    files_unpinned = await self._process_manifest_files_parallel(manifest_data, owner)
                    logger.info(
                        f"🗑️ UNPIN_REQUEST_FILES: account={account} CID={manifest_id}... - unpinned {files_unpinned} files from manifest"
                    )

                    # Also unpin the manifest itself
                    async with self.db_pool.acquire() as conn:
                        if await self._unpin_single_file(conn, manifest_cid, owner):
                            files_unpinned += 1

                elif isinstance(manifest_data, dict):
                    # Single file object format
                    logger.info(
                        f"🗑️ UNPIN_REQUEST_SINGLE: account={account} CID={manifest_id}... - manifest contains single file object"
                    )
                    file_cid = manifest_data.get("cid")

                    if file_cid:
                        logger.info(
                            f"🗑️ UNPIN_REQUEST_SINGLE: account={account} manifest_CID={manifest_id}... file_CID={file_cid[:20]}..."
                        )
                        async with self.db_pool.acquire() as conn:
                            if await self._unpin_single_file(conn, file_cid, owner):
                                files_unpinned += 1

                    # Also unpin the manifest itself
                    async with self.db_pool.acquire() as conn:
                        if await self._unpin_single_file(conn, manifest_cid, owner):
                            files_unpinned += 1
                else:
                    # Not a JSON object/array, treat as raw file
                    logger.info(
                        f"🗑️ UNPIN_REQUEST_RAW: account={account} CID={manifest_id}... - content is not JSON, treating as raw file"
                    )
                    async with self.db_pool.acquire() as conn:
                        if await self._unpin_single_file(conn, manifest_cid, owner):
                            files_unpinned = 1

            except json.JSONDecodeError:
                # Not a JSON file, treat manifest CID as a single file
                logger.info(
                    f"🗑️ UNPIN_REQUEST_BINARY: account={account} CID={manifest_id}... - not JSON, treating as binary file"
                )
                async with self.db_pool.acquire() as conn:
                    if await self._unpin_single_file(conn, manifest_cid, owner):
                        files_unpinned = 1

        else:
            # Could not fetch manifest content, but still try to unpin based on CID
            logger.warning(
                f"🗑️ UNPIN_REQUEST_FETCH_FAILED: account={account} CID={manifest_id}... - could not fetch manifest, trying direct unpin"
            )
            async with self.db_pool.acquire() as conn:
                if await self._unpin_single_file(conn, manifest_cid, owner):
                    files_unpinned = 1

        # Record that we've processed this unpin request
        async with self.db_pool.acquire() as conn:
            # ===== UNPIN REQUEST TRACING - STEP 6: STORING request_id IN DATABASE =====
            logger.info(f"💾 REQUEST_STORE: Storing unpin request data account={account}")

            # Record that we've processed this unpin request
            await conn.execute(
                """
                INSERT INTO processed_unpin_requests (request_id, owner, file_hash, files_unpinned)
                VALUES ($1, $2, $3, $4) ON CONFLICT DO NOTHING
            """,
                request_id,
                owner,
                file_hash_hex,
                files_unpinned,
            )

        # Collect confirmation data for blockchain submission
        if files_unpinned > 0:
            confirmation_data = {
                "storage_request_owner": owner,
                "storage_request_file_hash": manifest_cid,
                "file_size": 0,
            }
            self.unpin_confirmations.append(confirmation_data)
        logger.info(
            f"✅ UNPIN_REQUEST_COMPLETE: account={account} CID={manifest_id if manifest_cid else 'N/A'}... files_unpinned={files_unpinned}"
        )

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

    async def submit_unpin_confirmations(self):
        """Submit collected unpin confirmations to blockchain."""
        if not self.unpin_confirmations:
            logger.info("No unpin confirmations to submit")
            return

        try:
            logger.info(
                f"🚀 BLOCKCHAIN_SUBMIT: Submitting {len(self.unpin_confirmations)} unpin confirmations to blockchain"
            )

            # Submit confirmations to blockchain
            await call_update_unpin_and_storage_requests(self.unpin_confirmations)

            logger.info(
                f"✅ BLOCKCHAIN_SUBMIT: Successfully submitted {len(self.unpin_confirmations)} unpin confirmations"
            )

            # Clear the confirmations after successful submission
            self.unpin_confirmations = []

        except Exception:
            logger.exception("BLOCKCHAIN_SUBMIT: Failed to submit unpin confirmations:")

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
        await self.submit_unpin_confirmations()

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
