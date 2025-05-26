"""Blockchain submission with transaction management.

This module provides utilities for submitting data to the blockchain with
transaction management, retry logic, and rollback handling.
"""

from typing import List, Optional

from pydantic import BaseModel

from app.utils.logging import logger
from substrate_fetcher.transaction_manager import batch_submit


class SubmissionResult(BaseModel):
    """Result of blockchain submission."""

    successful_submissions: int = 0
    failed_submissions: int = 0
    total_submissions: int = 0
    epoch: Optional[int] = None
    submission_block: Optional[int] = None
    transaction_hashes: List[str] = []


async def submit_pending_data_to_blockchain(
    db_pool, current_block: int, current_epoch: int,
) -> SubmissionResult:
    """
    Submit pending data to blockchain with transaction management.

    Args:
        db_pool: Database connection pool
        current_block: Current block number
        current_epoch: Current epoch number

    Returns:
        SubmissionResult object with submission status
    """
    result = SubmissionResult(epoch=current_epoch, submission_block=current_block)

    async with db_pool.acquire() as conn:
        async with conn.transaction():
            # Check if we've already submitted in this epoch
            last_submission_epoch = await conn.fetchval(
                """
                SELECT last_submission_epoch FROM epoch_tracking
                WHERE id = 1
                """,
            )

            # If we've already submitted in this epoch, skip
            if last_submission_epoch == current_epoch:
                logger.info(f"Already submitted data for epoch {current_epoch}, skipping")
                return result

            # Check for large number of pending submissions
            pending_count = await conn.fetchval(
                """
                SELECT COUNT(*) FROM pending_submissions
                WHERE submitted = false
                """,
            )

            # Use bulk processing for large datasets
            if pending_count > 1000:
                logger.info(f"Using bulk processing for {pending_count} pending submissions")

                # Define function to fetch submissions in chunks
                async def fetch_submissions_chunk(offset, limit):
                    chunk = await conn.fetch(
                        """
                        SELECT id, submission_id, node_id, owner_id, submission_type, data
                        FROM pending_submissions
                        WHERE submitted = false
                        ORDER BY id
                        LIMIT $1 OFFSET $2
                        """,
                        limit,
                        offset,
                    )
                    return [dict(submission) for submission in chunk]

                # Fetch and process submissions in chunks
                submissions = []
                for offset in range(0, pending_count, 500):
                    chunk = await fetch_submissions_chunk(offset, 500)
                    submissions.extend(chunk)
                    logger.info(f"Fetched {len(submissions)}/{pending_count} pending submissions")
            else:
                # Fetch all submissions at once for smaller datasets
                pending_submissions = await conn.fetch(
                    """
                    SELECT id, submission_id, node_id, owner_id, submission_type, data
                    FROM pending_submissions
                    WHERE submitted = false
                    """,
                )

                # Convert to list of dictionaries
                submissions = [dict(submission) for submission in pending_submissions]

            result.total_submissions = len(submissions)

            if not submissions:
                logger.info(f"No pending submissions for epoch {current_epoch}")

                # Update last submission epoch even if no submissions
                await conn.execute(
                    """
                    INSERT INTO epoch_tracking (id, last_submission_epoch, updated_at)
                    VALUES (1, $1, NOW())
                    ON CONFLICT (id) DO UPDATE
                    SET last_submission_epoch = $1, updated_at = NOW()
                    """,
                    current_epoch,
                )

                return result

            logger.info(f"Found {len(submissions)} pending submissions for epoch {current_epoch}")

            # Define submission function with transaction safety
            async def submit_to_blockchain_with_transaction(submission):
                """Submit to blockchain with database transaction safety."""
                submission_id = submission["id"]
                submission_type = submission["submission_type"]

                # Start a nested transaction for this submission
                async with conn.transaction():
                    try:
                        # Step 1: Mark as being processed (prevents double-processing)
                        await conn.execute(
                            """
                            UPDATE pending_submissions 
                            SET processing = true, processing_started_at = NOW()
                            WHERE id = $1 AND submitted = false AND processing = false
                            """,
                            submission_id,
                        )

                        # Check if we actually updated a row (prevents race conditions)
                        updated_count = await conn.fetchval(
                            "SELECT COUNT(*) FROM pending_submissions WHERE id = $1 AND processing = true",
                            submission_id,
                        )

                        if updated_count == 0:
                            logger.warning(
                                f"Submission {submission_id} already being processed or submitted",
                            )
                            return None

                        # Step 2: Submit to blockchain (this is where real submission would happen)
                        blockchain_tx_hash = await submit_to_blockchain_real(submission)

                        if not blockchain_tx_hash:
                            # Blockchain submission failed, rollback database changes
                            await conn.execute(
                                """
                                UPDATE pending_submissions 
                                SET processing = false, processing_started_at = NULL,
                                    error_message = 'Blockchain submission failed'
                                WHERE id = $1
                                """,
                                submission_id,
                            )
                            logger.error(f"Blockchain submission failed for {submission_id}")
                            return None

                        # Step 3: Mark as successfully submitted in database
                        await conn.execute(
                            """
                            UPDATE pending_submissions 
                            SET submitted = true, transaction_hash = $2, submitted_at = NOW(),
                                processing = false
                            WHERE id = $1
                            """,
                            submission_id,
                            blockchain_tx_hash,
                        )

                        logger.info(
                            f"Successfully submitted {submission_type} {submission_id} with tx hash: {blockchain_tx_hash}",
                        )
                        return blockchain_tx_hash

                    except Exception as e:
                        # Any error causes the entire transaction to rollback
                        logger.error(f"Transaction failed for submission {submission_id}: {e}")
                        # The transaction rollback happens automatically
                        return None

            async def submit_to_blockchain_real(submission):
                """Actual blockchain submission logic - replace with real Substrate calls."""
                submission_type = submission["submission_type"]

                # Different submission logic based on type
                if submission_type == "miner_profile":
                    logger.info(f"Submitting miner profile for {submission['node_id']}")
                    # TODO: Replace with actual substrate call
                    # tx_hash = await substrate_client.submit_miner_profile(data)
                    # For now, simulate success/failure
                    import random

                    if random.random() > 0.1:  # 90% success rate for testing
                        return f"0x{submission['id']:032x}real"
                    return None  # Simulate failure

                if submission_type == "storage_request":
                    logger.info(f"Submitting storage request for {submission['owner_id']}")
                    # TODO: Replace with actual substrate call
                    # tx_hash = await substrate_client.submit_storage_request(data)
                    import random

                    if random.random() > 0.1:  # 90% success rate for testing
                        return f"0x{submission['id']:032x}real"
                    return None  # Simulate failure

                # For unknown types, return None
                logger.warning(f"Unknown submission type: {submission_type}")
                return None

            # Define rollback function
            async def rollback_submission(submission):
                submission_type = submission["submission_type"]
                logger.warning(f"Rolling back {submission_type} submission {submission['id']}")
                # In a real implementation, this would call the appropriate rollback logic
                return True

            # Submit transactions with retry and rollback
            transaction_results = await batch_submit(
                submissions, submit_to_blockchain, max_batch_size=5, max_retries=3,
            )

            # Process results
            successful = 0
            failed = 0
            transaction_hashes = []

            # Process results and update submission status
            # Use bulk update for large datasets
            if len(submissions) > 1000:
                # Create lists of successful and failed submissions
                successful_submissions = []

                for submission, tx_result in zip(submissions, transaction_results):
                    if tx_result.success:
                        successful_submissions.append(submission)
                        successful += 1
                        if tx_result.transaction_hash:
                            transaction_hashes.append(tx_result.transaction_hash)
                    else:
                        failed += 1
                        logger.error(
                            f"Failed to submit {submission['submission_type']} "
                            f"(ID: {submission['id']}): {tx_result.error_message}",
                        )

                # Use bulk update for successful submissions
                if successful_submissions:
                    logger.info(
                        f"Updating {len(successful_submissions)} successful submissions with bulk update",
                    )

                    # Use a temporary table for more efficient updates with many records
                    await conn.execute(
                        """
                        CREATE TEMP TABLE successful_submissions (
                            id INTEGER PRIMARY KEY
                        )
                        """,
                    )

                    # Insert submission IDs in batches
                    for i in range(0, len(successful_submissions), 500):
                        batch = successful_submissions[i : i + 500]
                        values = ", ".join([f"({s['id']})" for s in batch])
                        await conn.execute(
                            f"INSERT INTO successful_submissions (id) VALUES {values}",
                        )

                    # Update all submissions in one query
                    await conn.execute(
                        """
                        UPDATE pending_submissions
                        SET submitted = true,
                            submission_block = $1,
                            updated_at = NOW()
                        FROM successful_submissions
                        WHERE pending_submissions.id = successful_submissions.id
                        """,
                        current_block,
                    )

                    # Drop the temporary table
                    await conn.execute("DROP TABLE successful_submissions")
            else:
                # For smaller datasets, update one by one
                for submission, tx_result in zip(submissions, transaction_results):
                    if tx_result.success:
                        # Update submission status in DB
                        await conn.execute(
                            """
                            UPDATE pending_submissions
                            SET submitted = true, 
                                submission_block = $2,
                                updated_at = NOW()
                            WHERE id = $1
                            """,
                            submission["id"],
                            current_block,
                        )

                        successful += 1
                        if tx_result.transaction_hash:
                            transaction_hashes.append(tx_result.transaction_hash)
                    else:
                        # If transaction failed even with retries
                        failed += 1
                        logger.error(
                            f"Failed to submit {submission['submission_type']} "
                            f"(ID: {submission['id']}): {tx_result.error_message}",
                        )

            # Update result
            result.successful_submissions = successful
            result.failed_submissions = failed
            result.transaction_hashes = transaction_hashes

            # Update last submission epoch
            await conn.execute(
                """
                INSERT INTO epoch_tracking (id, last_submission_epoch, updated_at)
                VALUES (1, $1, NOW())
                ON CONFLICT (id) DO UPDATE
                SET last_submission_epoch = $1, updated_at = NOW()
                """,
                current_epoch,
            )

            logger.info(
                f"Blockchain submission for epoch {current_epoch} completed: "
                f"{successful}/{len(submissions)} successful",
            )

    return result
