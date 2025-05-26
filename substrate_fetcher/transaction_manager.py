"""Transaction management for blockchain submissions.

This module provides utilities for managing blockchain transactions,
including retry logic, rollback handling, and transaction batching.
"""

import asyncio
import time
from typing import Callable, Dict, List, Optional

from pydantic import BaseModel

from app.utils.logging import logger


class TransactionResult(BaseModel):
    """Result of a blockchain transaction."""

    success: bool = False
    transaction_hash: Optional[str] = None
    error_message: Optional[str] = None
    retry_count: int = 0
    total_time_ms: int = 0
    rollback_executed: bool = False


async def with_retry(
    func: Callable,
    *args,
    max_retries: int = 3,
    delay_ms: int = 1000,
    backoff_factor: float = 2.0,
    **kwargs,
) -> TransactionResult:
    """
    Execute a function with retry logic.

    Args:
        func: Function to execute
        *args: Positional arguments for the function
        max_retries: Maximum number of retries
        delay_ms: Initial delay between retries in milliseconds
        backoff_factor: Factor to increase delay between retries
        **kwargs: Keyword arguments for the function

    Returns:
        TransactionResult object with execution status
    """
    result = TransactionResult()
    start_time = time.time()

    for attempt in range(max_retries + 1):
        try:
            # Execute the function
            transaction_hash = await func(*args, **kwargs)

            # If successful, update result and return
            result.success = True
            result.transaction_hash = transaction_hash
            result.retry_count = attempt
            result.total_time_ms = int((time.time() - start_time) * 1000)

            if attempt > 0:
                logger.info(f"Transaction succeeded after {attempt} retries")

            return result

        except Exception as e:
            error_message = str(e)
            logger.warning(f"Transaction attempt {attempt + 1} failed: {error_message}")

            # If this was the last attempt, update result and return
            if attempt == max_retries:
                result.success = False
                result.error_message = error_message
                result.retry_count = attempt
                result.total_time_ms = int((time.time() - start_time) * 1000)
                return result

            # Calculate delay with exponential backoff
            delay = delay_ms * (backoff_factor**attempt)
            await asyncio.sleep(delay / 1000)  # Convert to seconds

    # This should never be reached but explicit return for linting
    return result


async def submit_with_rollback(
    submit_func: Callable, rollback_func: Callable, *args, max_retries: int = 3, **kwargs,
) -> TransactionResult:
    """
    Submit a transaction with rollback capability.

    Args:
        submit_func: Function to submit the transaction
        rollback_func: Function to roll back the transaction if it fails
        *args: Positional arguments for the submit function
        max_retries: Maximum number of retries
        **kwargs: Keyword arguments for the submit function

    Returns:
        TransactionResult object with execution status
    """
    # Try to submit the transaction with retry
    result = await with_retry(submit_func, *args, max_retries=max_retries, **kwargs)

    # If transaction failed, execute rollback
    if not result.success:
        try:
            await rollback_func(*args, **kwargs)
            result.rollback_executed = True
            logger.info("Rollback executed successfully")
        except Exception as e:
            logger.error(f"Rollback failed: {str(e)}")

    return result


async def batch_submit(
    transactions: List[Dict], submit_func: Callable, max_batch_size: int = 10, **kwargs,
) -> List[TransactionResult]:
    """
    Submit transactions in batches.

    Args:
        transactions: List of transactions to submit
        submit_func: Function to submit each transaction
        max_batch_size: Maximum number of transactions per batch
        **kwargs: Additional keyword arguments for the submit function

    Returns:
        List of TransactionResult objects for each transaction
    """
    results = []

    # Process transactions in batches
    for i in range(0, len(transactions), max_batch_size):
        batch = transactions[i : i + max_batch_size]

        # Submit transactions in batch concurrently
        batch_tasks = []
        for tx in batch:
            task = asyncio.create_task(with_retry(submit_func, tx, **kwargs))
            batch_tasks.append(task)

        # Wait for all transactions in batch to complete
        batch_results = await asyncio.gather(*batch_tasks)
        results.extend(batch_results)

        # Log batch results
        successful = sum(1 for r in batch_results if r.success)
        logger.info(
            f"Batch {i // max_batch_size + 1} completed: {successful}/{len(batch)} successful",
        )

    return results
