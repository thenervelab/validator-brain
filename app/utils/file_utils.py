"""
File-related utilities, including fetching file sizes from IPFS.
"""

import asyncio
import json
import os
import httpx
from aiolimiter import AsyncLimiter
from app.utils.logging import logger

# Get IPFS node URL from environment, with a default
IPFS_NODE_URL = os.getenv("IPFS_NODE_URL", "http://127.0.0.1:5001")

# Create a shared rate limiter to avoid overwhelming the IPFS node
RATE_LIMIT = int(os.getenv("IPFS_API_RATE_LIMIT", "10"))
rate_limiter = AsyncLimiter(RATE_LIMIT, 1)

# Create a shared, reusable httpx client for performance
ipfs_client = httpx.AsyncClient()


async def fetch_ipfs_file_size(file_hash: str) -> int:
    """
    Fetches the size of an IPFS file given its hash.

    Args:
        file_hash: The hash of the IPFS file.

    Returns:
        The size of the IPFS file in bytes.

    Raises:
        Exception: If there's an error fetching or parsing the file stats.
    """
    await rate_limiter.acquire()
    url = f"{IPFS_NODE_URL}/api/v0/files/stat?arg=/ipfs/{file_hash}"

    try:
        response = await ipfs_client.post(
            url,
            headers={"Content-Type": "application/json"},
            timeout=10.0
        )
        response.raise_for_status()

        json_data = response.json()
        size = json_data.get("Size")

        if size is None:
            raise Exception(f"Failed to extract 'Size' field for file hash: {file_hash}")

        return int(size)

    except httpx.TimeoutException:
        logger.error(f"Request timed out for file hash: {file_hash}")
        raise
    except httpx.ConnectError:
        logger.error(f"Connection error for file hash: {file_hash} - Check IPFS node URL")
        raise
    except httpx.HTTPStatusError as e:
        logger.error(f"Unexpected status code: {e.response.status_code} for file hash: {file_hash}")
        raise
    except json.JSONDecodeError as e:
        logger.error(f"Failed to parse JSON response for file hash {file_hash}: {e}")
        raise
    except Exception as e:
        logger.error(f"An unexpected error occurred while fetching file size for {file_hash}: {e}")
        raise 