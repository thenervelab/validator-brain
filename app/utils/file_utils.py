"""
File-related utilities, including fetching file sizes from IPFS.
"""

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
    
    # Try the /files/stat endpoint first (works for both files and directories)
    url = f"{IPFS_NODE_URL}/api/v0/files/stat?arg=/ipfs/{file_hash}"
    
    logger.debug(f"Fetching IPFS file size for {file_hash[:16]}... from {url}")

    try:
        response = await ipfs_client.post(
            url,
            headers={"Content-Type": "application/json"},
            timeout=10.0
        )
        response.raise_for_status()

        json_data = response.json()
        size = json_data.get("Size")
        
        logger.debug(f"IPFS /files/stat response for {file_hash[:16]}...: {json_data}")

        if size is None:
            logger.warning(f"No 'Size' field in /files/stat response for {file_hash}: {json_data}")
            # Fall back to block/stat endpoint
            return await _fetch_ipfs_file_size_fallback(file_hash)

        # Validate size is reasonable
        if isinstance(size, (int, float)) and size >= 0:
            file_size = int(size)
            logger.info(f"✅ IPFS file size for {file_hash[:16]}...: {file_size:,} bytes")
            return file_size
        else:
            logger.error(f"Invalid size value for {file_hash}: {size} (type: {type(size)})")
            return 0

    except httpx.TimeoutException:
        logger.error(f"Request timed out for file hash: {file_hash}")
        raise
    except httpx.ConnectError:
        logger.error(f"Connection error for file hash: {file_hash} - Check IPFS node URL: {IPFS_NODE_URL}")
        raise
    except httpx.HTTPStatusError as e:
        logger.error(f"HTTP {e.response.status_code} error for file hash: {file_hash}")
        logger.error(f"Response text: {e.response.text}")
        
        # If /files/stat fails, try fallback method
        if e.response.status_code in [400, 500]:
            logger.info(f"Trying fallback method for {file_hash}")
            return await _fetch_ipfs_file_size_fallback(file_hash)
        raise
    except json.JSONDecodeError as e:
        logger.error(f"Failed to parse JSON response for file hash {file_hash}: {e}")
        logger.error(f"Response text: {response.text}")
        raise
    except Exception as e:
        logger.error(f"An unexpected error occurred while fetching file size for {file_hash}: {e}")
        raise 


async def fetch_ipfs_file_size_with_state(state: "AppState", file_hash: str) -> int:
    """
    Fetches the size of an IPFS file given its hash using AppState configuration.
    
    This version matches the original user's signature and uses the AppState's
    IPFS node URL, HTTP client, and rate limiter.

    Args:
        state: An AppState object containing IPFS node URL, HTTP client, and rate limiter.
        file_hash: The hash of the IPFS file.

    Returns:
        The size of the IPFS file as an integer (u32 equivalent).

    Raises:
        Exception: If there's an error during the request, response processing,
                   or if the file size exceeds the u32 maximum.
    """
    await state.rate_limiter.until_ready()

    url = f"{state.ipfs_node_url}/api/v0/files/stat?arg=/ipfs/{file_hash}"
    
    logger.debug(f"Fetching IPFS file size for {file_hash[:16]}... from {url}")

    try:
        response = await state.ipfs_client.post(
            url,
            headers={"Content-Type": "application/json"},
            timeout=5
        )
        response.raise_for_status()  # Raises HTTPStatusError for bad responses (4xx or 5xx)

    except httpx.TimeoutException:
        raise Exception(f"Request timed out for file hash: {file_hash}")
    except httpx.ConnectError:
        raise Exception(
            f"Connection error for file hash: {file_hash} - Check IPFS node URL"
        )
    except httpx.HTTPStatusError as e:
        # Try fallback method for HTTP errors
        if e.response.status_code in [400, 500]:
            logger.info(f"Trying fallback method for {file_hash} due to HTTP {e.response.status_code}")
            return await _fetch_ipfs_file_size_fallback_with_state(state, file_hash)
        
        raise Exception(
            f"Unexpected status code: {e.response.status_code} for file hash: {file_hash}"
        )
    except httpx.RequestError as e:
        raise Exception(
            f"Failed to send request for file hash: {file_hash} - Error: {e}"
        )

    try:
        body = response.text
        json_data = json.loads(body)
        
        logger.debug(f"IPFS /files/stat response for {file_hash[:16]}...: {json_data}")
        
    except json.JSONDecodeError as e:
        raise Exception(
            f"Failed to parse JSON response for file hash {file_hash}: {e}"
        )
    except Exception as e:
        raise Exception(
            f"Failed to read response body for file hash {file_hash}: {e}"
        )

    size = json_data.get("Size")

    if size is not None:
        if isinstance(size, (int, float)): # IPFS API typically returns integer, but checking for robustness
            # Python integers handle arbitrary size, so we'll check against u32 max explicitly
            U32_MAX = 2**32 - 1
            if size > U32_MAX:
                raise Exception(
                    f"File size {size} exceeds u32 maximum ({U32_MAX}) for file hash: {file_hash}"
                )
            
            file_size = int(size)
            logger.info(f"✅ IPFS file size for {file_hash[:16]}...: {file_size:,} bytes")
            return file_size
        else:
            raise Exception(
                f"Unexpected type for 'Size' field for file hash {file_hash}: {type(size)}"
            )
    else:
        logger.warning(f"No 'Size' field in /files/stat response for {file_hash}, trying fallback")
        return await _fetch_ipfs_file_size_fallback_with_state(state, file_hash)


async def _fetch_ipfs_file_size_fallback_with_state(state: "AppState", file_hash: str) -> int:
    """Fallback method using /block/stat endpoint with AppState."""
    url = f"{state.ipfs_node_url}/api/v0/block/stat?arg={file_hash}"
    logger.info(f"Using fallback /block/stat endpoint for {file_hash}")
    
    try:
        response = await state.ipfs_client.post(
            url,
            headers={"Content-Type": "application/json"},
            timeout=5
        )
        response.raise_for_status()
        
        json_data = response.json()
        size = json_data.get("Size")
        
        logger.debug(f"IPFS /block/stat response for {file_hash[:16]}...: {json_data}")
        
        if size is None:
            raise Exception(f"No 'Size' field in /block/stat response for {file_hash}")
        
        if isinstance(size, (int, float)) and size >= 0:
            U32_MAX = 2**32 - 1
            if size > U32_MAX:
                raise Exception(
                    f"File size {size} exceeds u32 maximum ({U32_MAX}) for file hash: {file_hash}"
                )
            
            file_size = int(size)
            logger.info(f"✅ IPFS block size (fallback) for {file_hash[:16]}...: {file_size:,} bytes")
            return file_size
        else:
            raise Exception(f"Invalid size value in fallback for {file_hash}: {size}")
    
    except Exception as e:
        raise Exception(f"Both /files/stat and /block/stat failed for file hash: {file_hash} - {e}")


async def _fetch_ipfs_file_size_fallback(file_hash: str) -> int:
    """
    Fallback method using /block/stat endpoint.
    
    Args:
        file_hash: The hash of the IPFS file.
        
    Returns:
        The size of the IPFS block in bytes.
    """
    url = f"{IPFS_NODE_URL}/api/v0/block/stat?arg={file_hash}"
    logger.info(f"Using fallback /block/stat endpoint for {file_hash}")
    
    try:
        response = await ipfs_client.post(
            url,
            headers={"Content-Type": "application/json"},
            timeout=10.0
        )
        response.raise_for_status()

        json_data = response.json()
        size = json_data.get("Size")
        
        logger.debug(f"IPFS /block/stat response for {file_hash[:16]}...: {json_data}")

        if size is None:
            logger.error(f"No 'Size' field in /block/stat response for {file_hash}: {json_data}")
            return 0

        if isinstance(size, (int, float)) and size >= 0:
            file_size = int(size)
            logger.info(f"✅ IPFS block size (fallback) for {file_hash[:16]}...: {file_size:,} bytes")
            return file_size
        else:
            logger.error(f"Invalid size value in fallback for {file_hash}: {size}")
            return 0

    except Exception as e:
        logger.error(f"Fallback method also failed for {file_hash}: {e}")
        return 0 