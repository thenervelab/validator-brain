"""HTTP client utilities."""
import logging

import httpx

# Configure logging
logger = logging.getLogger(__name__)

_http_client = None


def get_http_client():
    """
    Get or create a shared HTTP client for connection pooling.
    
    Returns:
        An httpx.AsyncClient instance with configured timeouts and connection limits
    """
    global _http_client
    if _http_client is None:
        # Create a persistent client with connection pooling
        _http_client = httpx.AsyncClient(timeout=30.0, limits=httpx.Limits(max_connections=200),
            follow_redirects=True)
    return _http_client


async def close_http_client():
    """Close the global HTTP client if it exists."""
    global _http_client
    if _http_client is not None:
        await _http_client.aclose()
        _http_client = None
        logger.info("HTTP client closed")