"""IPFS API service."""

import json
import os
from typing import Dict, List, Optional

import httpx
from pydantic import BaseModel

from app.utils.config import get_ipfs_node_url, get_ipfs_timeout
from app.utils.logging import logger


class BlockCheckResult(BaseModel):
    ipfs_peer_id: str
    cid: str
    success: bool


class PingCheckResult(BaseModel):
    time_ms: float = None
    success: bool


async def ping_ipfs_node(ipfs_peer_id: str) -> PingCheckResult:
    """
    Ping an IPFS node to test connectivity.

    Args:
        ipfs_peer_id: The IPFS peer ID to ping

    Returns: PingCheckResult
    """

    try:
        ipfs_node_url = get_ipfs_node_url()
        api_url = f"{ipfs_node_url}/api/v0/ping"
        params = {"arg": ipfs_peer_id, "count": "1"}
        timeout = get_ipfs_timeout("ping")

        async with httpx.AsyncClient(timeout=timeout) as client:
            response = await client.post(api_url, params=params)

            if response.status_code == 200:
                response_text = response.text.strip()
                logger.info(f"Ping response for {ipfs_peer_id}: {response_text}")

                for line in response_text.splitlines():
                    if not line.strip():
                        continue

                    try:
                        data = json.loads(line)
                        logger.debug(f"Parsed ping data: {data}")

                        # Check for success in the response
                        if data.get("Success") is True:
                            # Try different possible field names for timing
                            time_ms = (
                                data.get("Time")
                                or data.get("time")
                                or data.get("RTT")
                                or data.get("rtt")
                            )
                            if time_ms is None:
                                # Look for timing in nanoseconds and convert
                                time_ns = data.get("time_ns") or data.get("Time_ns")
                                if time_ns:
                                    time_ms = time_ns / 1_000_000  # Convert ns to ms

                            return PingCheckResult(
                                success=True,
                                time_ms=time_ms or 0.0,
                            )
                    except json.JSONDecodeError as e:
                        logger.debug(f"Failed to parse ping response line: {line}, error: {e}")
                        continue

    except httpx.ReadTimeout:
        logger.warning(f"Timeout pinging {ipfs_peer_id}")
    except Exception as e:
        logger.error(f"Error pinging {ipfs_peer_id}: {e}")

    return PingCheckResult(success=False)


async def check_cid_is_provided(cid: str, ipfs_peer_id: str) -> BlockCheckResult:
    """
    Check if a peer is a provider for this CID using routing findprovs.

    Args:
        cid: The CID to check
        ipfs_peer_id: The IPFS peer ID to check against

    Returns: BlockCheckResult
    """
    ipfs_node_url = get_ipfs_node_url()
    routing_url = f"{ipfs_node_url}/api/v0/routing/findprovs"
    params = {"arg": cid}
    timeout = get_ipfs_timeout("dht")

    # Cast to string to handle any special types
    normalized_peer_id = str(ipfs_peer_id)
    logger.info(f"Checking if {normalized_peer_id} provides {cid}")

    success = False
    try:
        async with httpx.AsyncClient(timeout=timeout) as client:
            response = await client.post(routing_url, params=params)
            response_lines = response.text.splitlines()

            for _line_idx, line in enumerate(response_lines):
                if not line.strip():
                    continue

                try:
                    data = json.loads(line)

                    # Type 0 responses are direct provider announcements
                    if data.get("Type") == 0:
                        provider_id = str(data.get("ID", ""))

                        # Do case-insensitive comparison to catch capitalization issues
                        if provider_id.lower() == normalized_peer_id.lower():
                            success = True
                            break

                    # Type 1 responses contain lists of providers from other peers
                    if data.get("Type") == 1 and data.get("Responses"):
                        for provider in data.get("Responses", []):
                            provider_id = str(provider.get("ID", ""))

                            # Do case-insensitive comparison
                            if provider_id.lower() == normalized_peer_id.lower():
                                success = True
                                break

                        if success:
                            break

                    # Some IPFS nodes use a different format with direct Providers array
                    if "Providers" in data:
                        providers_list = data.get("Providers", [])
                        for provider in providers_list:
                            if isinstance(provider, str):
                                provider_id = provider
                            elif isinstance(provider, dict):
                                provider_id = str(provider.get("ID", ""))
                            else:
                                continue

                            if provider_id.lower() == normalized_peer_id.lower():
                                success = True
                                break

                        if success:
                            break

                except json.JSONDecodeError:
                    continue

    except Exception as e:
        logger.error(f"Error checking if peer provides CID: {e}")
        return BlockCheckResult(
            ipfs_peer_id=str(ipfs_peer_id),
            cid=cid,
            success=False,
        )

    logger.info(f"CID provider check result: {normalized_peer_id=} {cid=} {success=}")

    return BlockCheckResult(
        ipfs_peer_id=str(ipfs_peer_id),
        cid=cid,
        success=success,
    )


async def get_block_cids(cid: str) -> List[str]:
    """
    Get all block CIDs that make up a given CID using the refs API.

    Args:
        cid: The CID to get blocks for

    Returns:
        List of block CIDs
    """
    ipfs_node_url = get_ipfs_node_url()
    refs_api_url = f"{ipfs_node_url}/api/v0/refs"
    refs_params = {"arg": cid, "recursive": "true"}
    timeout = get_ipfs_timeout("refs")

    block_cids = []
    async with httpx.AsyncClient(timeout=timeout) as client:
        response = await client.post(refs_api_url, params=refs_params)
        if response.status_code == 200:
            for line in response.text.splitlines():
                if not line.strip():
                    continue
                try:
                    block_cids.append(json.loads(line)["Ref"])
                except (json.JSONDecodeError, KeyError):
                    continue

    return block_cids if block_cids else [cid]


async def upload_json_to_ipfs(
    data: Optional[List[Dict]] = None,
    file_path: Optional[str] = None,
    json_str: Optional[str] = None,
    api_url: Optional[str] = None,
    pin: bool = True,
    timeout: int = 20,
) -> Dict[str, Optional[str]]:
    """
    Upload JSON data to IPFS and return the CID.

    Args:
        data: JSON-serializable data to upload
        file_path: Path to JSON file to upload
        json_str: Pre-serialized JSON string to upload
        api_url: IPFS API endpoint URL (default: from env vars)
        pin: Whether to pin the content
        timeout: Request timeout in seconds

    Returns:
        Dictionary with success status, CID, and error message
    """
    # Validate exactly one input source is provided
    input_sources = [data is not None, file_path is not None, json_str is not None]
    if sum(input_sources) != 1:
        return {
            "success": False,
            "cid": None,
            "error": "Exactly one of data, file_path, or json_str must be provided",
        }

    # Use default API URL if not provided
    if api_url is None:
        api_url = get_ipfs_node_url()

    try:
        # Prepare the JSON data
        if file_path:
            with open(file_path) as f:
                json_str = f.read()
            filename = os.path.basename(file_path)
        elif data is not None:
            json_str = json.dumps(data)
            filename = "data.json"
        else:
            filename = "data.json"

        # Prepare the request
        url = f"{api_url}/api/v0/add?cid-version=1&pin={str(pin).lower()}"
        boundary = "----WebKitFormBoundary7MA4YWxkTrZu0gW"

        body = (
            f"--{boundary}\r\n"
            f'Content-Disposition: form-data; name="file"; filename="{filename}"\r\n'
            f"Content-Type: application/json\r\n\r\n"
            f"{json_str}\r\n"
            f"--{boundary}--\r\n"
        ).encode()

        headers = {"Content-Type": f"multipart/form-data; boundary={boundary}"}

        # Execute the request
        async with httpx.AsyncClient() as client:
            response = await client.post(url, headers=headers, content=body, timeout=timeout)

            if not response.is_success:
                error_text = response.text
                return {
                    "success": False,
                    "cid": None,
                    "error": f"IPFS API error: {response.status_code} - {error_text}",
                }

            response_data = response.json()
            cid = response_data.get("Hash")

            if not cid:
                return {"success": False, "cid": None, "error": "No CID in response"}

            return {"success": True, "cid": cid, "error": None}

    except httpx.TimeoutException:
        return {"success": False, "cid": None, "error": "Request timed out"}
    except Exception as e:
        return {"success": False, "cid": None, "error": f"Unexpected error: {str(e)}"}


async def get_child_cids(cid: str) -> List[str]:
    """
    Fetch content from IPFS using the cat API.

    Args:
        cid: The CID to fetch content for

    Returns:
        A list of CIDs.
    """

    logger.info(f"Fetching nested block cids for {str(cid)=}")
    ipfs_node_url = get_ipfs_node_url()
    cat_url = f"{ipfs_node_url}/api/v0/cat"
    timeout = get_ipfs_timeout("fetch")
    params = {"arg": cid}

    async with httpx.AsyncClient(timeout=timeout) as client:
        response = await client.post(cat_url, params=params)
        response.raise_for_status()
        result = response.json()

    cids = []
    for entry in result:
        raw_cid = entry["file_hash"]
        decoded_hex = bytearray(raw_cid).decode("utf-8")
        cid = bytes.fromhex(decoded_hex).decode("utf-8")
        cids.append(cid)

    return cids


async def get_file_size(cid: str) -> Optional[int]:
    """
    Get the size of a file in IPFS by its CID.

    Args:
        cid: The CID to get the size for

    Returns:
        The file size in bytes, or None if the file could not be found
    """
    ipfs_node_url = get_ipfs_node_url()
    stat_url = f"{ipfs_node_url}/api/v0/files/stat"
    timeout = get_ipfs_timeout("fetch")
    params = {"arg": f"/ipfs/{cid}"}

    try:
        async with httpx.AsyncClient(timeout=timeout) as client:
            response = await client.post(stat_url, params=params)
            response.raise_for_status()

            data = response.json()
            return data.get("Size")
    except Exception as e:
        logger.error(f"Error getting file size for CID {cid}: {e}")
        return None
