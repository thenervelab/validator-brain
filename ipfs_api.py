import json
import os
from typing import Dict, List, Optional, Any

import httpx

from utils import get_ipfs_node_url, get_ipfs_timeout, logger


async def ping_ipfs_node(ipfs_peer_id: str) -> Dict:
    """
    Ping an IPFS node to test connectivity.
    
    Args:
        ipfs_peer_id: The IPFS peer ID to ping
        
    Returns:
        Dictionary with ping results {
            'success': bool,
            'time_ms': Optional[float],
            'error': Optional[str]
        }
    """
    logger.info(f"Pinging IPFS node: {ipfs_peer_id}")

    result = {'success': False, 'time_ms': None, 'error': None}

    try:
        ipfs_node_url = get_ipfs_node_url()
        api_url = f"{ipfs_node_url}/api/v0/ping"
        params = {'arg': ipfs_peer_id, 'count': '1'}
        timeout = get_ipfs_timeout("ping")

        async with httpx.AsyncClient(timeout=timeout) as client:
            response = await client.post(api_url, params=params)

            if response.status_code == 200:
                # IPFS ping returns newline-delimited JSON
                for line in response.text.splitlines():
                    if not line.strip():
                        continue

                    try:
                        data = json.loads(line)
                        if data.get('Success'):
                            result['success'] = True
                            result['time_ms'] = data.get('Time', 0)
                            logger.info(f"Successfully pinged {ipfs_peer_id}: {data}")
                            break
                    except json.JSONDecodeError:
                        continue
            else:
                error_text = response.text
                logger.warning(
                    f"Failed to ping {ipfs_peer_id}: HTTP {response.status_code} - {error_text}")
                result['error'] = f"HTTP {response.status_code}: {error_text}"

    except httpx.ReadTimeout:
        logger.warning(f"Timeout pinging {ipfs_peer_id}")
        result['error'] = "Request timed out"
    except Exception as e:
        logger.error(f"Error pinging {ipfs_peer_id}: {e}")
        result['error'] = str(e)

    return result


async def check_cid_is_provided(cid: str, ipfs_peer_id: str) -> bool:
    """
    Check if a peer is a provider for this CID using routing findprovs.
    
    Args:
        cid: The CID to check
        ipfs_peer_id: The IPFS peer ID to check against
        
    Returns:
        True if the peer is a provider, False otherwise
    """
    ipfs_node_url = get_ipfs_node_url()
    routing_url = f"{ipfs_node_url}/api/v0/routing/findprovs"
    params = {'arg': cid}
    timeout = get_ipfs_timeout("dht")

    try:
        async with httpx.AsyncClient(timeout=timeout) as client:
            response = await client.post(routing_url, params=params)

            # Routing findprovs returns newline-delimited JSON
            for line in response.text.splitlines():
                if not line.strip():
                    continue

                data = json.loads(line)

                # Check if this peer is directly listed as provider
                if data.get('ID') == ipfs_peer_id:
                    return True

                # Check if peer is in the Responses list
                if data.get('Responses'):
                    for provider in data.get('Responses', []):
                        if provider.get('ID') == ipfs_peer_id:
                            return True
    except httpx.ReadTimeout:
        logger.error(f"Timed out waiting for routing findprovs for CID {cid}")
    except Exception as e:
        logger.error(f"Error checking if {ipfs_peer_id} provides {cid}: {e}")

    return False


async def get_file_size(cid: str) -> int:
    """
    Get the file size for a CID from IPFS using dag/stat.
    
    Args:
        cid: The CID to get the size for
        
    Returns:
        File size in bytes, defaults to 0 if size cannot be determined
    """
    try:
        ipfs_node_url = get_ipfs_node_url()
        url = f"{ipfs_node_url}/api/v0/dag/stat"
        params = {'arg': cid}
        timeout = get_ipfs_timeout("default")

        async with httpx.AsyncClient(timeout=timeout) as client:
            response = await client.post(url, params=params)

            if response.status_code == 200:
                # Parse the JSON response
                response_text = response.text
                for line in response_text.splitlines():
                    if not line.strip():
                        continue

                    try:
                        data = json.loads(line)
                        if 'TotalSize' in data:
                            return data['TotalSize']
                    except json.JSONDecodeError:
                        continue

                logger.warning(f"Could not find TotalSize in response for CID {cid}")
            else:
                logger.warning(
                    f"Failed to get file size for CID {cid}: HTTP {response.status_code}")

    except Exception as e:
        logger.error(f"Error getting file size for CID {cid}: {e}")

    # Default size if we can't determine it
    return 0


async def get_cid_blocks(cid: str) -> List[str]:
    """
    Get all block CIDs that make up a given CID using the refs API.
    
    Args:
        cid: The CID to get blocks for
        
    Returns:
        List of block CIDs
    """
    ipfs_node_url = get_ipfs_node_url()
    refs_api_url = f"{ipfs_node_url}/api/v0/refs"
    refs_params = {'arg': cid, 'recursive': 'true'}
    timeout = get_ipfs_timeout("refs")

    block_cids = []
    logger.info(f"Fetching blocks for CID {cid}")

    try:
        async with httpx.AsyncClient(timeout=timeout) as client:
            response = await client.post(refs_api_url, params=refs_params)

            if response.status_code == 200:
                # Parse the response which contains refs one per line
                for line in response.text.splitlines():
                    if not line.strip():
                        continue

                    try:
                        data = json.loads(line)
                        if 'Ref' in data:
                            block_cids.append(data['Ref'])
                    except json.JSONDecodeError:
                        continue
    except Exception as e:
        logger.error(f"Error fetching blocks for CID {cid}: {e}")

    # If no blocks found (maybe it's a raw file), use the CID itself as the only block
    if not block_cids:
        logger.info(f"No blocks found for CID {cid}, using the CID itself")
        block_cids = [cid]

    return block_cids


async def upload_json_to_ipfs(data: Optional[List[Dict]] = None, file_path: Optional[str] = None,
        json_str: Optional[str] = None, api_url: Optional[str] = None, pin: bool = True,
        timeout: int = 20) -> Dict[str, Optional[str]]:
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
        return {'success': False, 'cid': None,
            'error': "Exactly one of data, file_path, or json_str must be provided"}

    # Use default API URL if not provided
    if api_url is None:
        api_url = get_ipfs_node_url()

    try:
        # Prepare the JSON data
        if file_path:
            with open(file_path, 'r') as f:
                json_str = f.read()
            filename = os.path.basename(file_path)
        elif data is not None:
            json_str = json.dumps(data)
            filename = 'data.json'
        else:
            filename = 'data.json'

        # Prepare the request
        url = f"{api_url}/api/v0/add?cid-version=1&pin={str(pin).lower()}"
        boundary = "----WebKitFormBoundary7MA4YWxkTrZu0gW"

        body = (f"--{boundary}\r\n"
                f'Content-Disposition: form-data; name="file"; filename="{filename}"\r\n'
                f"Content-Type: application/json\r\n\r\n"
                f"{json_str}\r\n"
                f"--{boundary}--\r\n").encode('utf-8')

        headers = {"Content-Type": f"multipart/form-data; boundary={boundary}"}

        # Execute the request
        async with httpx.AsyncClient() as client:
            response = await client.post(url, headers=headers, content=body, timeout=timeout)

            if not response.is_success:
                error_text = response.text
                return {'success': False, 'cid': None,
                    'error': f"IPFS API error: {response.status_code} - {error_text}"}

            response_data = response.json()
            cid = response_data.get('Hash')

            if not cid:
                return {'success': False, 'cid': None, 'error': "No CID in response"}

            return {'success': True, 'cid': cid, 'error': None}

    except httpx.TimeoutException:
        return {'success': False, 'cid': None, 'error': "Request timed out"}
    except Exception as e:
        return {'success': False, 'cid': None, 'error': f"Unexpected error: {str(e)}"}


async def get_ipfs_content(cid: str) -> Dict[str, Any]:
    """
    Fetch content from IPFS using the cat API.
    
    Args:
        cid: The CID to fetch content for
        
    Returns:
        Dictionary with success status, content, and error message
    """
    if not cid:
        logger.warning("No CID provided for IPFS content fetch.")
        return {'success': False, 'content': None, 'error': "No CID provided"}

    logger.info(f"Fetching IPFS content for CID: {cid}")
    ipfs_node_url = get_ipfs_node_url()
    cat_url = f"{ipfs_node_url}/api/v0/cat"
    timeout = get_ipfs_timeout("fetch")
    params = {'arg': cid}

    try:
        async with httpx.AsyncClient(timeout=timeout) as client:
            response = await client.post(cat_url, params=params)

            if response.status_code != 200:
                error_text = await response.text
                logger.warning(
                    f"Failed to fetch IPFS content for CID {cid}: HTTP {response.status_code} - {error_text}")
                return {'success': False, 'content': None,
                        'error': f"HTTP {response.status_code}: {error_text}"}

            content_text = response.text
            try:
                content = json.loads(content_text)
                logger.info(f"Successfully fetched IPFS content for CID {cid}")
                return {'success': True, 'content': content, 'error': None}
            except json.JSONDecodeError as e:
                logger.error(f"Failed to parse IPFS content as JSON for CID {cid}: {e}")
                return {'success': False, 'content': None, 'error': f"Invalid JSON: {str(e)}"}

    except httpx.TimeoutException:
        logger.error(f"Timeout while fetching IPFS content for CID {cid}")
        return {'success': False, 'content': None, 'error': "Request timed out"}
    except Exception as e:
        logger.error(f"Unexpected error while fetching IPFS content for CID {cid}: {e}")
        return {'success': False, 'content': None, 'error': f"Unexpected error: {str(e)}"}
