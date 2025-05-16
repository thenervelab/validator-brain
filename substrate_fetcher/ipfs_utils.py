import aiohttp
import json
import logging
import asyncio
import os
import aiofiles
from multiformats import CID
from typing import Dict, Optional, List, Any

# Configure logging
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

async def test_node_connectivity(api_url: str) -> bool:
    """Test if the IPFS node is reachable."""
    async with aiohttp.ClientSession() as session:
        try:
            async with session.post(
                f"{api_url}/api/v0/version",
                timeout=aiohttp.ClientTimeout(total=3)
            ) as resp:
                logger.debug("Node version check status: %s", resp.status)
                if resp.status == 200:
                    data = await resp.json()
                    logger.info("IPFS node reachable at %s: %s", api_url, data)
                    return True
                else:
                    logger.error("Node version check failed at %s: %s - %s", api_url, resp.status, await resp.text())
                    return False
        except Exception as e:
            logger.error("Failed to connect to IPFS node at %s: %s", api_url, e)
            return False

async def pin_cid(cid: str, api_url: str = 'http://127.0.0.1:5001', recursive: bool = True) -> Dict:
    """
    Pin a CID to ensure it is retained locally.

    Args:
        cid (str): The CID to pin.
        api_url (str): The IPFS HTTP API endpoint (default: http://127.0.0.1:5001).
        recursive (bool): Recursively pin linked objects (default: True).

    Returns:
        Dict: {'success': bool, 'cid': str, 'error': str or None}
    """
    if not await test_node_connectivity(api_url):
        return {'success': False, 'cid': cid, 'error': "IPFS node is not reachable"}

    async with aiohttp.ClientSession() as session:
        try:
            # Validate CID
            logger.debug("Parsing CID: %s", cid)
            try:
                CID.decode(cid)
            except ValueError as e:
                logger.error("Invalid CID %s: %s", cid, e)
                return {'success': False, 'cid': cid, 'error': str(e)}

            # Pin the CID
            logger.debug("Pinning CID %s at %s", cid, api_url)
            async with session.post(
                f"{api_url}/api/v0/pin/add?arg={cid}&recursive={str(recursive).lower()}",
                timeout=aiohttp.ClientTimeout(total=10)
            ) as resp:
                logger.debug("Pin add response status for CID %s: %s", cid, resp.status)
                if resp.status != 200:
                    error_text = await resp.text()
                    logger.error("Pin add failed for CID %s: %s - %s", cid, resp.status, error_text)
                    return {'success': False, 'cid': cid, 'error': f"Pin failed: {error_text}"}
                
                data = await resp.json()
                logger.debug("Pin add response for CID %s: %s", cid, data)
                if cid in data.get('Pins', []):
                    return {'success': True, 'cid': cid, 'error': None}
                else:
                    return {'success': False, 'cid': cid, 'error': "CID not found in pinned list"}

        except asyncio.TimeoutError:
            logger.error("Timeout pinning CID %s at %s", cid, api_url)
            return {'success': False, 'cid': cid, 'error': "Timeout pinning CID"}
        except Exception as e:
            logger.error("Error pinning CID %s at %s: %s", cid, api_url, e)
            return {'success': False, 'cid': cid, 'error': str(e)}

async def unpin_cid(cid: str, api_url: str = 'http://127.0.0.1:5001', recursive: bool = True) -> Dict:
    """
    Unpin a CID to allow it to be garbage collected.

    Args:
        cid (str): The CID to unpin.
        api_url (str): The IPFS HTTP API endpoint (default: http://127.0.0.1:5001).
        recursive (bool): Recursively unpin linked objects (default: True).

    Returns:
        Dict: {'success': bool, 'cid': str, 'error': str or None}
    """
    if not await test_node_connectivity(api_url):
        return {'success': False, 'cid': cid, 'error': "IPFS node is not reachable"}

    async with aiohttp.ClientSession() as session:
        try:
            # Validate CID
            logger.debug("Parsing CID: %s", cid)
            try:
                CID.decode(cid)
            except ValueError as e:
                logger.error("Invalid CID %s: %s", cid, e)
                return {'success': False, 'cid': cid, 'error': str(e)}

            # Unpin the CID
            logger.debug("Unpinning CID %s at %s", cid, api_url)
            async with session.post(
                f"{api_url}/api/v0/pin/rm?arg={cid}&recursive={str(recursive).lower()}",
                timeout=aiohttp.ClientTimeout(total=10)
            ) as resp:
                logger.debug("Pin rm response status for CID %s: %s", cid, resp.status)
                if resp.status != 200:
                    error_text = await resp.text()
                    logger.error("Pin rm failed for CID %s: %s - %s", cid, resp.status, error_text)
                    return {'success': False, 'cid': cid, 'error': f"Unpin failed: {error_text}"}
                
                data = await resp.json()
                logger.debug("Pin rm response for CID %s: %s", cid, data)
                if cid in data.get('Pins', []):
                    return {'success': True, 'cid': cid, 'error': None}
                else:
                    return {'success': False, 'cid': cid, 'error': "CID not found in unpinned list"}

        except asyncio.TimeoutError:
            logger.error("Timeout unpinning CID %s at %s", cid, api_url)
            return {'success': False, 'cid': cid, 'error': "Timeout unpinning CID"}
        except Exception as e:
            logger.error("Error unpinning CID %s at %s: %s", cid, api_url, e)
            return {'success': False, 'cid': cid, 'error': str(e)}

async def run_garbage_collector(api_url: str = 'http://127.0.0.1:5001', quiet: bool = False) -> Dict:
    """
    Run the garbage collector to remove unpinned blocks.

    Args:
        api_url (str): The IPFS HTTP API endpoint (default: http://127.0.0.1:5001).
        quiet (bool): Write minimal output (default: False).

    Returns:
        Dict: {'success': bool, 'removed_blocks': List[Dict], 'error': str or None}
    """
    if not await test_node_connectivity(api_url):
        return {'success': False, 'removed_blocks': [], 'error': "IPFS node is not reachable"}

    async with aiohttp.ClientSession() as session:
        try:
            logger.debug("Running garbage collector at %s", api_url)
            async with session.post(
                f"{api_url}/api/v0/repo/gc?quiet={str(quiet).lower()}",
                timeout=aiohttp.ClientTimeout(total=30)
            ) as resp:
                logger.debug("Garbage collector response status: %s", resp.status)
                if resp.status != 200:
                    error_text = await resp.text()
                    logger.error("Garbage collector failed: %s - %s", resp.status, error_text)
                    return {'success': False, 'removed_blocks': [], 'error': f"Garbage collection failed: {error_text}"}
                
                removed_blocks = []
                response_text = await resp.text()
                logger.debug("Garbage collector response: %s", response_text[:200])
                try:
                    # Parse NDJSON response
                    for line in response_text.splitlines():
                        if line.strip():
                            data = json.loads(line)
                            if data.get('Key', {}).get('/'):
                                removed_blocks.append({
                                    'cid': data['Key']['/'],
                                    'error': data.get('Error', '')
                                })
                    logger.debug("Removed %d blocks", len(removed_blocks))
                    return {'success': True, 'removed_blocks': removed_blocks, 'error': None}
                except json.JSONDecodeError:
                    logger.error("Garbage collector returned non-JSON response: %s", response_text)
                    return {'success': False, 'removed_blocks': [], 'error': "Non-JSON response from garbage collector"}

        except asyncio.TimeoutError:
            logger.error("Timeout running garbage collector at %s", api_url)
            return {'success': False, 'removed_blocks': [], 'error': "Timeout running garbage collector"}
        except Exception as e:
            logger.error("Error running garbage collector at %s: %s", api_url, e)
            return {'success': False, 'removed_blocks': [], 'error': str(e)}

async def get_file_size(cid: str, api_url: str = 'http://127.0.0.1:5001') -> Dict:
    """
    Get the cumulative size of a CID's DAG (file or folder).

    Args:
        cid (str): The CID to get the size for.
        api_url (str): The IPFS HTTP API endpoint (default: http://127.0.0.1:5001).

    Returns:
        Dict: {'success': bool, 'cid': str, 'size': int or None, 'error': str or None}
    """
    if not await test_node_connectivity(api_url):
        return {'success': False, 'cid': cid, 'size': None, 'error': "IPFS node is not reachable"}

    async with aiohttp.ClientSession() as session:
        try:
            # Validate CID
            logger.debug("Parsing CID: %s", cid)
            try:
                CID.decode(cid)
            except ValueError as e:
                logger.error("Invalid CID %s: %s", cid, e)
                return {'success': False, 'cid': cid, 'size': None, 'error': str(e)}

            # Get DAG stats
            logger.debug("Fetching DAG stats for CID %s at %s", cid, api_url)
            async with session.post(
                f"{api_url}/api/v0/dag/stat?arg={cid}",
                timeout=aiohttp.ClientTimeout(total=10)
            ) as resp:
                logger.debug("DAG stat response status for CID %s: %s", cid, resp.status)
                if resp.status != 200:
                    error_text = await resp.text()
                    logger.error("DAG stat failed for CID %s: %s - %s", cid, resp.status, error_text)
                    return {'success': False, 'cid': cid, 'size': None, 'error': f"DAG stat failed: {error_text}"}
                
                data = await resp.json()
                logger.debug("DAG stat response for CID %s: %s", cid, data)
                size = data.get('TotalSize', None)
                if size is not None:
                    return {'success': True, 'cid': cid, 'size': size, 'error': None}
                else:
                    return {'success': False, 'cid': cid, 'size': None, 'error': "No size found in DAG stats"}

        except asyncio.TimeoutError:
            logger.error("Timeout fetching DAG stats for CID %s at %s", cid, api_url)
            return {'success': False, 'cid': cid, 'size': None, 'error': "Timeout fetching DAG stats"}
        except Exception as e:
            logger.error("Error fetching DAG stats for CID %s at %s: %s", cid, api_url, e)
            return {'success': False, 'cid': cid, 'size': None, 'error': str(e)}

async def create_or_update_profile_json(
    profile_type: str,
    id: str,
    profile_data: Dict,
    api_url: str = 'http://127.0.0.1:5001',
    output_dir: str = './profiles'
) -> Dict:
    """
    Create or append to a local JSON file for a user or miner profile.

    Args:
        profile_type (str): 'user' or 'miner' to determine the profile structure.
        id (str): miner_id (peer ID) for miners or user_id (SS58 address) for users.
        profile_data (Dict): Profile data matching the user or miner structure.
        api_url (str): The IPFS HTTP API endpoint (default: http://127.0.0.1:5001).
        output_dir (str): Directory to store JSON files (default: ./profiles).

    Returns:
        Dict: {'success': bool, 'file_path': str, 'error': str or None}
    """
    if not await test_node_connectivity(api_url):
        return {'success': False, 'file_path': '', 'error': "IPFS node is not reachable"}

    try:
        # Validate profile_type
        if profile_type.lower() not in ['user', 'miner']:
            return {'success': False, 'file_path': '', 'error': "Invalid profile_type: must be 'user' or 'miner'"}

        # Validate profile_data structure
        required_user_fields = {
            'created_at', 'file_hash', 'file_name', 'file_size_in_bytes', 'is_assigned',
            'last_charged_at', 'main_req_hash', 'miner_ids', 'owner', 'selected_validator', 'total_replicas'
        }
        required_miner_fields = {
            'created_at', 'file_hash', 'file_size_in_bytes', 'miner_node_id', 'selected_validator'
        }
        if profile_type.lower() == 'user' and not all(field in profile_data for field in required_user_fields):
            return {'success': False, 'file_path': '', 'error': f"Missing required user profile fields: {required_user_fields}"}
        if profile_type.lower() == 'miner' and not all(field in profile_data for field in required_miner_fields):
            return {'success': False, 'file_path': '', 'error': f"Missing required miner profile fields: {required_miner_fields}"}

        # Create output directory if it doesn't exist
        os.makedirs(output_dir, exist_ok=True)

        # Determine file name
        file_name = f"{id}.json"
        file_path = os.path.join(output_dir, file_name)
        logger.debug("Processing profile for %s ID %s at %s", profile_type, id, file_path)

        # Read existing data or initialize new list
        profiles = []
        if os.path.exists(file_path):
            try:
                with open(file_path, 'r') as f:
                    profiles = json.load(f)
                    if not isinstance(profiles, list):
                        logger.error("Existing file %s is not a JSON list", file_path)
                        return {'success': False, 'file_path': file_path, 'error': "Existing file is not a JSON list"}
            except json.JSONDecodeError:
                logger.error("Invalid JSON in existing file %s", file_path)
                return {'success': False, 'file_path': file_path, 'error': "Invalid JSON in existing file"}

        # Append new profile data
        profiles.append(profile_data)

        # Write updated data back to file
        try:
            with open(file_path, 'w') as f:
                json.dump(profiles, f, indent=2)
            logger.debug("Successfully updated %s profile file %s with %d entries", profile_type, file_path, len(profiles))
            return {'success': True, 'file_path': file_path, 'error': None}
        except Exception as e:
            logger.error("Error writing to file %s: %s", file_path, e)
            return {'success': False, 'file_path': file_path, 'error': f"Error writing to file: {str(e)}"}

    except Exception as e:
        logger.error("Error processing %s profile for ID %s: %s", profile_type, id, e)
        return {'success': False, 'file_path': '', 'error': str(e)}

async def upload_json_to_ipfs(
    data: Optional[List[Dict]] = None,
    file_path: Optional[str] = None,
    json_str: Optional[str] = None,
    api_url: str = 'http://127.0.0.1:5001',
    pin: bool = True
) -> Dict:
    """
    Upload a JSON list, file, or pre-serialized JSON string to IPFS and return the CID.

    Args:
        data (List[Dict], optional): JSON data to upload directly.
        file_path (str, optional): Path to a JSON file to upload.
        json_str (str, optional): Pre-serialized JSON string to upload.
        api_url (str): The IPFS HTTP API endpoint (default: http://127.0.0.1:5001).
        pin (bool): Pin the uploaded data locally (default: True).

    Returns:
        Dict: {'success': bool, 'cid': str or None, 'error': str or None}
    """
    logger.info("data_present=%s, file_path_present=%s, json_str_present=%s", 
                     data_present, file_path_present, json_str_present)
    if sum(1 for x in (data_present, file_path_present, json_str_present)) != 1:
        logger.info("Validation failed: data_present=%s, file_path_present=%s, json_str_present=%s", 
                     data_present, file_path_present, json_str_present)
        return {'success': False, 'cid': None, 'error': "Exactly one of data, file_path, or json_str must be provided"}

    async with aiohttp.ClientSession() as session:
        try:
            # Prepare data
            if file_path:
                logger.debug("Reading JSON file from %s", file_path)
                try:
                    with open(file_path, 'r') as f:
                        json_str = f.read()
                    filename = os.path.basename(file_path)
                except Exception as e:
                    logger.error("Error reading file %s: %s", file_path, e)
                    return {'success': False, 'cid': None, 'error': f"Error reading file: {str(e)}"}
            elif data is not None:
                json_str = json.dumps(data)
                filename = 'profiles.json'
            elif json_str is not None:
                filename = 'profiles.json'

            logger.debug("Uploading JSON data to IPFS at %s (size: %d bytes)", api_url, len(json_str))

            # Prepare multipart form data
            form_data = aiohttp.FormData()
            form_data.add_field('file', json_str, filename=filename, content_type='application/json')

            # Upload to IPFS
            async with session.post(
                f"{api_url}/api/v0/add?pin={str(pin).lower()}",
                data=form_data,
                timeout=aiohttp.ClientTimeout(total=10)
            ) as resp:
                logger.debug("Add response status: %s", resp.status)
                if resp.status != 200:
                    error_text = await resp.text()
                    logger.error("Add failed: %s - %s", resp.status, error_text)
                    return {'success': False, 'cid': None, 'error': f"Upload failed: {error_text}"}

                response_text = await resp.text()
                logger.debug("Add response: %s", response_text[:200])
                try:
                    data = json.loads(response_text)
                    cid = data.get('Hash')
                    if cid:
                        try:
                            CID.decode(cid)
                            return {'success': True, 'cid': cid, 'error': None}
                        except ValueError:
                            return {'success': False, 'cid': None, 'error': "Invalid CID returned"}
                    else:
                        return {'success': False, 'cid': None, 'error': "No CID returned"}
                except json.JSONDecodeError:
                    logger.error("Add returned non-JSON response: %s", response_text)
                    return {'success': False, 'cid': None, 'error': "Non-JSON response from add"}

        except asyncio.TimeoutError:
            logger.error("Timeout uploading JSON to IPFS at %s", api_url)
            return {'success': False, 'cid': None, 'error': "Timeout uploading JSON"}
        except Exception as e:
            logger.error("Error uploading JSON to IPFS at %s: %s", api_url, e)
            return {'success': False, 'cid': None, 'error': str(e)}

async def clean_profile_directory(
    api_url: str = 'http://127.0.0.1:5001',
    output_dir: str = './profiles'
) -> Dict:
    """
    Remove all JSON files from the profile directory to empty it.

    Args:
        api_url (str): The IPFS HTTP API endpoint (default: http://127.0.0.1:5001).
        output_dir (str): Directory containing JSON profile files (default: ./profiles).

    Returns:
        Dict: {'success': bool, 'removed_files': List[str], 'error': str or None}
    """
    if not await test_node_connectivity(api_url):
        return {'success': False, 'removed_files': [], 'error': "IPFS node is not reachable"}

    try:
        # Ensure directory exists
        os.makedirs(output_dir, exist_ok=True)
        logger.debug("Cleaning profile directory %s", output_dir)

        removed_files = []
        for filename in os.listdir(output_dir):
            if filename.endswith('.json'):
                file_path = os.path.join(output_dir, filename)
                try:
                    async with aiofiles.open(file_path, 'r') as f:
                        # Verify it's a valid JSON file
                        await f.read()  # Read to ensure file is accessible
                    await aiofiles.os.remove(file_path)
                    logger.debug("Removed file %s", file_path)
                    removed_files.append(file_path)
                except json.JSONDecodeError:
                    logger.warning("Skipping invalid JSON file %s", file_path)
                except Exception as e:
                    logger.error("Error removing file %s: %s", file_path, e)
                    return {'success': False, 'removed_files': removed_files, 'error': f"Error removing file {file_path}: {str(e)}"}

        logger.debug("Successfully removed %d files from %s", len(removed_files), output_dir)
        return {'success': True, 'removed_files': removed_files, 'error': None}

    except Exception as e:
        logger.error("Error cleaning profile directory %s: %s", output_dir, e)
        return {'success': False, 'removed_files': [], 'error': str(e)}


async def ping_ipfs_node(ipfs_peer_id: str) -> bool:
    """Pings an IPFS node to test connectivity without storing results in the database."""
    if not ipfs_peer_id:
        logger.warning(f"No IPFS peer ID provided. Skipping ping.")
        return False

    logger.info(f"Pinging IPFS node: {ipfs_peer_id}...")
    
    ping_successful = False
    api_url = f"{config.IPFS_NODE_URL.rstrip('/')}/api/v0/ping"
    params = {'arg': ipfs_peer_id, 'count': '1'}  # count must be a string for query params
    timeout_seconds = getattr(config, 'IPFS_TIMEOUT_SECONDS', 10)
    request_timeout = aiohttp.ClientTimeout(total=timeout_seconds)

    try:
        async with aiohttp.ClientSession(timeout=request_timeout) as session:
            async with session.post(api_url, params=params) as response:
                if response.status == 200:
                    async for line in response.content:
                        try:
                            data = json.loads(line.decode('utf-8'))
                            if data.get('Success') and (data.get('Time') or data.get('AvgLatency')):
                                ping_successful = True
                                break  # Found success signal
                        except json.JSONDecodeError:
                            logger.debug(f"Non-JSON line from IPFS ping for {ipfs_peer_id}: {line}")
                        except Exception as e_parse:
                            logger.warning(f"Error parsing IPFS ping response line for {ipfs_peer_id}: {e_parse}")
                    if not ping_successful:
                        logger.warning(f"IPFS ping to {ipfs_peer_id} completed with HTTP 200 but no definitive success signal (RTT or Avg Latency) in response stream.")
                else:
                    error_text = await response.text()
                    logger.warning(f"IPFS ping to {ipfs_peer_id} failed with status {response.status}: {error_text}")
    except asyncio.TimeoutError:
        logger.warning(f"IPFS ping to {ipfs_peer_id} timed out after {timeout_seconds} seconds.")
    except aiohttp.ClientConnectorError as e_conn:
        logger.error(f"IPFS connection error for {ipfs_peer_id}: {e_conn}")
    except Exception as e_req:
        logger.error(f"Request error during IPFS ping for {ipfs_peer_id}: {e_req}")

    if ping_successful:
        logger.info(f"Successfully pinged IPFS node: {ipfs_peer_id}")
    else:
        logger.warning(f"Failed to ping IPFS node: {ipfs_peer_id}")
    
    return ping_successful

async def get_ipfs_content(cid: str, api_url: str) -> dict:
    """
    Fetches content from IPFS using the provided CID via POST request.

    Args:
        cid (str): The IPFS CID to fetch content for.
        api_url (str): The IPFS HTTP API endpoint (e.g., 'http://127.0.0.1:5001').

    Returns:
        Dict: A dictionary with the following keys:
            - success (bool): True if the operation was successful, False otherwise.
            - content (List[Dict] or None): The fetched content as a list of dictionaries, or None if failed.
            - error (str or None): Error message if the operation failed, None otherwise.
    """
    if not cid:
        logger.warning("No CID provided for IPFS content fetch.")
        return {'success': False, 'content': None, 'error': "No CID provided"}

    logger.info(f"Fetching IPFS content for CID: {cid}")
    cat_url = f"{api_url.rstrip('/')}/api/v0/cat"
    timeout = aiohttp.ClientTimeout(total=10)

    try:
        async with aiohttp.ClientSession(timeout=timeout) as session:
            # Use query parameters instead of form data with the correct parameter name 'ipfs-path'
            params = {'arg': cid}
            async with session.post(cat_url, params=params) as response:
                if response.status != 200:
                    error_text = await response.text()
                    logger.warning(f"Failed to fetch IPFS content for CID {cid}: HTTP {response.status} - {error_text}")
                    return {'success': False, 'content': None, 'error': f"HTTP {response.status}: {error_text}"}

                content_text = await response.text()
                try:
                    content = json.loads(content_text)
                    logger.info(f"Successfully fetched IPFS content for CID {cid}")
                    return {'success': True, 'content': content, 'error': None}
                except json.JSONDecodeError as e:
                    logger.error(f"Failed to parse IPFS content as JSON for CID {cid}: {e}")
                    return {'success': False, 'content': None, 'error': f"Invalid JSON: {str(e)}"}

    except asyncio.TimeoutError:
        logger.error(f"Timeout while fetching IPFS content for CID {cid}")
        return {'success': False, 'content': None, 'error': "Request timed out"}
    except aiohttp.ClientError as e:
        logger.error(f"Client error while fetching IPFS content for CID {cid}: {e}")
        return {'success': False, 'content': None, 'error': f"Client error: {str(e)}"}
    except Exception as e:
        logger.error(f"Unexpected error while fetching IPFS content for CID {cid}: {e}")
        return {'success': False, 'content': None, 'error': f"Unexpected error: {str(e)}"}