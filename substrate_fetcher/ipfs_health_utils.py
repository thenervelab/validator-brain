import asyncio
import asyncpg
import logging
import json
import aiohttp
import random  # Added for selecting random block
from app.utils import config
from .file_availability_manager import FileAvailabilityManager

logger = logging.getLogger(__name__)


async def perform_ipfs_ping(db_pool: asyncpg.Pool, node_id: str, ipfs_peer_id: str, epoch_number: int, block_number: int = None, stop_event=None, availability_manager: FileAvailabilityManager = None):
    """
    Performs an IPFS ping to a miner using the local IPFS node's HTTP API
    and updates or inserts health stats into the miner_epoch_health table.
    
    Args:
        db_pool: Database connection pool
        node_id: The substrate node ID
        ipfs_peer_id: The IPFS peer ID
        epoch_number: The current epoch
        block_number: The current block number (optional)
        stop_event: Event to signal shutdown
    """
    stop_event = stop_event or asyncio.Event()  # Fallback to a new event if none provided

    if db_pool is None:
        logger.error(f"Database pool is not initialized. Cannot ping {node_id} (IPFS: {ipfs_peer_id}).")
        return
    if not ipfs_peer_id:
        logger.warning(f"No IPFS peer ID provided for node {node_id}. Skipping ping.")
        return

    logger.info(f"Pinging {node_id} (IPFS: {ipfs_peer_id}) for epoch {epoch_number}...")
    
    ping_successful = False
    # Ensure IPFS_NODE_URL is correctly formatted (e.g., http://localhost:5001)
    api_url = f"{config.get_ipfs_node_url()}/api/v0/ping"
    params = {'arg': ipfs_peer_id, 'count': '1'}  # count must be a string for query params
    timeout_seconds = config.get_ipfs_timeout('ping')
    
    request_timeout = aiohttp.ClientTimeout(total=timeout_seconds)

    try:
        async with aiohttp.ClientSession(timeout=request_timeout) as session:
            async with session.post(api_url, params=params) as response:
                if response.status == 200:
                    async for line in response.content:
                        if stop_event.is_set():  # Check for shutdown signal
                            logger.warning(f"Shutdown signal received during ping for {node_id}")
                            return  # Exit early if shutdown is signaled
                        try:
                            data = json.loads(line.decode('utf-8'))
                            if data.get('Success') and (data.get('Time') or data.get('AvgLatency')):
                                ping_successful = True
                                break  # Found success signal
                        except json.JSONDecodeError:
                            logger.debug(f"Non-JSON line from IPFS ping for {node_id}: {line}")
                        except Exception as e_parse:
                            logger.warning(f"Error parsing IPFS ping response line for {node_id}: {e_parse}")
                    if not ping_successful:
                        logger.warning(f"IPFS ping to {ipfs_peer_id} (Node: {node_id}) completed with HTTP 200 but no definitive success signal (RTT or Avg Latency) in response stream.")
                else:
                    error_text = await response.text()
                    logger.warning(f"IPFS ping to {ipfs_peer_id} (Node: {node_id}) failed with status {response.status}: {error_text}")
    except asyncio.TimeoutError:
        logger.warning(f"IPFS ping to {ipfs_peer_id} (Node: {node_id}) timed out after {timeout_seconds} seconds.")
    except aiohttp.ClientConnectorError as e_conn:
        logger.error(f"IPFS connection error for {ipfs_peer_id} (Node: {node_id}): {e_conn}")
    except Exception as e_req:
        logger.error(f"Request error during IPFS ping for {ipfs_peer_id} (Node: {node_id}): {e_req}")

    success_val = 1 if ping_successful else 0
    failure_val = 0 if ping_successful else 1

    async with db_pool.acquire() as conn:
        try:
            # Insert new record or update existing one for the (node_id, epoch)
            await conn.execute(
                """
                INSERT INTO miner_epoch_health (node_id, ipfs_peer_id, epoch,
                                              ping_successes, ping_failures,
                                              last_ping_attempt, last_ping_block, last_activity_at)
                VALUES ($1, $2, $3, $4, $5, NOW(), $6, NOW())
                ON CONFLICT (node_id, epoch) DO UPDATE
                SET ipfs_peer_id = EXCLUDED.ipfs_peer_id, /* Update if it changed */
                    ping_successes = miner_epoch_health.ping_successes + EXCLUDED.ping_successes,
                    ping_failures = miner_epoch_health.ping_failures + EXCLUDED.ping_failures,
                    last_ping_attempt = NOW(),
                    last_ping_block = $6,
                    last_activity_at = NOW();
                """, 
                node_id, ipfs_peer_id, epoch_number, success_val, failure_val, block_number
            )
            if ping_successful:
                logger.info(f"Successfully pinged and updated/inserted health for {node_id} (IPFS: {ipfs_peer_id}) in epoch {epoch_number}.")
            else:
                logger.warning(f"Failed to ping {node_id} (IPFS: {ipfs_peer_id}) in epoch {epoch_number}. Updated/inserted health stats.")
        except Exception as e_db:
            logger.error(f"Database error updating/inserting health for {node_id} (IPFS: {ipfs_peer_id}) in epoch {epoch_number}: {e_db}")

async def perform_ipfs_pin_check(db_pool: asyncpg.Pool, node_id: str, ipfs_peer_id: str, root_cid_to_check: str, epoch_number: int, stop_event=None, availability_manager: FileAvailabilityManager = None):
    """
    Performs an IPFS pin check. It verifies if a given ipfs_peer_id is a provider 
    for a randomly selected block within the DAG of root_cid_to_check.
    Logs the attempt to pin_check_log and updates miner_epoch_health.
    """
    stop_event = stop_event or asyncio.Event()  # Fallback to a new event if none provided

    if db_pool is None:
        logger.error(f"Database pool not initialized. Cannot perform pin check for {node_id}.")
        return
    if not root_cid_to_check:
        logger.warning(f"No root CID provided for pin check for {node_id} in epoch {epoch_number}. Skipping.")
        return
    if not ipfs_peer_id:
        logger.warning(f"No IPFS peer ID for node {node_id} for pin check. Skipping.")
        return

    logger.info(f"Starting pin check for {node_id} (IPFS: {ipfs_peer_id}) on root CID {root_cid_to_check} in epoch {epoch_number}...")
    
    pin_check_successful = False
    effective_cid_checked = root_cid_to_check  # Default to root CID
    refs_timeout_seconds = config.get_ipfs_timeout('refs')
    dht_timeout_seconds = config.get_ipfs_timeout('dht')

    all_block_cids = {root_cid_to_check}  # Include root CID itself as a possibility

    # 1. Get all unique block CIDs from the root CID's DAG
    refs_api_url = f"{config.get_ipfs_node_url()}/api/v0/refs"
    refs_params = {'arg': root_cid_to_check, 'recursive': 'true', 'unique': 'true'}
    refs_request_timeout = aiohttp.ClientTimeout(total=refs_timeout_seconds)

    try:
        logger.debug(f"Fetching refs for {root_cid_to_check} (Node: {node_id}). Timeout: {refs_timeout_seconds}s")
        async with aiohttp.ClientSession(timeout=refs_request_timeout) as session:
            async with session.post(refs_api_url, params=refs_params) as response:
                if response.status == 200:
                    async for line in response.content:
                        if stop_event.is_set():  # Check for shutdown signal
                            logger.warning(f"Shutdown signal received during refs fetch for {root_cid_to_check}")
                            return
                        try:
                            data = json.loads(line.decode('utf-8'))
                            if data.get('Ref'):
                                all_block_cids.add(data['Ref'])
                        except json.JSONDecodeError:
                            logger.debug(f"Non-JSON line from IPFS refs for {root_cid_to_check}: {line}")
                        except Exception as e_parse:
                            logger.warning(f"Error parsing IPFS refs response line for {root_cid_to_check}: {e_parse}")
                    logger.debug(f"Found {len(all_block_cids)} unique blocks for {root_cid_to_check}.")
                else:
                    error_text = await response.text()
                    logger.warning(f"Failed to fetch refs for {root_cid_to_check} (Node: {node_id}), status {response.status}: {error_text}. Will check root CID directly.")
    except asyncio.TimeoutError:
        logger.warning(f"Timeout ({refs_timeout_seconds}s) fetching refs for {root_cid_to_check} (Node: {node_id}). Will check root CID directly.")
    except aiohttp.ClientConnectorError as e_conn:
        logger.error(f"Connection error fetching refs for {root_cid_to_check} (Node: {node_id}): {e_conn}. Will check root CID directly.")
    except Exception as e_req:
        logger.error(f"Request error fetching refs for {root_cid_to_check} (Node: {node_id}): {e_req}. Will check root CID directly.")

    if all_block_cids:
        effective_cid_checked = random.choice(list(all_block_cids))
        logger.info(f"Selected random block {effective_cid_checked} (from {len(all_block_cids)} blocks) for DHT check for {node_id} (Root CID: {root_cid_to_check}).")
    else:
        # Should not happen if root_cid_to_check was added, but as a fallback
        logger.warning(f"No blocks found for {root_cid_to_check}, using root CID itself for DHT check.")
        effective_cid_checked = root_cid_to_check

    # 2. Check if the target ipfs_peer_id is a provider for the effective_cid_checked
    dht_api_url = f"{config.get_ipfs_node_url()}/api/v0/routing/findprovs"
    dht_params = {'arg': effective_cid_checked}  # Use the newer routing API
    dht_request_timeout = aiohttp.ClientTimeout(total=dht_timeout_seconds)

    try:
        logger.debug(f"Finding providers for {effective_cid_checked} (Node: {node_id}). Timeout: {dht_timeout_seconds}s")
        async with aiohttp.ClientSession(timeout=dht_request_timeout) as session:
            async with session.post(dht_api_url, params=dht_params) as response:
                if response.status == 200:
                    async for line in response.content:
                        if stop_event.is_set():  # Check for shutdown signal
                            logger.warning(f"Shutdown signal received during DHT findprovs for {effective_cid_checked}")
                            return
                        try:
                            data = json.loads(line.decode('utf-8'))
                            # Type 4 indicates a provider
                            if data.get('Type') == 4 and data.get('ID') == ipfs_peer_id:
                                logger.info(f"SUCCESS: Miner {node_id} (IPFS: {ipfs_peer_id}) IS a provider for {effective_cid_checked}.")
                                pin_check_successful = True
                                break 
                            # Also check Responses list, as provider messages can be nested
                            if data.get('Type') == 4 and data.get('Responses'):
                                for resp_peer in data['Responses']:
                                    if resp_peer.get('ID') == ipfs_peer_id:
                                        logger.info(f"SUCCESS: Miner {node_id} (IPFS: {ipfs_peer_id}) IS a provider (found in Responses) for {effective_cid_checked}.")
                                        pin_check_successful = True
                                        break
                            if pin_check_successful: break
                        except json.JSONDecodeError:
                            logger.debug(f"Non-JSON line from IPFS DHT for {effective_cid_checked}: {line}")
                        except Exception as e_parse:
                            logger.warning(f"Error parsing IPFS DHT response line for {effective_cid_checked}: {e_parse}")
                    if not pin_check_successful:
                        logger.warning(f"FAILURE: Miner {node_id} (IPFS: {ipfs_peer_id}) is NOT listed as a provider for {effective_cid_checked} after checking all stream results.")
                else:
                    error_text = await response.text()
                    logger.warning(f"DHT findprovs for {effective_cid_checked} (Node: {node_id}) failed with status {response.status}: {error_text}")
    except asyncio.TimeoutError:
        logger.warning(f"Timeout ({dht_timeout_seconds}s) during DHT findprovs for {effective_cid_checked} (Node: {node_id}).")
    except aiohttp.ClientConnectorError as e_conn:
        logger.error(f"Connection error during DHT findprovs for {effective_cid_checked} (Node: {node_id}): {e_conn}")
    except Exception as e_req:
        logger.error(f"Request error during DHT findprovs for {effective_cid_checked} (Node: {node_id}): {e_req}")

    # 3. update the database and record availability
    async with db_pool.acquire() as conn:
        try:
            # Update aggregate stats in miner_epoch_health
            success_val = 1 if pin_check_successful else 0
            failure_val = 0 if pin_check_successful else 1
            await conn.execute(
                """
                INSERT INTO miner_epoch_health (node_id, ipfs_peer_id, epoch,
                                              pin_check_successes, pin_check_failures,
                                              last_pin_check_attempt, last_activity_at)
                VALUES ($1, $2, $3, $4, $5, NOW(), NOW())
                ON CONFLICT (node_id, epoch) DO UPDATE
                SET ipfs_peer_id = EXCLUDED.ipfs_peer_id, /* Update if it changed */
                    pin_check_successes = miner_epoch_health.pin_check_successes + EXCLUDED.pin_check_successes,
                    pin_check_failures = miner_epoch_health.pin_check_failures + EXCLUDED.pin_check_failures,
                    last_pin_check_attempt = NOW(),
                    last_activity_at = NOW();
                """,
                node_id, ipfs_peer_id, epoch_number, success_val, failure_val
            )
            
            # Record availability status if manager is provided
            if availability_manager:
                if pin_check_successful:
                    await availability_manager.record_success(root_cid_to_check, node_id, epoch_number)
                else:
                    failure_reason = f"Miner {node_id} is not a provider for {effective_cid_checked}"
                    await availability_manager.record_failure(
                        root_cid_to_check, 
                        node_id, 
                        epoch_number, 
                        'not_provider', 
                        failure_reason
                    )
            
            logger.info(f"Updated pin check stats for {node_id} in epoch {epoch_number} (Root CID: {root_cid_to_check}, Checked CID: {effective_cid_checked}, Success: {pin_check_successful}).")
        except Exception as e_db:
            logger.error(f"Database error updating pin check stats for {node_id} in epoch {epoch_number}: {e_db}")

async def run_all_health_checks_for_epoch(db_pool: asyncpg.Pool, epoch_number: int, stop_event=None):
    """
    Iterates through miners in miner_epoch_health for the current epoch 
    and calls ping and pin check functions, using a random file_hash from miner_profile.
    """
    stop_event = stop_event or asyncio.Event()  # Fallback to a new event if none provided

    if db_pool is None:
        logger.error(f"Database pool is not initialized. Cannot run health checks for epoch {epoch_number}.")
        return

    async with db_pool.acquire() as conn:
        miners_to_check = await conn.fetch(
            "SELECT node_id, ipfs_peer_id FROM miner_epoch_health WHERE epoch = $1",
            epoch_number
        )

    if not miners_to_check:
        logger.info(f"No miners found in miner_epoch_health for epoch {epoch_number} to perform orchestrated checks.")
        return

    logger.info(f"Starting orchestrated health checks for {len(miners_to_check)} miners for epoch {epoch_number}.")
    for miner in miners_to_check:
        if stop_event.is_set():
            logger.info("Stop event detected, stopping health checks")
            return

        node_id = miner['node_id']
        ipfs_peer_id = miner['ipfs_peer_id']
        
        # Fetch a random file_hash from miner_profile for this miner
        async with db_pool.acquire() as conn:
            file_hash_record = await conn.fetchrow(
                """
                SELECT file_hash
                FROM miner_profile
                WHERE miner_node_id = $1
                ORDER BY RANDOM()
                LIMIT 1
                """,
                node_id
            )

        if not file_hash_record:
            logger.warning(f"No file_hash found in miner_profile for miner {node_id}. Skipping pin check.")
            # Perform only the ping check
            await perform_ipfs_ping(db_pool, node_id, ipfs_peer_id, epoch_number, stop_event=stop_event)
            continue

        file_hash_to_check = file_hash_record['file_hash']
        logger.info(f"Selected random file_hash {file_hash_to_check} for miner {node_id} pin check.")

        # Perform health checks
        await perform_ipfs_ping(db_pool, node_id, ipfs_peer_id, epoch_number, stop_event=stop_event)
        await perform_ipfs_pin_check(db_pool, node_id, ipfs_peer_id, file_hash_to_check, epoch_number, stop_event=stop_event)
        
    logger.info(f"Completed one round of orchestrated health checks for epoch {epoch_number}.") 