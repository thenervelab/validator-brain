import aiohttp
import json
import random
import asyncio
import logging
from multiformats import CID
from typing import List, Dict

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
                    logger.info("IPFS node reachable: %s", data)
                    return True
                else:
                    logger.error("Node version check failed: %s - %s", resp.status, await resp.text())
                    return False
        except Exception as e:
            logger.error("Failed to connect to IPFS node: %s", e)
            return False

async def find_peer_multiaddr(api_url: str, peer_id: str, retries: int = 3) -> str:
    """Find a peer's multiaddress using /routing/findpeer with retries."""
    for attempt in range(1, retries + 1):
        async with aiohttp.ClientSession() as session:
            try:
                async with session.post(
                    f"{api_url}/api/v0/routing/findpeer?arg={peer_id}",
                    timeout=aiohttp.ClientTimeout(total=10)
                ) as resp:
                    logger.debug("Findpeer attempt %d response status for peer %s: %s", attempt, peer_id, resp.status)
                    if resp.status != 200:
                        error_text = await resp.text()
                        logger.debug("Findpeer failed for peer %s: %s - %s", peer_id, resp.status, error_text)
                        if attempt == retries:
                            return None
                        continue
                    response_text = await resp.text()
                    try:
                        for line in response_text.splitlines():
                            if line.strip():
                                data = json.loads(line)
                                if data.get('Type') == 4 and data.get('Responses'):
                                    for response in data['Responses']:
                                        if response.get('ID') == peer_id:
                                            addrs = response.get('Addrs', [])
                                            if addrs:
                                                logger.debug("Found peer multiaddress: %s", addrs[0])
                                                return addrs[0]
                    except json.JSONDecodeError:
                        logger.error("Findpeer returned non-JSON response for peer %s: %s", peer_id, response_text)
                if attempt == retries:
                    return None
            except asyncio.TimeoutError:
                logger.error("Timeout finding peer %s on attempt %d", peer_id, attempt)
                if attempt == retries:
                    return None
                await asyncio.sleep(1)
            except Exception as e:
                logger.error("Error finding peer %s on attempt %d: %s", peer_id, attempt, e)
                if attempt == retries:
                    return None
    return None

async def connect_to_peer(api_url: str, peer_multiaddr: str) -> bool:
    """Connect to the specified peer using its multiaddress."""
    async with aiohttp.ClientSession() as session:
        try:
            async with session.post(
                f"{api_url}/api/v0/swarm/connect?arg={peer_multiaddr}",
                timeout=aiohttp.ClientTimeout(total=5)
            ) as resp:
                logger.debug("Swarm connect response status for %s: %s", peer_multiaddr, resp.status)
                if resp.status == 200:
                    logger.info("Successfully connected to peer %s", peer_multiaddr)
                    return True
                else:
                    error_text = await resp.text()
                    logger.error("Failed to connect to peer %s: %s - %s", peer_multiaddr, resp.status, error_text)
                    return False
        except asyncio.TimeoutError:
            logger.error("Timeout connecting to peer %s", peer_multiaddr)
            return False
        except Exception as e:
            logger.error("Error connecting to peer %s: %s", peer_multiaddr, e)
            return False

async def check_bitswap_ledger(api_url: str, cid: str, peer_id: str) -> bool:
    """Check if the peer has the block in their Bitswap ledger."""
    async with aiohttp.ClientSession() as session:
        try:
            async with session.post(
                f"{api_url}/api/v0/bitswap/ledger?arg={peer_id}",
                timeout=aiohttp.ClientTimeout(total=5)
            ) as resp:
                logger.debug("Bitswap ledger response status for peer %s: %s", peer_id, resp.status)
                if resp.status != 200:
                    error_text = await resp.text()
                    logger.debug("Bitswap ledger failed for peer %s: %s - %s", peer_id, resp.status, error_text)
                    return False
                data = await resp.json()
                logger.debug("Bitswap ledger data for peer %s: %s", peer_id, data)
                return data.get("Exchanged", 0) > 0  # Indicates block exchange activity
        except asyncio.TimeoutError:
            logger.error("Timeout checking Bitswap ledger for peer %s", peer_id)
            return False
        except Exception as e:
            logger.error("Error checking Bitswap ledger for peer %s: %s", peer_id, e)
            return False

async def check_block_stat(api_url: str, cid: str, peer_id: str, peers_data: Dict) -> bool:
    """Check block availability with /block/stat, isolating the target peer."""
    disconnected_peers = []
    async with aiohttp.ClientSession() as session:
        try:
            # Disconnect other peers
            logger.debug("Disconnecting other peers for CID %s", cid)
            for peer in peers_data.get('Peers', []):
                if peer['Peer'] != peer_id:
                    multiaddr = f"{peer['Addr']}/p2p/{peer['Peer']}"
                    try:
                        async with session.post(
                            f"{api_url}/api/v0/swarm/disconnect?arg={multiaddr}",
                            timeout=aiohttp.ClientTimeout(total=5)
                        ) as disconnect_resp:
                            logger.debug("Disconnected peer %s for CID %s: %s", peer['Peer'], cid, await disconnect_resp.text())
                            disconnected_peers.append(multiaddr)
                    except Exception as e:
                        logger.warning("Failed to disconnect peer %s for CID %s: %s", peer['Peer'], cid, e)

            # Check block availability
            logger.debug("Checking block availability for CID %s: %s", cid, cid)
            async with session.post(
                f"{api_url}/api/v0/block/stat?arg={cid}",
                timeout=aiohttp.ClientTimeout(total=5)
            ) as resp:
                logger.debug("Block stat response status for CID %s: %s", cid, resp.status)
                if resp.status == 200:
                    logger.debug("Block %s is available; peer %s has it", cid, peer_id)
                    return True
                else:
                    error_text = await resp.text()
                    logger.debug("Block %s not found for CID %s: %s - %s", cid, cid, resp.status, error_text)
                    return False
        except asyncio.TimeoutError:
            logger.error("Timeout checking block stat for CID %s", cid)
            return False
        except Exception as e:
            logger.error("Error checking block stat for CID %s: %s", cid, e)
            return False
        finally:
            # Reconnect disconnected peers
            if disconnected_peers:
                logger.debug("Reconnecting %d disconnected peers", len(disconnected_peers))
                for multiaddr in disconnected_peers:
                    try:
                        async with session.post(
                            f"{api_url}/api/v0/swarm/connect?arg={multiaddr}",
                            timeout=aiohttp.ClientTimeout(total=5)
                        ) as connect_resp:
                            logger.debug("Reconnected peer %s: %s", multiaddr, await connect_resp.text())
                    except Exception as e:
                        logger.warning("Failed to reconnect peer %s: %s", multiaddr, e)

async def check_random_block(cids: List[str], peer_id: str, api_url: str = 'http://127.0.0.1:5001') -> List[Dict]:
    """
    Checks if a specific IPFS peer has pinned a CID by verifying a random block's presence.

    Args:
        cids (List[str]): List of Content Identifiers to check.
        peer_id (str): The Peer ID of the miner to check.
        api_url (str): The IPFS HTTP API endpoint (default: local node).

    Returns:
        List[Dict]: List of results with 'cid', 'has_block', 'block_cid', and 'error' keys.
    """
    # Test node connectivity
    logger.debug("Testing IPFS node connectivity at %s", api_url)
    if not await test_node_connectivity(api_url):
        raise RuntimeError("IPFS node is not reachable")

    # Attempt to find and connect to the peer
    logger.debug("Attempting to find multiaddress for peer %s", peer_id)
    peer_multiaddr = await find_peer_multiaddr(api_url, peer_id)
    connected_peers = []
    peers_data = None
    async with aiohttp.ClientSession() as session:
        if peer_multiaddr:
            logger.debug("Connecting to peer %s", peer_multiaddr)
            await connect_to_peer(api_url, peer_multiaddr)
        else:
            logger.warning("Could not find multiaddress for peer %s", peer_id)

        # Check connected peers
        try:
            async with session.post(
                f"{api_url}/api/v0/swarm/peers",
                timeout=aiohttp.ClientTimeout(total=5)
            ) as peers_resp:
                logger.debug("Swarm peers response status: %s", peers_resp.status)
                if peers_resp.status == 200:
                    peers_data = await peers_resp.json()
                    connected_peers = [peer['Peer'] for peer in peers_data.get('Peers', [])]
                    logger.debug("Connected peers: %s", connected_peers)
        except Exception as e:
            logger.error("Error checking swarm peers: %s", e)

    results = []
    async with aiohttp.ClientSession() as session:
        async def process_cid(cid: str) -> Dict:
            try:
                # Parse CID
                logger.debug("Parsing CID: %s", cid)
                try:
                    cid_obj = CID.decode(cid)
                    logger.debug("CID version: %s, codec: %s", cid_obj.version, cid_obj.codec)
                except ValueError as e:
                    logger.error("Invalid CID %s: %s", cid, e)
                    return {'cid': cid, 'has_block': False, 'block_cid': None, 'error': str(e)}

                # Get DAG references (block CIDs)
                refs = []
                if cid_obj.codec != 'raw':
                    logger.debug("Fetching DAG for CID: %s", cid)
                    try:
                        async with session.post(
                            f"{api_url}/api/v0/dag/get?arg={cid}",
                            data={'path': '/'},
                            timeout=aiohttp.ClientTimeout(total=5)
                        ) as resp:
                            logger.debug("DAG get response status for CID %s: %s", cid, resp.status)
                            if resp.status != 200:
                                error_text = await resp.text()
                                logger.error("DAG get failed for CID %s: %s - %s", cid, resp.status, error_text)
                                return {'cid': cid, 'has_block': False, 'block_cid': None, 'error': f"DAG get failed: {error_text}"}
                            content_type = resp.headers.get('Content-Type', '')
                            logger.debug("DAG get content-type for CID %s: %s", cid, content_type)
                            response_text = await resp.text()
                            logger.debug("DAG get response for CID %s: %s", cid, response_text[:200])
                            try:
                                data = json.loads(response_text)
                                logger.debug("DAG data for CID %s: %s", cid, data)
                                refs = [link['Cid']['/'] for link in data.get('Links', []) if 'Cid' in link]
                                logger.debug("Found %d block CIDs for CID %s: %s", len(refs), cid, refs)
                            except json.JSONDecodeError:
                                logger.error("DAG get returned non-JSON response for CID %s: %s", cid, response_text)
                                return {'cid': cid, 'has_block': False, 'block_cid': None, 'error': f"Non-JSON response: {response_text}"}
                    except asyncio.TimeoutError:
                        logger.error("Timeout fetching DAG for CID %s", cid)
                        return {'cid': cid, 'has_block': False, 'block_cid': None, 'error': "Timeout fetching DAG"}
                else:
                    logger.debug("CID %s is a raw block; using CID directly", cid)
                    refs = [cid]

                # If no refs, use the CID itself
                if not refs:
                    logger.debug("No linked blocks found for CID %s; using CID as block", cid)
                    refs = [cid]

                # Select a random block CID
                random_block_cid = random.choice(refs)
                logger.debug("Selected random block CID for %s: %s", cid, random_block_cid)

                # Check Bitswap ledger if peer is connected
                if peer_id in connected_peers:
                    logger.debug("Checking Bitswap ledger for block CID %s", random_block_cid)
                    has_block = await check_bitswap_ledger(api_url, random_block_cid, peer_id)
                    if has_block:
                        return {'cid': cid, 'has_block': True, 'block_cid': random_block_cid, 'error': None}

                # Fallback to block/stat if peer is connected and peers_data exists
                if peer_id in connected_peers and peers_data:
                    logger.debug("Falling back to block/stat for CID %s", cid)
                    has_block = await check_block_stat(api_url, random_block_cid, peer_id, peers_data)
                    return {'cid': cid, 'has_block': has_block, 'block_cid': random_block_cid, 'error': None}
                else:
                    logger.debug("Peer %s not connected for CID %s; assuming no block", peer_id, cid)
                    return {'cid': cid, 'has_block': False, 'block_cid': random_block_cid, 'error': "Peer not connected"}

            except Exception as e:
                logger.error("Error processing CID %s: %s", cid, e)
                return {'cid': cid, 'has_block': False, 'block_cid': None, 'error': str(e)}

        # Run CID checks in parallel
        tasks = [process_cid(cid) for cid in cids]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        return results

# Example usage
if __name__ == "__main__":
    async def main():
        try:
            logger.info("Starting check_random_block")
            cids = [
                "QmY6yUVTYpedYMmmFgSCdhQ1yQ7bjUyR8wZNa6oaMLJzsH",
                "QmfGWbetc9BfLCWLL4yXvaESdbwLXkS3CUHLApbmTrrDuz",
                "QmetMmUKFs3NPiKnxkMDGYKYGaCQBaWjoyrF5oRCn3os73"
            ]
            results = await check_random_block(
                cids=cids,
                peer_id="12D3KooWEicpP6sEUkd3tJYKMAU4qHFJ2RWvxg5NViC5QCVK6wef",
                api_url="http://127.0.0.1:5001"
            )
            for result in results:
                logger.info("CID: %s, Block CID: %s, Peer has block: %s, Error: %s",
                            result['cid'], result['block_cid'], result['has_block'], result['error'])
        except Exception as e:
            logger.error("Error in main: %s", e)

    asyncio.run(main())