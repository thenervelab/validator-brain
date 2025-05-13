# substrate_fetcher/utils.py

import asyncio
import asyncpg
import json
import multiprocessing as mp
import queue  # Import the queue module for QueueEmpty exception
import signal
import sys
from typing import Any, Dict, List, Tuple
from . import config

# --- Database Configuration ---
async def create_db_pool():
    """Creates and returns a connection pool to the PostgreSQL database."""
    try:
        pool = await asyncpg.create_pool(config.DATABASE_URL)
        print("Successfully created database connection pool.")
        return pool
    except Exception as e:
        print(f"Failed to create database connection pool: {e}")
        raise

async def init_db(pool: asyncpg.Pool):
    """Initializes the database by creating necessary tables if they don't exist."""
    async with pool.acquire() as conn:
        # Create miners table
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS miners (
                node_id VARCHAR(100) PRIMARY KEY,
                ipfs_storage_max BIGINT,
                ipfs_zfs_pool_size BIGINT,
                last_online_block INTEGER,
                miner_profile_cid VARCHAR(255),
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """)

        # Create registration table
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS registration (
                node_id VARCHAR(100) PRIMARY KEY,
                ipfs_node_id VARCHAR(54) NOT NULL,
                node_type VARCHAR(50) NOT NULL,
                owner VARCHAR(50) NOT NULL,
                registered_at INTEGER NOT NULL,
                status VARCHAR(20) NOT NULL,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """)
        print("Database tables initialized.")

# --- Database Operations for Fetcher ---
async def clear_table(pool: asyncpg.Pool, table_name: str):
    """Clears all data from the specified table."""
    async with pool.acquire() as conn:
        await conn.execute(f"DELETE FROM {table_name};")
        print(f"Cleared table: {table_name}")

async def update_execution_unit_metrics(pool: asyncpg.Pool, metrics_data: Dict[str, Dict[str, int]]):
    """
    Updates or inserts ExecutionUnit.NodeMetrics data into the miners table.
    If the node_id exists, updates the metrics; otherwise, inserts a new row.
    """
    async with pool.acquire() as conn:
        async with conn.transaction():
            for node_id, metrics in metrics_data.items():
                ipfs_storage_max = metrics.get("ipfs_storage_max", None)
                ipfs_zfs_pool_size = metrics.get("ipfs_zfs_pool_size", None)

                # Check if the node_id exists
                exists = await conn.fetchval(
                    "SELECT 1 FROM miners WHERE node_id = $1", node_id
                )

                if exists:
                    # Update existing row
                    await conn.execute(
                        """
                        UPDATE miners
                        SET ipfs_storage_max = $2,
                            ipfs_zfs_pool_size = $3,
                            updated_at = CURRENT_TIMESTAMP
                        WHERE node_id = $1;
                        """,
                        node_id, ipfs_storage_max, ipfs_zfs_pool_size
                    )
                    print(f"Updated ExecutionUnit metrics for node_id: {node_id}")
                else:
                    # Insert new row (with only metrics for now)
                    await conn.execute(
                        """
                        INSERT INTO miners (node_id, ipfs_storage_max, ipfs_zfs_pool_size)
                        VALUES ($1, $2, $3)
                        ON CONFLICT (node_id) DO UPDATE
                        SET ipfs_storage_max = EXCLUDED.ipfs_storage_max,
                            ipfs_zfs_pool_size = EXCLUDED.ipfs_zfs_pool_size,
                            updated_at = CURRENT_TIMESTAMP;
                        """,
                        node_id, ipfs_storage_max, ipfs_zfs_pool_size
                    )
                    print(f"Inserted new ExecutionUnit metrics for node_id: {node_id}")

async def save_miners_data(pool: asyncpg.Pool, block_numbers: Dict[str, List[int]], miner_profiles: Dict[str, str]):
    """
    Saves BlockNumbers and MinerProfile data into the miners table.
    Deletes previous data for these fields (but preserves ExecutionUnit metrics).
    """
    async with pool.acquire() as conn:
        async with conn.transaction():
            # Step 1: Clear previous block numbers and miner profiles (set to NULL)
            await conn.execute(
                """
                UPDATE miners
                SET last_online_block = NULL,
                    miner_profile_cid = NULL,
                    updated_at = CURRENT_TIMESTAMP;
                """
            )
            print("Cleared previous BlockNumbers and MinerProfile data from miners table.")

            # Step 2: Batch insert or update BlockNumbers and MinerProfile data
            inserts = []
            for node_id in set(list(block_numbers.keys()) + list(miner_profiles.keys())):
                last_online_block = block_numbers.get(node_id, [None])[0] if node_id in block_numbers else None
                miner_profile_cid = miner_profiles.get(node_id, None)

                inserts.append(
                    conn.execute(
                        """
                        INSERT INTO miners (node_id, last_online_block, miner_profile_cid)
                        VALUES ($1, $2, $3)
                        ON CONFLICT (node_id) DO UPDATE
                        SET last_online_block = EXCLUDED.last_online_block,
                            miner_profile_cid = EXCLUDED.miner_profile_cid,
                            updated_at = CURRENT_TIMESTAMP;
                        """,
                        node_id, last_online_block, miner_profile_cid
                    )
                )
                print(f"Inserted/Updated miners data for node_id: {node_id}")

            # Execute all inserts concurrently
            if inserts:
                await asyncio.gather(*inserts)
                
async def save_registration_data(pool: asyncpg.Pool, node_registration: Dict, coldkey_registration: Dict):
    """Saves NodeRegistration and ColdkeyNodeRegistration data into the registration table."""
    async with pool.acquire() as conn:
        async with conn.transaction():
            # Step 1: Clear previous registration data
            await clear_table(pool, "registration")

            # Step 2: Combine both registration datasets
            all_registrations = {**node_registration, **coldkey_registration}

            # Step 3: Insert new registration data
            for node_id, data in all_registrations.items():
                try:
                    await conn.execute(
                        """
                        INSERT INTO registration (node_id, ipfs_node_id, node_type, owner, registered_at, status)
                        VALUES ($1, $2, $3, $4, $5, $6)
                        ON CONFLICT (node_id) DO UPDATE
                        SET ipfs_node_id = EXCLUDED.ipfs_node_id,
                            node_type = EXCLUDED.node_type,
                            owner = EXCLUDED.owner,
                            registered_at = EXCLUDED.registered_at,
                            status = EXCLUDED.status,
                            updated_at = CURRENT_TIMESTAMP;
                        """,
                        node_id,
                        data["ipfs_node_id"],
                        data["node_type"],
                        data["owner"],
                        data["registered_at"],
                        data["status"]
                    )
                    print(f"Saved registration data for node_id: {node_id}")
                except Exception as e:
                    print(f"Error saving registration data for node_id {node_id}: {e}")

# --- IPFS Fetch Worker ---
def ipfs_fetch_worker(queue: Any, shared_content: Any):
    """Worker process to fetch IPFS content for CIDs."""
    import requests

    def signal_handler(sig, frame):
        print(f"IPFS fetch worker received signal {sig}, shutting down gracefully...")
        sys.exit(0)

    signal.signal(signal.SIGTERM, signal_handler)
    signal.signal(signal.SIGINT, signal_handler)

    while True:
        try:
            item = queue.get(timeout=10)  # Add timeout to prevent hanging
            if item is None:  # Sentinel value to stop the worker
                print("IPFS fetch worker received stop signal.")
                break

            cids_to_fetch, all_node_ids = item
            print(f"IPFS fetch worker processing {len(cids_to_fetch)} CIDs...")

            # Remove content for node_ids that no longer exist
            node_ids_to_remove = set(shared_content.keys()) - set(all_node_ids)
            for node_id in node_ids_to_remove:
                if node_id in shared_content:
                    del shared_content[node_id]
                    print(f"Removed IPFS content for node_id: {node_id}")

            # Fetch content for new or changed CIDs
            for node_id, cid in cids_to_fetch:
                try:
                    if not cid:
                        shared_content[node_id] = None
                        print(f"No CID for node_id {node_id}, setting content to None.")
                        continue

                    # Try fetching via IPFS gateway
                    gateway_url = f"{config.IPFS_GATEWAY_URL}/ipfs/{cid}"
                    response = requests.get(gateway_url, timeout=config.IPFS_TIMEOUT_SECONDS)
                    if response.status_code == 200:
                        try:
                            content = response.json()
                            shared_content[node_id] = content
                            print(f"Fetched IPFS content for node_id {node_id} (CID: {cid})")
                        except ValueError:
                            content = response.text
                            shared_content[node_id] = content
                            print(f"Fetched IPFS content (non-JSON) for node_id {node_id} (CID: {cid})")
                    else:
                        print(f"Failed to fetch CID {cid} for node_id {node_id}: HTTP {response.status_code}")
                        shared_content[node_id] = None

                except requests.Timeout:
                    print(f"Timeout while fetching CID {cid} for node_id {node_id}")
                    shared_content[node_id] = None
                except Exception as e:
                    print(f"Error fetching CID {cid} for node_id {node_id}: {e}")
                    shared_content[node_id] = None

        except queue.Empty:  # Correct exception for queue timeout
            print("IPFS fetch worker queue empty, waiting...")
            continue
        except BrokenPipeError as e:
            print(f"Error in IPFS fetch worker: [Errno 32] Broken pipe - {e}. Attempting to recover...")
            continue
        except Exception as e:
            print(f"Error in IPFS fetch worker: {e}")
            continue
    print("IPFS fetch worker stopped.")

# --- Utility Functions ---
def get_storage_key_string(module: str, storage_item: str, params: Any = None) -> str:
    """Generates a string representation of a storage key for consistent JSON keys."""
    if params is None:
        return f"{module}.{storage_item}"
    if isinstance(params, (str, int)):
        return f"{module}.{storage_item}({params})"
    if isinstance(params, bytes):
        return f"{module}.{storage_item}(0x{params.hex()})"
    if isinstance(params, (list, tuple)):
        params_str = ",".join([f"0x{p.hex()}" if isinstance(p, bytes) else str(p) for p in params])
        return f"{module}.{storage_item}({params_str})"
    return f"{module}.{storage_item}({str(params)})"