"""Bulk processing utilities for large scale operations.

This module provides utilities for handling large scale operations,
particularly for scenarios with 1000+ files and miners.
"""

from datetime import datetime
from typing import Any, Callable, Dict, List

from app.utils.logging import logger


async def process_in_chunks(
    items: List[Any], process_func: Callable, chunk_size: int = 100, **kwargs,
) -> List[Any]:
    """
    Process a large list of items in chunks.

    Args:
        items: List of items to process
        process_func: Function to process each chunk
        chunk_size: Number of items per chunk
        **kwargs: Additional keyword arguments for the process function

    Returns:
        Combined results from all chunks
    """
    results = []
    total_items = len(items)

    # Process in chunks
    for i in range(0, total_items, chunk_size):
        chunk = items[i : i + chunk_size]
        chunk_result = await process_func(chunk, **kwargs)
        results.extend(chunk_result)

        # Log progress
        processed = min(i + chunk_size, total_items)
        logger.info(f"Processed {processed}/{total_items} items ({processed / total_items:.1%})")

    return results


async def bulk_db_insert(
    db_pool, table_name: str, records: List[Dict], batch_size: int = 1000, upsert: bool = False,
) -> int:
    """
    Insert a large number of records into a database table efficiently.

    Args:
        db_pool: Database connection pool
        table_name: Name of the table to insert into
        records: List of record dictionaries
        batch_size: Number of records per batch
        upsert: Whether to use upsert instead of insert

    Returns:
        Total number of records inserted
    """
    if not records:
        return 0

    # Get column names from the first record
    columns = list(records[0].keys())

    # Build the base query
    if upsert:
        # For upsert, we need primary key information
        # This is a simplified version, in practice you'd need proper conflict target
        base_query = f"""
        INSERT INTO {table_name} ({", ".join(columns)})
        VALUES %s
        ON CONFLICT (id) DO UPDATE SET
        {", ".join([f"{col} = EXCLUDED.{col}" for col in columns if col != "id"])}
        """
    else:
        base_query = f"""
        INSERT INTO {table_name} ({", ".join(columns)})
        VALUES %s
        """

    # Insert in batches
    total_inserted = 0

    async with db_pool.acquire() as conn:
        # Start a transaction
        async with conn.transaction():
            for i in range(0, len(records), batch_size):
                batch = records[i : i + batch_size]

                # Convert batch to list of tuples for psycopg2 execute_values
                values = []
                for record in batch:
                    row = tuple(record.get(col) for col in columns)
                    values.append(row)

                # Use execute_values for efficient bulk insert
                # Note: For asyncpg, we need to construct the full query with values
                placeholders = []
                for j in range(len(batch)):
                    placeholders.append(
                        f"({', '.join([f'${j * len(columns) + k + 1}' for k in range(len(columns))])})",
                    )

                full_query = base_query.replace("%s", ", ".join(placeholders))

                # Flatten values for execution
                flat_values = []
                for row in values:
                    flat_values.extend(row)

                # Execute the query
                await conn.execute(full_query, *flat_values)

                total_inserted += len(batch)
                logger.info(f"Inserted {total_inserted}/{len(records)} records into {table_name}")

    return total_inserted


async def bulk_update_miner_profiles(db_pool, miner_profiles: List[Dict]) -> int:
    """
    Update a large number of miner profiles efficiently.

    Args:
        db_pool: Database connection pool
        miner_profiles: List of miner profile dictionaries

    Returns:
        Number of profiles updated
    """
    if not miner_profiles:
        return 0

    updated_count = 0
    start_time = datetime.now()

    # Group profiles by miner_id for efficient updating
    profiles_by_miner = {}
    for profile in miner_profiles:
        miner_id = profile["node_id"]
        if miner_id not in profiles_by_miner:
            profiles_by_miner[miner_id] = []
        profiles_by_miner[miner_id].append(profile)

    # Update each miner's profiles
    async with db_pool.acquire() as conn:
        async with conn.transaction():
            for miner_id, profiles in profiles_by_miner.items():
                # First, clear existing profiles for this miner
                await conn.execute("DELETE FROM miner_profile WHERE node_id = $1", miner_id)

                # Then insert new profiles
                for profile in profiles:
                    await conn.execute(
                        """
                        INSERT INTO miner_profile 
                        (node_id, ipfs_peer_id, profile_cid, is_online, health_score,
                         storage_capacity_bytes, total_files_pinned, total_files_size_bytes,
                         created_at, updated_at)
                        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, NOW(), NOW())
                        """,
                        miner_id,
                        profile.get("ipfs_peer_id", miner_id),
                        profile.get("profile_cid"),
                        profile.get("is_online", True),
                        profile.get("health_score", 100),
                        profile.get("storage_capacity_bytes", 1000000000),
                        profile.get("total_files_pinned", 0),
                        profile.get("total_files_size_bytes", 0),
                    )

                updated_count += len(profiles)

                # Log progress every 100 miners
                if updated_count % 100 == 0:
                    elapsed = (datetime.now() - start_time).total_seconds()
                    rate = updated_count / elapsed if elapsed > 0 else 0
                    logger.info(f"Updated {updated_count} profiles at {rate:.1f} profiles/sec")

    # Log final statistics
    elapsed = (datetime.now() - start_time).total_seconds()
    rate = updated_count / elapsed if elapsed > 0 else 0
    logger.info(
        f"Updated {updated_count} miner profiles in {elapsed:.1f} seconds ({rate:.1f} profiles/sec)",
    )

    return updated_count
