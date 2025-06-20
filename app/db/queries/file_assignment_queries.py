"""Queries for file assignment operations."""

from typing import List, Dict, Any, Optional
from asyncpg import Connection


async def get_files_by_offline_miner(
    conn: Connection,
    miner_node_id: str,
) -> List[Dict[str, Any]]:
    """
    Get all files hosted by a specific miner with file details.
    Useful when a miner goes offline and we need to reassign their files.
    
    Args:
        conn: Database connection
        miner_node_id: Node ID of the offline miner
        
    Returns:
        List of file assignments with file details
    """
    query = """
        SELECT 
            fa.*,
            f.name as file_name,
            f.size as file_size,
            f.created_date as file_created_date,
            -- Count how many miners are assigned to this file
            (CASE WHEN fa.miner1 IS NOT NULL THEN 1 ELSE 0 END +
             CASE WHEN fa.miner2 IS NOT NULL THEN 1 ELSE 0 END +
             CASE WHEN fa.miner3 IS NOT NULL THEN 1 ELSE 0 END +
             CASE WHEN fa.miner4 IS NOT NULL THEN 1 ELSE 0 END +
             CASE WHEN fa.miner5 IS NOT NULL THEN 1 ELSE 0 END) as total_miners,
            -- Get position of the offline miner
            CASE 
                WHEN fa.miner1 = $1 THEN 1
                WHEN fa.miner2 = $1 THEN 2
                WHEN fa.miner3 = $1 THEN 3
                WHEN fa.miner4 = $1 THEN 4
                WHEN fa.miner5 = $1 THEN 5
            END as miner_position
        FROM file_assignments fa
        JOIN files f ON fa.cid = f.cid
        WHERE fa.miner1 = $1 OR fa.miner2 = $1 OR fa.miner3 = $1 OR fa.miner4 = $1 OR fa.miner5 = $1
        ORDER BY fa.updated_at DESC
    """
    
    rows = await conn.fetch(query, miner_node_id)
    return [dict(row) for row in rows]


async def get_files_needing_replication(
    conn: Connection,
    min_replicas: int = 5,
) -> List[Dict[str, Any]]:
    """
    Get files that have fewer than the minimum required replicas.
    
    Args:
        conn: Database connection
        min_replicas: Minimum number of replicas required
        
    Returns:
        List of files needing more replicas
    """
    query = """
        SELECT 
            fa.*,
            f.name as file_name,
            f.size as file_size,
            (CASE WHEN fa.miner1 IS NOT NULL THEN 1 ELSE 0 END +
             CASE WHEN fa.miner2 IS NOT NULL THEN 1 ELSE 0 END +
             CASE WHEN fa.miner3 IS NOT NULL THEN 1 ELSE 0 END +
             CASE WHEN fa.miner4 IS NOT NULL THEN 1 ELSE 0 END +
             CASE WHEN fa.miner5 IS NOT NULL THEN 1 ELSE 0 END) as current_replicas
        FROM file_assignments fa
        JOIN files f ON fa.cid = f.cid
        WHERE (CASE WHEN fa.miner1 IS NOT NULL THEN 1 ELSE 0 END +
               CASE WHEN fa.miner2 IS NOT NULL THEN 1 ELSE 0 END +
               CASE WHEN fa.miner3 IS NOT NULL THEN 1 ELSE 0 END +
               CASE WHEN fa.miner4 IS NOT NULL THEN 1 ELSE 0 END +
               CASE WHEN fa.miner5 IS NOT NULL THEN 1 ELSE 0 END) < $1
        ORDER BY current_replicas ASC, fa.updated_at DESC
    """
    
    rows = await conn.fetch(query, min_replicas)
    return [dict(row) for row in rows]


async def remove_miner_from_assignments(
    conn: Connection,
    miner_node_id: str,
) -> int:
    """
    Remove a miner from all file assignments (e.g., when they go offline).
    
    Args:
        conn: Database connection
        miner_node_id: Node ID of the miner to remove
        
    Returns:
        Number of assignments updated
    """
    query = """
        UPDATE file_assignments
        SET 
            miner1 = CASE WHEN miner1 = $1 THEN NULL ELSE miner1 END,
            miner2 = CASE WHEN miner2 = $1 THEN NULL ELSE miner2 END,
            miner3 = CASE WHEN miner3 = $1 THEN NULL ELSE miner3 END,
            miner4 = CASE WHEN miner4 = $1 THEN NULL ELSE miner4 END,
            miner5 = CASE WHEN miner5 = $1 THEN NULL ELSE miner5 END
        WHERE miner1 = $1 OR miner2 = $1 OR miner3 = $1 OR miner4 = $1 OR miner5 = $1
        RETURNING id
    """
    
    result = await conn.fetch(query, miner_node_id)
    return len(result)


async def get_miner_file_count(
    conn: Connection,
    miner_node_id: str,
) -> Dict[str, int]:
    """
    Get statistics about files hosted by a miner.
    
    Args:
        conn: Database connection
        miner_node_id: Node ID of the miner
        
    Returns:
        Dictionary with file count and total size
    """
    query = """
        SELECT 
            COUNT(DISTINCT fa.cid) as file_count,
            COALESCE(SUM(DISTINCT f.size), 0) as total_size
        FROM file_assignments fa
        JOIN files f ON fa.cid = f.cid
        WHERE fa.miner1 = $1 OR fa.miner2 = $1 OR fa.miner3 = $1 OR fa.miner4 = $1 OR fa.miner5 = $1
    """
    
    row = await conn.fetchrow(query, miner_node_id)
    return {
        "file_count": row["file_count"],
        "total_size": row["total_size"]
    }


async def reassign_miner_slot(
    conn: Connection,
    cid: str,
    old_miner: str,
    new_miner: str,
) -> bool:
    """
    Replace a specific miner with a new one in a file assignment.
    
    Args:
        conn: Database connection
        cid: Content ID of the file
        old_miner: Node ID of the miner to replace
        new_miner: Node ID of the new miner
        
    Returns:
        True if successful, False otherwise
    """
    query = """
        UPDATE file_assignments
        SET 
            miner1 = CASE WHEN miner1 = $2 THEN $3 ELSE miner1 END,
            miner2 = CASE WHEN miner2 = $2 THEN $3 ELSE miner2 END,
            miner3 = CASE WHEN miner3 = $2 THEN $3 ELSE miner3 END,
            miner4 = CASE WHEN miner4 = $2 THEN $3 ELSE miner4 END,
            miner5 = CASE WHEN miner5 = $2 THEN $3 ELSE miner5 END
        WHERE cid = $1 AND (miner1 = $2 OR miner2 = $2 OR miner3 = $2 OR miner4 = $2 OR miner5 = $2)
        RETURNING id
    """
    
    result = await conn.fetchrow(query, cid, old_miner, new_miner)
    return result is not None 