-- name: local_miner_profiles
SELECT DISTINCT 
    r.node_id, 
    r.ipfs_peer_id,
    CASE 
        WHEN ms.last_online_block IS NOT NULL AND 
             ms.last_online_block >= (SELECT MAX(block_number) FROM latest_block) - 100
        THEN true 
        ELSE false 
    END as is_online,
    COALESCE(ms.health_score, 0) as health_score,
    COALESCE(ms.storage_capacity_bytes, 0) as storage_capacity_bytes,
    COALESCE(ms.total_files_pinned, 0) as total_files_pinned,
    COALESCE(ms.total_files_size_bytes, 0) as total_files_size_bytes
FROM registration r
LEFT JOIN miner_stats ms ON r.node_id = ms.node_id
WHERE r.node_type = 'miner'