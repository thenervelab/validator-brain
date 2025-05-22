-- name: get_storage_miners
SELECT r.node_id, r.ipfs_peer_id, 
       m.storage_capacity_bytes, 
       m.available_space_bytes, 
       m.total_files_pinned, 
       m.total_files_size_bytes, 
       CASE WHEN m.last_online_block IS NOT NULL THEN true ELSE false END as is_online,
       m.health_score
FROM registration r
JOIN miner_stats m ON r.node_id = m.node_id
WHERE r.node_type = 'StorageMiner';