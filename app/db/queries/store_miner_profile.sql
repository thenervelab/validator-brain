-- name: store_miner_profile
-- Store or update miner profile information
INSERT INTO miner_profile (miner_node_id, file_hash, file_size_bytes, created_at, selected_validator, updated_at)
VALUES ($1, $2, $3, $4, $5, NOW())
ON CONFLICT (miner_node_id, file_hash) 
DO UPDATE SET 
    file_size_bytes = $3,
    selected_validator = $5,
    updated_at = NOW();