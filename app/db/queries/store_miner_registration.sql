-- name: store_miner_registration
-- Store or update miner registration information
INSERT INTO registration (node_id, ipfs_peer_id, registered_at, node_type, owner_account, status, updated_at)
VALUES ($1, $2, $3, $4, $5, 'active', NOW())
ON CONFLICT (node_id) 
DO UPDATE SET 
    ipfs_peer_id = $2,
    registered_at = $3,
    node_type = $4,
    owner_account = $5,
    updated_at = NOW();