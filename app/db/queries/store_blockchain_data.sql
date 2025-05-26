-- Store blockchain data regardless of validator status

-- name: update_latest_block
-- Update the latest processed block number and hash
INSERT INTO latest_block (block_number, block_hash, updated_at)
VALUES ($1, $2, NOW())
ON CONFLICT (id) DO UPDATE SET
  block_number = $1,
  block_hash = $2,
  updated_at = NOW();

-- name: store_miner_registration
-- Store miner registration data
INSERT INTO registration (node_id, ipfs_peer_id, registered_at, node_type, owner_account, status)
VALUES ($1, $2, $3, $4, $5, 'active')
ON CONFLICT (node_id) DO UPDATE SET
  ipfs_peer_id = $2,
  updated_at = NOW();

-- name: store_miner_profile
-- Store miner profile data including CID
INSERT INTO miner_profile (miner_node_id, file_hash, file_size_bytes, created_at, selected_validator, updated_at)
VALUES ($1, $2, $3, $4, $5, NOW())
ON CONFLICT (miner_node_id, file_hash) DO UPDATE SET
  file_size_bytes = $3,
  selected_validator = $5,
  updated_at = NOW();