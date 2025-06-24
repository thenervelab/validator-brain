-- name: update_latest_block
-- Insert the latest block number and hash
INSERT INTO latest_block (block_number, block_hash, updated_at)
VALUES ($1, $2, NOW());