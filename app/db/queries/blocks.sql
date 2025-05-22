-- name: store_block
INSERT INTO latest_block (block_number, block_hash)
VALUES ($1, $2)
ON CONFLICT (id) DO UPDATE
SET block_number = $1, block_hash = $2, updated_at = CURRENT_TIMESTAMP;

-- name: set_current_validator
INSERT INTO current_epoch_validator (epoch, validator_account, selected_at_block)
VALUES ($1, $2, $3)
ON CONFLICT (epoch) DO UPDATE
SET validator_account = $2, selected_at_block = $3, updated_at = CURRENT_TIMESTAMP;