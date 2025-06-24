-- migrate:up
ALTER TABLE pending_miner_profile 
ADD COLUMN block_number BIGINT DEFAULT 0;

-- Create index for the new column
CREATE INDEX idx_pending_miner_profile_block_number ON pending_miner_profile(block_number);

-- migrate:down
ALTER TABLE pending_miner_profile 
DROP COLUMN IF EXISTS block_number; 