-- migrate:up
ALTER TABLE pending_miner_profile 
ADD COLUMN files_count INTEGER DEFAULT 0,
ADD COLUMN files_size BIGINT DEFAULT 0;

-- Create indexes for the new columns
CREATE INDEX idx_pending_miner_profile_files_count ON pending_miner_profile(files_count);
CREATE INDEX idx_pending_miner_profile_files_size ON pending_miner_profile(files_size);

-- migrate:down
ALTER TABLE pending_miner_profile 
DROP COLUMN IF EXISTS files_count,
DROP COLUMN IF EXISTS files_size; 