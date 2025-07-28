-- migrate:up
ALTER TABLE pending_miner_profile 
DROP CONSTRAINT IF EXISTS pending_miner_profile_status_check;

ALTER TABLE pending_miner_profile 
ADD CONSTRAINT pending_miner_profile_status_check 
CHECK (status IN ('pending', 'published', 'failed', 'submitted', 'needs_reconstruction'));

-- migrate:down
ALTER TABLE pending_miner_profile 
DROP CONSTRAINT IF EXISTS pending_miner_profile_status_check;

ALTER TABLE pending_miner_profile 
ADD CONSTRAINT pending_miner_profile_status_check 
CHECK (status IN ('pending', 'published', 'failed', 'submitted'));