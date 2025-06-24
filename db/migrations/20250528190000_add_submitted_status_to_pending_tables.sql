-- migrate:up
-- Add 'submitted' status to pending profile tables

-- Update pending_miner_profile status constraint
ALTER TABLE pending_miner_profile 
DROP CONSTRAINT IF EXISTS pending_miner_profile_status_check;

ALTER TABLE pending_miner_profile 
ADD CONSTRAINT pending_miner_profile_status_check 
CHECK (status IN ('pending', 'published', 'failed', 'submitted'));

-- Update pending_user_profile status constraint (if it exists)
ALTER TABLE pending_user_profile 
DROP CONSTRAINT IF EXISTS pending_user_profile_status_check;

ALTER TABLE pending_user_profile 
ADD CONSTRAINT pending_user_profile_status_check 
CHECK (status IN ('pending', 'published', 'failed', 'submitted'));

-- migrate:down
-- Revert to original constraints

ALTER TABLE pending_miner_profile 
DROP CONSTRAINT IF EXISTS pending_miner_profile_status_check;

ALTER TABLE pending_miner_profile 
ADD CONSTRAINT pending_miner_profile_status_check 
CHECK (status IN ('pending', 'published', 'failed'));

ALTER TABLE pending_user_profile 
DROP CONSTRAINT IF EXISTS pending_user_profile_status_check;

ALTER TABLE pending_user_profile 
ADD CONSTRAINT pending_user_profile_status_check 
CHECK (status IN ('pending', 'published', 'failed')); 