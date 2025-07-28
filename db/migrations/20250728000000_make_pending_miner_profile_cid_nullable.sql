-- migrate:up
ALTER TABLE pending_miner_profile 
ALTER COLUMN cid DROP NOT NULL;

-- migrate:down  
ALTER TABLE pending_miner_profile 
ALTER COLUMN cid SET NOT NULL;