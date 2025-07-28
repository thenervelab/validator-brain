-- migrate:up
ALTER TABLE pending_miner_profile 
ADD CONSTRAINT pending_miner_profile_node_id_unique UNIQUE (node_id);

-- migrate:down
ALTER TABLE pending_miner_profile 
DROP CONSTRAINT IF EXISTS pending_miner_profile_node_id_unique;