-- migrate:up
CREATE TABLE IF NOT EXISTS pending_miner_profile (
    id SERIAL PRIMARY KEY,
    cid TEXT NOT NULL UNIQUE,
    node_id TEXT NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    published_at TIMESTAMP WITH TIME ZONE,
    status TEXT DEFAULT 'pending' CHECK (status IN ('pending', 'published', 'failed')),
    error_message TEXT
);

-- Create indexes
CREATE INDEX idx_pending_miner_profile_node_id ON pending_miner_profile(node_id);
CREATE INDEX idx_pending_miner_profile_status ON pending_miner_profile(status);
CREATE INDEX idx_pending_miner_profile_created_at ON pending_miner_profile(created_at);

-- migrate:down
DROP TABLE IF EXISTS pending_miner_profile; 