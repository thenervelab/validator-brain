-- migrate:up
CREATE TABLE pending_user_profile (
    id SERIAL PRIMARY KEY,
    cid VARCHAR(255) NOT NULL,
    owner VARCHAR(255) NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    published_at TIMESTAMP,
    status VARCHAR(50) DEFAULT 'pending',
    error_message TEXT,
    files_count INTEGER DEFAULT 0,
    files_size BIGINT DEFAULT 0,
    block_number INTEGER DEFAULT 0
);

-- Create indexes for better query performance
CREATE INDEX idx_pending_user_profile_owner ON pending_user_profile(owner);
CREATE INDEX idx_pending_user_profile_status ON pending_user_profile(status);
CREATE INDEX idx_pending_user_profile_created_at ON pending_user_profile(created_at);

-- migrate:down
DROP TABLE IF EXISTS pending_user_profile; 