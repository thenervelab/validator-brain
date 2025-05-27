-- migrate:up
CREATE TABLE pending_assignment_file (
    id SERIAL PRIMARY KEY,
    cid VARCHAR(255) NOT NULL UNIQUE,
    owner VARCHAR(255) NOT NULL,
    filename VARCHAR(500),
    file_size_bytes BIGINT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    processed_at TIMESTAMP,
    status VARCHAR(50) DEFAULT 'pending',
    error_message TEXT
);

-- Create indexes for better query performance
CREATE INDEX idx_pending_assignment_file_owner ON pending_assignment_file(owner);
CREATE INDEX idx_pending_assignment_file_status ON pending_assignment_file(status);
CREATE INDEX idx_pending_assignment_file_created_at ON pending_assignment_file(created_at);

-- migrate:down
DROP TABLE IF EXISTS pending_assignment_file; 