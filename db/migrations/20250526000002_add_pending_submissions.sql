-- migrate:up
-- Add pending_submissions table for blockchain transaction management

CREATE TABLE IF NOT EXISTS pending_submissions (
    id SERIAL PRIMARY KEY,
    submission_id VARCHAR(100) NOT NULL,
    node_id VARCHAR(100),
    owner_id VARCHAR(100),
    submission_type VARCHAR(50) NOT NULL,
    data JSONB NOT NULL,
    submitted BOOLEAN NOT NULL DEFAULT FALSE,
    submission_block BIGINT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    -- Foreign key constraints where applicable
    FOREIGN KEY (node_id) REFERENCES registration(node_id) ON DELETE SET NULL
);

-- Create indexes for performance
CREATE INDEX idx_pending_submissions_submitted ON pending_submissions(submitted);
CREATE INDEX idx_pending_submissions_type ON pending_submissions(submission_type);
CREATE INDEX idx_pending_submissions_node_id ON pending_submissions(node_id);
CREATE INDEX idx_pending_submissions_created_at ON pending_submissions(created_at);

-- Add trigger for automatic timestamp updates
CREATE TRIGGER update_pending_submissions_timestamp
BEFORE UPDATE ON pending_submissions
FOR EACH ROW EXECUTE FUNCTION update_timestamp();

-- migrate:down
DROP TRIGGER IF EXISTS update_pending_submissions_timestamp ON pending_submissions;
DROP INDEX IF EXISTS idx_pending_submissions_created_at;
DROP INDEX IF EXISTS idx_pending_submissions_node_id;
DROP INDEX IF EXISTS idx_pending_submissions_type;
DROP INDEX IF EXISTS idx_pending_submissions_submitted;
DROP TABLE IF EXISTS pending_submissions;