-- migrate:up
-- Add processed_unpin_requests table to track processed unpin requests and avoid duplicates

CREATE TABLE IF NOT EXISTS processed_unpin_requests
(
    id             SERIAL PRIMARY KEY,
    request_id     VARCHAR(255) UNIQUE NOT NULL,
    owner          VARCHAR(100)        NOT NULL,
    file_hash      VARCHAR(255),
    files_unpinned INTEGER   DEFAULT 0,
    created_at     TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create indexes for better query performance
CREATE INDEX IF NOT EXISTS idx_processed_unpin_requests_request_id ON processed_unpin_requests (request_id);
CREATE INDEX IF NOT EXISTS idx_processed_unpin_requests_owner ON processed_unpin_requests (owner);

-- migrate:down
DROP TABLE IF EXISTS processed_unpin_requests;