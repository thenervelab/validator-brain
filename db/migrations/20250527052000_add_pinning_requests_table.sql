-- migrate:up
CREATE TABLE IF NOT EXISTS pinning_requests (
    id SERIAL PRIMARY KEY,
    request_hash VARCHAR(255) NOT NULL UNIQUE, -- The hex hash from the storage key
    owner VARCHAR(255) NOT NULL, -- SS58 address of the owner
    file_hash VARCHAR(255) NOT NULL, -- The file hash (CID)
    file_name VARCHAR(255),
    total_replicas INTEGER DEFAULT 0,
    is_assigned BOOLEAN DEFAULT FALSE,
    selected_validator VARCHAR(255),
    created_at BIGINT, -- Block number when created
    last_charged_at BIGINT, -- Block number when last charged
    processed_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Create indexes for better query performance
CREATE INDEX idx_pinning_requests_owner ON pinning_requests(owner);
CREATE INDEX idx_pinning_requests_file_hash ON pinning_requests(file_hash);
CREATE INDEX idx_pinning_requests_request_hash ON pinning_requests(request_hash);
CREATE INDEX idx_pinning_requests_is_assigned ON pinning_requests(is_assigned);
CREATE INDEX idx_pinning_requests_selected_validator ON pinning_requests(selected_validator);

-- Create trigger to update updated_at timestamp
CREATE OR REPLACE FUNCTION update_pinning_requests_updated_at()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER update_pinning_requests_updated_at_trigger
    BEFORE UPDATE ON pinning_requests
    FOR EACH ROW
    EXECUTE FUNCTION update_pinning_requests_updated_at();

-- Table to track processed request hashes
CREATE TABLE IF NOT EXISTS processed_pinning_requests (
    id SERIAL PRIMARY KEY,
    request_hash VARCHAR(255) NOT NULL UNIQUE,
    processed_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    miner_count INTEGER DEFAULT 0 -- Number of miners assigned at time of processing
);

CREATE INDEX idx_processed_pinning_requests_hash ON processed_pinning_requests(request_hash);

-- migrate:down
DROP TRIGGER IF EXISTS update_pinning_requests_updated_at_trigger ON pinning_requests;
DROP FUNCTION IF EXISTS update_pinning_requests_updated_at();
DROP TABLE IF EXISTS processed_pinning_requests;
DROP TABLE IF EXISTS pinning_requests; 