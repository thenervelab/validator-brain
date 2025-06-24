-- migrate:up
-- Create table to track file failures and miner unavailability
CREATE TABLE file_failures (
    id SERIAL PRIMARY KEY,
    cid VARCHAR(255) NOT NULL,
    miner_id VARCHAR(255) NOT NULL,
    epoch INTEGER NOT NULL,
    failure_type VARCHAR(50) NOT NULL, -- 'ping_failed', 'pin_failed', 'not_provider'
    failure_reason TEXT,
    detected_at TIMESTAMP DEFAULT NOW(),
    resolved_at TIMESTAMP NULL,
    retry_count INTEGER DEFAULT 0,
    
    -- Unique constraint to prevent duplicate failure records
    CONSTRAINT unique_failure UNIQUE (cid, miner_id, epoch, failure_type)
);

-- Create indexes for file_failures
CREATE INDEX idx_file_failures_cid ON file_failures (cid);
CREATE INDEX idx_file_failures_miner ON file_failures (miner_id);
CREATE INDEX idx_file_failures_epoch ON file_failures (epoch);
CREATE INDEX idx_file_failures_type ON file_failures (failure_type);
CREATE INDEX idx_file_failures_detected ON file_failures (detected_at);

-- Create table to track miner availability scores
CREATE TABLE miner_availability (
    miner_id VARCHAR(255) PRIMARY KEY,
    total_files_assigned INTEGER DEFAULT 0,
    successful_checks INTEGER DEFAULT 0,
    failed_checks INTEGER DEFAULT 0,
    availability_score DECIMAL(5,4) DEFAULT 1.0000, -- 0.0000 to 1.0000
    last_successful_check TIMESTAMP NULL,
    last_failed_check TIMESTAMP NULL,
    consecutive_failures INTEGER DEFAULT 0,
    is_active BOOLEAN DEFAULT TRUE,
    updated_at TIMESTAMP DEFAULT NOW()
);

-- Create indexes for miner_availability
CREATE INDEX idx_miner_availability_score ON miner_availability (availability_score);
CREATE INDEX idx_miner_availability_active ON miner_availability (is_active);
CREATE INDEX idx_miner_availability_updated ON miner_availability (updated_at);

-- Create trigger to update miner_availability.updated_at
CREATE TRIGGER update_miner_availability_updated_at
    BEFORE UPDATE ON miner_availability
    FOR EACH ROW
    EXECUTE FUNCTION update_updated_at_column();

-- migrate:down
DROP TRIGGER IF EXISTS update_miner_availability_updated_at;
DROP TABLE IF EXISTS miner_availability;
DROP TABLE IF EXISTS file_failures; 