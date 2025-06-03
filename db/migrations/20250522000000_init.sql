-- migrate:up
-- IPFS Service Validator Database Schema

-- Create database tables with improved relationships
-- This schema reflects the requirements in the original project
-- but with some simplifications and improvements

-- Registration table: stores node registration information
CREATE TABLE IF NOT EXISTS registration (
    node_id VARCHAR(100) PRIMARY KEY,
    ipfs_peer_id VARCHAR(100) NOT NULL,  -- Changed from ipfs_node_id for clarity
    node_type VARCHAR(50) NOT NULL,
    owner_account VARCHAR(100) NOT NULL,
    registered_at BIGINT NOT NULL,
    status VARCHAR(20) NOT NULL DEFAULT 'active',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Latest block table: tracks the latest processed block
CREATE TABLE IF NOT EXISTS latest_block (
    id SERIAL PRIMARY KEY,
    block_number BIGINT NOT NULL,
    block_hash VARCHAR(100),
    epoch BIGINT GENERATED ALWAYS AS (
        CASE 
            WHEN block_number < 38 THEN 0
            ELSE (block_number - 38) / 100
        END
    ) STORED,  -- Auto-calculate epoch based on block (epochs start at blocks ending in 38)
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Current epoch validator table: tracks the validator for the current epoch
CREATE TABLE IF NOT EXISTS current_epoch_validator (
    epoch BIGINT PRIMARY KEY,
    validator_account VARCHAR(100) NOT NULL,
    selected_at_block BIGINT NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Miner epoch health table: tracks health metrics per miner per epoch
CREATE TABLE IF NOT EXISTS miner_epoch_health (
    node_id VARCHAR(100),
    ipfs_peer_id VARCHAR(100) NOT NULL,
    epoch BIGINT,
    ping_successes INTEGER DEFAULT 0,
    ping_failures INTEGER DEFAULT 0,
    pin_check_successes INTEGER DEFAULT 0,
    pin_check_failures INTEGER DEFAULT 0,
    last_ping_attempt TIMESTAMP,
    last_ping_block BIGINT,
    last_pin_check_attempt TIMESTAMP,
    last_activity_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (node_id, epoch),
    FOREIGN KEY (node_id) REFERENCES registration(node_id) ON DELETE CASCADE
);

-- Miner profile table: stores miner profile data and file pins
CREATE TABLE IF NOT EXISTS miner_profile (
    id SERIAL PRIMARY KEY,
    miner_node_id VARCHAR(100) NOT NULL,
    file_hash VARCHAR(255) NOT NULL,
    file_size_bytes BIGINT NOT NULL,
    created_at BIGINT NOT NULL,
    selected_validator VARCHAR(100) NOT NULL,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(miner_node_id, file_hash),
    FOREIGN KEY (miner_node_id) REFERENCES registration(node_id) ON DELETE CASCADE
);

-- User profile table: stores user profile data
CREATE TABLE IF NOT EXISTS user_profile (
    id SERIAL PRIMARY KEY,
    user_id VARCHAR(100) NOT NULL,
    file_hash VARCHAR(255) NOT NULL,
    file_name VARCHAR(255) NOT NULL,
    file_size_bytes BIGINT NOT NULL,
    is_assigned BOOLEAN NOT NULL DEFAULT FALSE,
    last_charged_at BIGINT NOT NULL,
    created_at BIGINT NOT NULL,
    main_request_hash VARCHAR(255),
    miner_ids TEXT[] NOT NULL,
    owner_account VARCHAR(100) NOT NULL,
    selected_validator VARCHAR(100) NOT NULL,
    total_replicas INTEGER NOT NULL DEFAULT 1,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(user_id, file_hash)
);

-- Storage requests table: tracks pending storage requests
CREATE TABLE IF NOT EXISTS storage_requests (
    id SERIAL PRIMARY KEY,
    owner_account VARCHAR(100) NOT NULL,
    file_hash VARCHAR(255) NOT NULL,
    file_name VARCHAR(255) NOT NULL,
    file_size_bytes BIGINT NOT NULL,
    total_replicas INTEGER NOT NULL DEFAULT 1,
    last_charged_at BIGINT NOT NULL,
    created_at BIGINT NOT NULL,
    miner_ids TEXT[],
    selected_validator VARCHAR(100) NOT NULL,
    status VARCHAR(20) NOT NULL DEFAULT 'pending',
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(owner_account, file_hash)
);

-- Aggregated miner stats table: stores accumulated metrics about miners
CREATE TABLE IF NOT EXISTS miner_stats (
    node_id VARCHAR(100) PRIMARY KEY,
    storage_capacity_bytes BIGINT DEFAULT 0,
    available_space_bytes BIGINT DEFAULT 0,
    total_files_pinned INTEGER DEFAULT 0,
    total_files_size_bytes BIGINT DEFAULT 0,
    successful_pin_checks INTEGER DEFAULT 0,
    total_pin_checks INTEGER DEFAULT 0,
    last_online_block BIGINT,
    health_score NUMERIC(5,2) GENERATED ALWAYS AS (
        CASE 
            WHEN total_pin_checks = 0 THEN 0
            ELSE (successful_pin_checks * 100.0 / total_pin_checks)
        END
    ) STORED,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (node_id) REFERENCES registration(node_id) ON DELETE CASCADE
);

-- System events table: tracks important system events like rebalancing
CREATE TABLE IF NOT EXISTS system_events (
    id SERIAL PRIMARY KEY,
    event_type VARCHAR(100) NOT NULL,
    event_data JSONB,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Files table: stores file metadata
CREATE TABLE IF NOT EXISTS files (
    cid VARCHAR(255) PRIMARY KEY,
    name VARCHAR(500),
    size BIGINT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- File assignments table: tracks which miners are assigned to which files
CREATE TABLE IF NOT EXISTS file_assignments (
    cid VARCHAR(255) PRIMARY KEY,
    owner VARCHAR(100) NOT NULL,
    miner1 VARCHAR(100),
    miner2 VARCHAR(100),
    miner3 VARCHAR(100),
    miner4 VARCHAR(100),
    miner5 VARCHAR(100),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (cid) REFERENCES files(cid) ON DELETE CASCADE,
    FOREIGN KEY (miner1) REFERENCES registration(node_id) ON DELETE SET NULL,
    FOREIGN KEY (miner2) REFERENCES registration(node_id) ON DELETE SET NULL,
    FOREIGN KEY (miner3) REFERENCES registration(node_id) ON DELETE SET NULL,
    FOREIGN KEY (miner4) REFERENCES registration(node_id) ON DELETE SET NULL,
    FOREIGN KEY (miner5) REFERENCES registration(node_id) ON DELETE SET NULL
);

-- Pending assignment file table: temporary storage for files awaiting assignment
CREATE TABLE IF NOT EXISTS pending_assignment_file (
    id SERIAL PRIMARY KEY,
    cid VARCHAR(255) NOT NULL,
    owner VARCHAR(100) NOT NULL,
    filename VARCHAR(500),
    file_size_bytes BIGINT,
    status VARCHAR(50) DEFAULT 'pending',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(cid, owner)
);

-- Pinning requests table: tracks pinning requests
CREATE TABLE IF NOT EXISTS pinning_requests (
    id SERIAL PRIMARY KEY,
    request_hash VARCHAR(255),
    owner VARCHAR(100) NOT NULL,
    file_hash VARCHAR(255),
    file_name VARCHAR(500),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Epoch submissions table: tracks blockchain submissions per epoch
CREATE TABLE IF NOT EXISTS epoch_submissions (
    epoch BIGINT PRIMARY KEY,
    submitted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    miner_count INTEGER NOT NULL DEFAULT 0,
    success BOOLEAN NOT NULL DEFAULT FALSE
);

-- Create indexes for frequent query patterns
CREATE INDEX idx_miner_epoch_health_epoch ON miner_epoch_health(epoch);
CREATE INDEX idx_miner_profile_miner ON miner_profile(miner_node_id);
CREATE INDEX idx_storage_requests_status ON storage_requests(status);
CREATE INDEX idx_registration_node_type ON registration(node_type);

-- Create index for system events
CREATE INDEX idx_system_events_type_time ON system_events(event_type, created_at);
CREATE INDEX idx_system_events_created_at ON system_events(created_at);

-- Create indexes for new tables
CREATE INDEX idx_files_cid ON files(cid);
CREATE INDEX idx_files_size ON files(size);
CREATE INDEX idx_file_assignments_owner ON file_assignments(owner);
CREATE INDEX idx_file_assignments_miners ON file_assignments(miner1, miner2, miner3, miner4, miner5);
CREATE INDEX idx_pending_assignment_file_status ON pending_assignment_file(status);
CREATE INDEX idx_pending_assignment_file_owner ON pending_assignment_file(owner);
CREATE INDEX idx_pinning_requests_owner ON pinning_requests(owner);
CREATE INDEX idx_pinning_requests_file_hash ON pinning_requests(file_hash);
CREATE INDEX idx_epoch_submissions_epoch ON epoch_submissions(epoch);

-- Functions to update timestamps automatically
CREATE OR REPLACE FUNCTION update_timestamp()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Create triggers to automatically update timestamps
CREATE TRIGGER update_registration_timestamp
BEFORE UPDATE ON registration
FOR EACH ROW EXECUTE FUNCTION update_timestamp();

CREATE TRIGGER update_miner_stats_timestamp
BEFORE UPDATE ON miner_stats
FOR EACH ROW EXECUTE FUNCTION update_timestamp();

CREATE TRIGGER update_miner_profile_timestamp
BEFORE UPDATE ON miner_profile
FOR EACH ROW EXECUTE FUNCTION update_timestamp();

CREATE TRIGGER update_user_profile_timestamp
BEFORE UPDATE ON user_profile
FOR EACH ROW EXECUTE FUNCTION update_timestamp();

CREATE TRIGGER update_storage_requests_timestamp
BEFORE UPDATE ON storage_requests
FOR EACH ROW EXECUTE FUNCTION update_timestamp();

CREATE TRIGGER update_current_epoch_validator_timestamp
BEFORE UPDATE ON current_epoch_validator
FOR EACH ROW EXECUTE FUNCTION update_timestamp();

-- Create triggers for new tables
CREATE TRIGGER update_files_timestamp
BEFORE UPDATE ON files
FOR EACH ROW EXECUTE FUNCTION update_timestamp();

CREATE TRIGGER update_file_assignments_timestamp
BEFORE UPDATE ON file_assignments
FOR EACH ROW EXECUTE FUNCTION update_timestamp();

CREATE TRIGGER update_pending_assignment_file_timestamp
BEFORE UPDATE ON pending_assignment_file
FOR EACH ROW EXECUTE FUNCTION update_timestamp();

-- Create view for online miners with good health
CREATE OR REPLACE VIEW healthy_miners AS
SELECT 
    r.node_id,
    r.ipfs_peer_id,
    m.storage_capacity_bytes,
    m.available_space_bytes,
    m.health_score,
    m.total_files_pinned,
    m.last_online_block
FROM 
    registration r
JOIN 
    miner_stats m ON r.node_id = m.node_id
WHERE 
    r.status = 'active' 
    AND m.health_score >= 80
    AND r.node_type = 'StorageMiner';

-- Create view for miner health summary by epoch
CREATE OR REPLACE VIEW miner_health_by_epoch AS
SELECT 
    meh.node_id,
    meh.epoch,
    meh.ping_successes,
    meh.ping_failures,
    meh.pin_check_successes,
    meh.pin_check_failures,
    CASE 
        WHEN (meh.ping_successes + meh.ping_failures) = 0 THEN 0
        ELSE (meh.ping_successes * 100.0 / (meh.ping_successes + meh.ping_failures))
    END AS ping_success_rate,
    CASE 
        WHEN (meh.pin_check_successes + meh.pin_check_failures) = 0 THEN 0
        ELSE (meh.pin_check_successes * 100.0 / (meh.pin_check_successes + meh.pin_check_failures))
    END AS pin_check_success_rate,
    meh.last_activity_at
FROM 
    miner_epoch_health meh;

-- migrate:down
-- Drop views
DROP VIEW IF EXISTS miner_health_by_epoch;
DROP VIEW IF EXISTS healthy_miners;

-- Drop triggers
DROP TRIGGER IF EXISTS update_current_epoch_validator_timestamp ON current_epoch_validator;
DROP TRIGGER IF EXISTS update_storage_requests_timestamp ON storage_requests;
DROP TRIGGER IF EXISTS update_user_profile_timestamp ON user_profile;
DROP TRIGGER IF EXISTS update_miner_profile_timestamp ON miner_profile;
DROP TRIGGER IF EXISTS update_miner_stats_timestamp ON miner_stats;
DROP TRIGGER IF EXISTS update_registration_timestamp ON registration;
DROP TRIGGER IF EXISTS update_files_timestamp ON files;
DROP TRIGGER IF EXISTS update_file_assignments_timestamp ON file_assignments;
DROP TRIGGER IF EXISTS update_pending_assignment_file_timestamp ON pending_assignment_file;

-- Drop functions
DROP FUNCTION IF EXISTS update_timestamp();

-- Drop indexes
DROP INDEX IF EXISTS idx_registration_node_type;
DROP INDEX IF EXISTS idx_storage_requests_status;
DROP INDEX IF EXISTS idx_miner_profile_miner;
DROP INDEX IF EXISTS idx_miner_epoch_health_epoch;

-- Drop tables in reverse order to avoid foreign key constraints
DROP TABLE IF EXISTS epoch_submissions;
DROP TABLE IF EXISTS pinning_requests;
DROP TABLE IF EXISTS pending_assignment_file;
DROP TABLE IF EXISTS file_assignments;
DROP TABLE IF EXISTS files;
DROP TABLE IF EXISTS system_events;
DROP TABLE IF EXISTS miner_stats;
DROP TABLE IF EXISTS storage_requests;
DROP TABLE IF EXISTS user_profile;
DROP TABLE IF EXISTS miner_profile;
DROP TABLE IF EXISTS miner_epoch_health;
DROP TABLE IF EXISTS current_epoch_validator;
DROP TABLE IF EXISTS latest_block;
DROP TABLE IF EXISTS registration;