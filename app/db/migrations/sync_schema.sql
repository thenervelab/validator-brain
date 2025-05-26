-- Blockchain synchronization tables
-- Used to track and resolve conflicts between local and blockchain state

-- Table for tracking pending submissions to the blockchain
CREATE TABLE IF NOT EXISTS pending_submissions (
    id SERIAL PRIMARY KEY,
    submission_id TEXT, -- For storage requests, this is the file hash
    node_id TEXT, -- For miner profiles, this is the node ID
    owner_id TEXT, -- For storage requests, this is the owner account ID
    submission_type TEXT NOT NULL,
    data JSONB NOT NULL,
    submitted BOOLEAN NOT NULL DEFAULT FALSE,
    submission_block INT,
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP NOT NULL DEFAULT NOW()
);

-- Create composite index for uniqueness
CREATE UNIQUE INDEX IF NOT EXISTS pending_submissions_composite_idx 
ON pending_submissions (COALESCE(submission_id, ''), COALESCE(node_id, ''), submission_type) 
WHERE (submission_id IS NOT NULL OR node_id IS NOT NULL);

-- Table for tracking synchronization status
CREATE TABLE IF NOT EXISTS blockchain_sync_status (
    id SERIAL PRIMARY KEY,
    in_sync BOOLEAN NOT NULL DEFAULT FALSE,
    conflicts_detected INT NOT NULL DEFAULT 0,
    conflicts_resolved INT NOT NULL DEFAULT 0,
    last_block_checked BIGINT,
    last_sync_time TIMESTAMP NOT NULL DEFAULT NOW(),
    actions_taken JSONB,
    created_at TIMESTAMP NOT NULL DEFAULT NOW()
);

-- Add sync_resolution column to miner_profile table
ALTER TABLE miner_profile ADD COLUMN IF NOT EXISTS sync_resolution BOOLEAN DEFAULT FALSE;