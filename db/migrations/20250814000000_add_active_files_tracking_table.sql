-- +migrate Up
-- Create temporary tracking table for files found in user profiles from chain
-- This table is used to track which files are currently active in user profiles
-- so we can clean up orphaned files that are no longer referenced on chain

CREATE TABLE IF NOT EXISTS active_files_from_chain (
    cid TEXT PRIMARY KEY,
    last_seen TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Index for faster lookups during cleanup operations
CREATE INDEX IF NOT EXISTS idx_active_files_last_seen ON active_files_from_chain(last_seen);

-- +migrate Down
DROP INDEX IF EXISTS idx_active_files_last_seen;
DROP TABLE IF EXISTS active_files_from_chain;