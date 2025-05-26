-- Epoch tracking table for blockchain submissions
-- Tracks which epochs we've already submitted to the blockchain

CREATE TABLE IF NOT EXISTS epoch_tracking (
    id SERIAL PRIMARY KEY, -- Only using id=1 for single row
    last_submission_epoch BIGINT NOT NULL DEFAULT 0,
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP NOT NULL DEFAULT NOW()
);

-- Initialize with a single row
INSERT INTO epoch_tracking (id, last_submission_epoch)
VALUES (1, 0)
ON CONFLICT (id) DO NOTHING;