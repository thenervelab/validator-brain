-- migrate:up
CREATE TABLE IF NOT EXISTS epoch_submissions (
    id SERIAL PRIMARY KEY,
    epoch BIGINT NOT NULL UNIQUE,
    submitted_at TIMESTAMP WITH TIME ZONE NOT NULL,
    miner_count INTEGER NOT NULL DEFAULT 0,
    success BOOLEAN NOT NULL DEFAULT false,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Create indexes for better query performance
CREATE INDEX idx_epoch_submissions_epoch ON epoch_submissions(epoch);
CREATE INDEX idx_epoch_submissions_submitted_at ON epoch_submissions(submitted_at);
CREATE INDEX idx_epoch_submissions_success ON epoch_submissions(success);

-- Create trigger to update updated_at timestamp
CREATE OR REPLACE FUNCTION update_epoch_submissions_updated_at()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER update_epoch_submissions_updated_at_trigger
    BEFORE UPDATE ON epoch_submissions
    FOR EACH ROW
    EXECUTE FUNCTION update_epoch_submissions_updated_at();

-- migrate:down
DROP TRIGGER IF EXISTS update_epoch_submissions_updated_at_trigger ON epoch_submissions;
DROP FUNCTION IF EXISTS update_epoch_submissions_updated_at();
DROP INDEX IF EXISTS idx_epoch_submissions_success;
DROP INDEX IF EXISTS idx_epoch_submissions_submitted_at;
DROP INDEX IF EXISTS idx_epoch_submissions_epoch;
DROP TABLE IF EXISTS epoch_submissions; 