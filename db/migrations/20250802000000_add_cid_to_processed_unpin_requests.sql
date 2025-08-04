-- migrate:up
-- Add cid column to processed_unpin_requests table

ALTER TABLE processed_unpin_requests
    ADD COLUMN cid TEXT;

-- Create index for better query performance
CREATE INDEX idx_processed_unpin_requests_cid ON processed_unpin_requests (cid);

-- migrate:down
DROP INDEX IF EXISTS idx_processed_unpin_requests_cid;
ALTER TABLE processed_unpin_requests
    DROP COLUMN IF EXISTS cid;