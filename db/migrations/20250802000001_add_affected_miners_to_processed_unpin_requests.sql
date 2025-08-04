-- migrate:up
-- Add affected_miners column to processed_unpin_requests table

ALTER TABLE processed_unpin_requests
    ADD COLUMN affected_miners TEXT[] DEFAULT '{}';

-- Create index for better query performance
CREATE INDEX idx_processed_unpin_requests_affected_miners ON processed_unpin_requests USING GIN (affected_miners);

-- migrate:down
DROP INDEX IF EXISTS idx_processed_unpin_requests_affected_miners;
ALTER TABLE processed_unpin_requests
    DROP COLUMN IF EXISTS affected_miners;