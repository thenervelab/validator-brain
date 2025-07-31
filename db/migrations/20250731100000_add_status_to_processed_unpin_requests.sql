-- migrate:up
-- Add status column to processed_unpin_requests table
ALTER TABLE processed_unpin_requests 
ADD COLUMN status VARCHAR(50) DEFAULT 'unprocessed' CHECK (status IN ('unprocessed', 'processed'));

-- Create index for better query performance
CREATE INDEX idx_processed_unpin_requests_status ON processed_unpin_requests(status);

-- Update existing records to have 'unprocessed' status
UPDATE processed_unpin_requests SET status = 'unprocessed' WHERE status IS NULL;

-- migrate:down
DROP INDEX IF EXISTS idx_processed_unpin_requests_status;
ALTER TABLE processed_unpin_requests DROP COLUMN IF EXISTS status;