-- migrate:up
-- Add processed_at column to pending_assignment_file table
-- This column is needed by the pinning request consumer
ALTER TABLE pending_assignment_file 
ADD COLUMN processed_at TIMESTAMP DEFAULT NULL;

-- Create index for the new column
CREATE INDEX idx_pending_assignment_file_processed_at ON pending_assignment_file(processed_at);

-- migrate:down
ALTER TABLE pending_assignment_file 
DROP COLUMN IF EXISTS processed_at;