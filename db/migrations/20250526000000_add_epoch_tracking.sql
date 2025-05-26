-- migrate:up
-- Add epoch tracking table for validator submissions

CREATE TABLE IF NOT EXISTS epoch_tracking (
    id INTEGER PRIMARY KEY DEFAULT 1,
    last_submission_epoch BIGINT NOT NULL DEFAULT 0,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT single_row_only CHECK (id = 1)
);

-- Insert initial row
INSERT INTO epoch_tracking (id, last_submission_epoch) VALUES (1, 0)
ON CONFLICT (id) DO NOTHING;

-- Add trigger for automatic timestamp updates
CREATE TRIGGER update_epoch_tracking_timestamp
BEFORE UPDATE ON epoch_tracking
FOR EACH ROW EXECUTE FUNCTION update_timestamp();

-- migrate:down
DROP TRIGGER IF EXISTS update_epoch_tracking_timestamp ON epoch_tracking;
DROP TABLE IF EXISTS epoch_tracking;