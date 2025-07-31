-- migrate:up
-- Increase column sizes for processed_unpin_requests table to handle longer values

-- Increase request_id to TEXT (unlimited) since it can be very long with hex-encoded file hashes
ALTER TABLE processed_unpin_requests ALTER COLUMN request_id TYPE TEXT;

-- Increase file_hash to TEXT as well since hex-encoded file hashes can be very long
ALTER TABLE processed_unpin_requests ALTER COLUMN file_hash TYPE TEXT;

-- migrate:down
-- Revert to smaller column sizes (data truncation may occur)
ALTER TABLE processed_unpin_requests ALTER COLUMN request_id TYPE VARCHAR(255);
ALTER TABLE processed_unpin_requests ALTER COLUMN file_hash TYPE VARCHAR(255);