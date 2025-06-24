-- Clear tables at the start of a new epoch to avoid conflicts with other validators

-- name: clear_epoch_data
-- Clear temporary tables containing epoch-specific data
-- TRUNCATE TABLE miner_epoch_health;
TRUNCATE TABLE storage_requests;