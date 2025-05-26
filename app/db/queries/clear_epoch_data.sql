-- Clear tables at the start of a new epoch to avoid conflicts with other validators

-- name: clear_epoch_data
-- Clear temporary tables containing epoch-specific data
TRUNCATE TABLE miner_epoch_health;
TRUNCATE TABLE pending_pool;
-- Reset miner status for new assignment
UPDATE miner_stats SET 
    is_online = false,
    last_health_check = NULL,
    pin_success_count = 0,
    pin_failure_count = 0
WHERE true;