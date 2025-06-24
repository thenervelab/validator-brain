-- name: update_miner_health
-- Update miner health metrics based on health checks
INSERT INTO miner_epoch_health (
    node_id, 
    ipfs_peer_id,
    epoch,
    ping_successes,
    ping_failures,
    pin_check_successes,
    pin_check_failures,
    last_activity_at
)
VALUES ($1, $2, $3, 
        CASE WHEN $4 THEN 1 ELSE 0 END,
        CASE WHEN $4 THEN 0 ELSE 1 END,
        $5, $6, NOW())
ON CONFLICT (node_id, epoch) 
DO UPDATE SET 
    ping_successes = miner_epoch_health.ping_successes + CASE WHEN $4 THEN 1 ELSE 0 END,
    ping_failures = miner_epoch_health.ping_failures + CASE WHEN $4 THEN 0 ELSE 1 END,
    pin_check_successes = miner_epoch_health.pin_check_successes + $5,
    pin_check_failures = miner_epoch_health.pin_check_failures + $6,
    last_activity_at = NOW();