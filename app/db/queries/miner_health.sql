-- name: update_miner_epoch_health
INSERT INTO miner_epoch_health 
(node_id, ipfs_peer_id, epoch, ping_successes, ping_failures, 
 pin_check_successes, pin_check_failures, last_ping_attempt)
VALUES 
($1, 
 (SELECT ipfs_peer_id FROM registration WHERE node_id = $1),
 (SELECT block_number / 100 FROM latest_block ORDER BY updated_at DESC LIMIT 1), 
 CASE WHEN $2 THEN 1 ELSE 0 END, 
 CASE WHEN $2 THEN 0 ELSE 1 END,
 $4, $5, NOW())
ON CONFLICT (node_id, epoch) 
DO UPDATE SET
    ping_successes = miner_epoch_health.ping_successes + CASE WHEN $2 THEN 1 ELSE 0 END,
    ping_failures = miner_epoch_health.ping_failures + CASE WHEN $2 THEN 0 ELSE 1 END,
    pin_check_successes = miner_epoch_health.pin_check_successes + $4,
    pin_check_failures = miner_epoch_health.pin_check_failures + $5,
    last_ping_attempt = NOW(),
    last_activity_at = NOW();

-- name: update_miner_stats
UPDATE miner_stats
SET last_online_block = CASE WHEN $2 THEN 
    (SELECT block_number FROM latest_block ORDER BY updated_at DESC LIMIT 1) 
    ELSE last_online_block END,
    successful_pin_checks = successful_pin_checks + $4,
    total_pin_checks = total_pin_checks + $4 + $5,
    updated_at = NOW()
WHERE node_id = $1;

-- name: get_offline_miners
SELECT r.node_id, r.ipfs_peer_id, p.file_hash as profile_cid, m.last_online_block
FROM registration r
JOIN miner_stats m ON r.node_id = m.node_id
LEFT JOIN miner_profile p ON r.node_id = p.miner_node_id
WHERE m.last_online_block IS NULL 
   OR m.last_online_block < (SELECT block_number FROM latest_block ORDER BY updated_at DESC LIMIT 1) - 100;