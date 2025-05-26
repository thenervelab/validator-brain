-- name: update_miner_stats
-- Update miner stats based on health checks
INSERT INTO miner_stats (
    node_id, 
    successful_pin_checks,
    total_pin_checks,
    last_online_block,
    updated_at
)
VALUES ($1, $3::INTEGER, $3::INTEGER + $4::INTEGER, 
        CASE WHEN $2 THEN 
            (SELECT block_number FROM latest_block ORDER BY updated_at DESC LIMIT 1) 
            ELSE NULL 
        END,
        NOW())
ON CONFLICT (node_id) 
DO UPDATE SET 
    successful_pin_checks = miner_stats.successful_pin_checks + $3::INTEGER,
    total_pin_checks = miner_stats.total_pin_checks + $3::INTEGER + $4::INTEGER,
    last_online_block = CASE WHEN $2 THEN 
        (SELECT block_number FROM latest_block ORDER BY updated_at DESC LIMIT 1) 
        ELSE miner_stats.last_online_block 
    END,
    updated_at = NOW();