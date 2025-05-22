-- name: get_latest_block_number
SELECT block_number
FROM latest_block
ORDER BY updated_at DESC
LIMIT 1;