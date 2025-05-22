-- name: get_current_validator
SELECT validator_account, selected_at_block, updated_at
FROM current_epoch_validator
ORDER BY updated_at DESC
LIMIT 1;