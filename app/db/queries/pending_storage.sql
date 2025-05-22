-- name: get_pending_storage_requests
SELECT owner, file_hash, file_name, selected_validator, main_req_hash
FROM pending_pool
WHERE status = 'pending'
LIMIT $1;

-- name: update_storage_request_status_with_miners
UPDATE pending_pool
SET status = $1, selected_miners = $4, updated_at = NOW()
WHERE owner = $2 AND file_hash = $3;

-- name: update_storage_request_status
UPDATE pending_pool
SET status = $1, updated_at = NOW()
WHERE owner = $2 AND file_hash = $3;

-- name: get_processed_storage_requests
SELECT owner, file_hash, main_req_hash, selected_miners
FROM pending_pool
WHERE status = $1;

-- name: delete_from_pending_pool
DELETE FROM pending_pool
WHERE owner = $1 AND file_hash = $2;

-- name: delete_from_user_storage_requests
DELETE FROM user_storage_requests
WHERE owner_account_id = $1 AND file_hash = $2;

-- name: check_request_exists_in_pending_pool
SELECT EXISTS (
    SELECT 1 FROM pending_pool WHERE main_req_hash = $1
);

-- name: add_to_pending_pool
INSERT INTO pending_pool (owner, file_hash, file_name, selected_validator, main_req_hash, status, selected_miners)
VALUES ($1, $2, $3, $4, $5, $6, $7);