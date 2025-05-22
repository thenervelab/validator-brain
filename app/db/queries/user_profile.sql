-- name: check_user_profile_exists
SELECT EXISTS(
    SELECT 1 FROM user_profile
    WHERE user_id = $1 AND file_hash = $2
);

-- name: update_existing_user_profile
UPDATE user_profile
SET file_name = $3, file_size_bytes = $4, is_assigned = true,
    selected_validator = $5, main_request_hash = $6, miner_ids = $7,
    updated_at = NOW()
WHERE user_id = $1 AND file_hash = $2;

-- name: create_new_user_profile
INSERT INTO user_profile
(user_id, file_hash, file_name, file_size_bytes, is_assigned,
 last_charged_at, main_request_hash, miner_ids, owner_account, selected_validator, total_replicas)
VALUES
($1, $2, $3, $4, true, extract(epoch from now())::bigint, $5, $6, $1, $7, $8);

-- name: get_all_user_profiles
SELECT user_id, created_at, file_hash, file_name, file_size_bytes as file_size_in_bytes, is_assigned,
      last_charged_at, main_request_hash as main_req_hash, miner_ids, owner_account as owner, selected_validator,
      total_replicas, updated_at
FROM user_profile;