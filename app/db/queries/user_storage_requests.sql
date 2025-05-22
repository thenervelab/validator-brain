-- name: get_all_user_storage_requests
SELECT owner_account_id, file_hash, total_replicas, file_name, last_charged_at, created_at, 
       miner_ids, selected_validator, is_assigned, updated_at
FROM user_storage_requests;