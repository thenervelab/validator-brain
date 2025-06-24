-- name: local_storage_requests
SELECT owner_account as owner_account_id, file_hash, 
       file_size_bytes as file_size, 
       created_at, status
FROM storage_requests
WHERE status = 'pending'