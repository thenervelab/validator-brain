-- name: check_miner_profile_exists
SELECT EXISTS(
    SELECT 1 FROM miner_profile
    WHERE miner_node_id = $1 AND file_hash = $2
);

-- name: update_existing_miner_profile
UPDATE miner_profile
SET file_size_bytes = $3, selected_validator = $4, updated_at = NOW()
WHERE miner_node_id = $1 AND file_hash = $2;

-- name: create_new_miner_profile
INSERT INTO miner_profile
(miner_node_id, file_hash, file_size_bytes, selected_validator, created_at)
VALUES
($1, $2, $3, $4, extract(epoch from now())::bigint);

-- name: update_miner_totals
UPDATE miner_stats
SET total_files_pinned = COALESCE(total_files_pinned, 0) + 1,
    total_files_size_bytes = COALESCE(total_files_size_bytes, 0) + $2
WHERE node_id = $1;

-- name: get_all_miner_profiles
SELECT miner_node_id, created_at, file_hash, file_size_bytes, selected_validator, updated_at
FROM miner_profile;

-- name: get_cids_for_miner
SELECT file_hash FROM miner_profile 
WHERE miner_node_id = $1;