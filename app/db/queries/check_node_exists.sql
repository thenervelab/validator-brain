-- name: check_node_exists
SELECT EXISTS(SELECT 1 FROM registration WHERE node_id = $1)