-- name: insert_slashing_recommendation
INSERT INTO slashing_recommendations
(owner_account_id, reason, details, created_at)
VALUES ($1, $2, $3, NOW())