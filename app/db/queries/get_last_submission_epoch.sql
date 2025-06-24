-- name: get_last_submission_epoch
-- Get the epoch number of the last blockchain submission
SELECT last_submission_epoch
FROM epoch_tracking
WHERE id = 1;