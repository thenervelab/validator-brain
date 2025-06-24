-- name: update_last_submission_epoch
-- Update the epoch number of the last blockchain submission
INSERT INTO epoch_tracking (id, last_submission_epoch, updated_at)
VALUES (1, $1, NOW())
ON CONFLICT (id) 
DO UPDATE SET 
    last_submission_epoch = $1,
    updated_at = NOW();