-- Track epoch-related data for blockchain submissions

-- name: get_last_submission_epoch
-- Get the last epoch number when we submitted to the blockchain
SELECT last_submission_epoch FROM epoch_tracking
WHERE id = 1;

-- name: update_last_submission_epoch
-- Update the last epoch number when we submitted to the blockchain
INSERT INTO epoch_tracking (id, last_submission_epoch, updated_at)
VALUES (1, $1, NOW())
ON CONFLICT (id) DO UPDATE SET
  last_submission_epoch = $1,
  updated_at = NOW();