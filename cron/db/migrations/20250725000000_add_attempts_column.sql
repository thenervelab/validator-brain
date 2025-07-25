-- migrate:up
ALTER TABLE pinning_status ADD COLUMN attempts INTEGER NOT NULL DEFAULT 0;

-- migrate:down
-- SQLite doesn't support DROP COLUMN, so we'd need to recreate the table
-- For now, this migration is not reversible
-- If reversal is needed, you would need to:
-- 1. CREATE TABLE pinning_status_backup AS SELECT owner, cid, pinned_status, processed_at FROM pinning_status;
-- 2. DROP TABLE pinning_status;
-- 3. CREATE TABLE pinning_status (owner TEXT NOT NULL, cid TEXT NOT NULL, pinned_status TEXT NOT NULL CHECK (pinned_status IN ('success', 'fail')), processed_at TEXT NOT NULL, PRIMARY KEY (owner, cid));
-- 4. INSERT INTO pinning_status SELECT * FROM pinning_status_backup;
-- 5. DROP TABLE pinning_status_backup;