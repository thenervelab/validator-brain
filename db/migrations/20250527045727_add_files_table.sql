-- migrate:up
CREATE TABLE IF NOT EXISTS files (
    id SERIAL PRIMARY KEY,
    cid VARCHAR(255) NOT NULL UNIQUE,
    name VARCHAR(255) NOT NULL,
    size BIGINT NOT NULL,
    created_date TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- Create indexes for better query performance
CREATE INDEX idx_files_cid ON files(cid);
CREATE INDEX idx_files_created_date ON files(created_date);

-- migrate:down
DROP TABLE IF EXISTS files;

