-- migrate:up
CREATE TABLE IF NOT EXISTS parsed_cids (
    id SERIAL PRIMARY KEY,
    cid VARCHAR(255) NOT NULL UNIQUE,
    profile_type VARCHAR(50) NOT NULL, -- 'user' or 'miner'
    parsed_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    file_count INTEGER DEFAULT 0, -- Number of files found in the profile
    account VARCHAR(255), -- Account/node_id associated with this profile
    CONSTRAINT unique_cid_type UNIQUE (cid, profile_type)
);

-- Create indexes for better query performance
CREATE INDEX idx_parsed_cids_cid ON parsed_cids(cid);
CREATE INDEX idx_parsed_cids_profile_type ON parsed_cids(profile_type);
CREATE INDEX idx_parsed_cids_parsed_at ON parsed_cids(parsed_at);

-- migrate:down
DROP TABLE IF EXISTS parsed_cids; 