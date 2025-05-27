-- migrate:up
CREATE TABLE IF NOT EXISTS file_assignments (
    id SERIAL PRIMARY KEY,
    cid VARCHAR(255) NOT NULL,
    owner VARCHAR(255) NOT NULL, -- SS58 address of the file owner
    miner1 VARCHAR(255), -- node_id of first assigned miner
    miner2 VARCHAR(255), -- node_id of second assigned miner
    miner3 VARCHAR(255), -- node_id of third assigned miner
    miner4 VARCHAR(255), -- node_id of fourth assigned miner
    miner5 VARCHAR(255), -- node_id of fifth assigned miner
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT fk_file_cid FOREIGN KEY (cid) REFERENCES files(cid) ON DELETE CASCADE
);

-- Create indexes for better query performance
CREATE INDEX idx_file_assignments_cid ON file_assignments(cid);
CREATE INDEX idx_file_assignments_owner ON file_assignments(owner);
CREATE INDEX idx_file_assignments_miner1 ON file_assignments(miner1);
CREATE INDEX idx_file_assignments_miner2 ON file_assignments(miner2);
CREATE INDEX idx_file_assignments_miner3 ON file_assignments(miner3);
CREATE INDEX idx_file_assignments_miner4 ON file_assignments(miner4);
CREATE INDEX idx_file_assignments_miner5 ON file_assignments(miner5);
CREATE INDEX idx_file_assignments_updated_at ON file_assignments(updated_at);

-- Add unique constraint to ensure one assignment per CID
CREATE UNIQUE INDEX idx_file_assignments_cid_unique ON file_assignments(cid);

-- Create trigger to update updated_at timestamp
CREATE OR REPLACE FUNCTION update_file_assignments_updated_at()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER update_file_assignments_updated_at_trigger
    BEFORE UPDATE ON file_assignments
    FOR EACH ROW
    EXECUTE FUNCTION update_file_assignments_updated_at();

-- migrate:down
DROP TRIGGER IF EXISTS update_file_assignments_updated_at_trigger ON file_assignments;
DROP FUNCTION IF EXISTS update_file_assignments_updated_at();
DROP TABLE IF EXISTS file_assignments; 