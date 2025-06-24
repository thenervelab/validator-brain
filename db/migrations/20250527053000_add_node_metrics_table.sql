-- migrate:up
CREATE TABLE IF NOT EXISTS node_metrics (
    id SERIAL PRIMARY KEY,
    miner_id VARCHAR(255) NOT NULL,
    ipfs_repo_size BIGINT NOT NULL,
    ipfs_storage_max BIGINT NOT NULL,
    block_number BIGINT NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Create indexes for better query performance
CREATE INDEX idx_node_metrics_miner_id ON node_metrics(miner_id);
CREATE INDEX idx_node_metrics_block_number ON node_metrics(block_number);
CREATE INDEX idx_node_metrics_created_at ON node_metrics(created_at);

-- Create a unique constraint to prevent duplicate entries for the same miner at the same block
CREATE UNIQUE INDEX idx_node_metrics_miner_block ON node_metrics(miner_id, block_number);

-- Create function to update updated_at timestamp if it doesn't exist
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ language 'plpgsql';

-- Create trigger to update updated_at timestamp
CREATE TRIGGER update_node_metrics_updated_at BEFORE UPDATE ON node_metrics
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

-- migrate:down
DROP TABLE IF EXISTS node_metrics; 