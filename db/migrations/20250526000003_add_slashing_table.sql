-- migrate:up
CREATE TABLE IF NOT EXISTS slashing_recommendations (
    id SERIAL PRIMARY KEY,
    owner_account_id VARCHAR(64) NOT NULL,
    reason VARCHAR(100) NOT NULL,
    details JSONB NOT NULL,
    created_at TIMESTAMP DEFAULT NOW(),
    processed BOOLEAN DEFAULT FALSE,
    processed_at TIMESTAMP NULL
);

CREATE INDEX IF NOT EXISTS idx_slashing_recommendations_owner ON slashing_recommendations(owner_account_id);
CREATE INDEX IF NOT EXISTS idx_slashing_recommendations_processed ON slashing_recommendations(processed);
CREATE INDEX IF NOT EXISTS idx_slashing_recommendations_created_at ON slashing_recommendations(created_at);

-- migrate:down
DROP TABLE IF EXISTS slashing_recommendations;