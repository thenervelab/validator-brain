-- Slashing recommendations table for users who lied about file sizes
-- Stores information about users who should be slashed for misrepresenting file sizes

CREATE TABLE IF NOT EXISTS slashing_recommendations (
    id SERIAL PRIMARY KEY,
    owner_account_id TEXT NOT NULL,
    reason TEXT NOT NULL,
    details JSONB NOT NULL,
    processed BOOLEAN NOT NULL DEFAULT FALSE,
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    processed_at TIMESTAMP
);

-- Create index on owner_account_id
CREATE INDEX IF NOT EXISTS slashing_recommendations_owner_idx ON slashing_recommendations (owner_account_id);

-- Create index on reason
CREATE INDEX IF NOT EXISTS slashing_recommendations_reason_idx ON slashing_recommendations (reason);

-- Create index on processed
CREATE INDEX IF NOT EXISTS slashing_recommendations_processed_idx ON slashing_recommendations (processed);