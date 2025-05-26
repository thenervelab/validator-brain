-- Monitoring schema for validator performance metrics

-- Performance metrics table
CREATE TABLE IF NOT EXISTS performance_metrics (
    id SERIAL PRIMARY KEY,
    name TEXT NOT NULL,
    value FLOAT NOT NULL,
    timestamp TIMESTAMP NOT NULL DEFAULT NOW(),
    unit TEXT NOT NULL DEFAULT '',
    tags JSONB,
    created_at TIMESTAMP NOT NULL DEFAULT NOW()
);

-- Create index on name
CREATE INDEX IF NOT EXISTS performance_metrics_name_idx ON performance_metrics (name);

-- Create index on timestamp
CREATE INDEX IF NOT EXISTS performance_metrics_timestamp_idx ON performance_metrics (timestamp);

-- Validation metrics table
CREATE TABLE IF NOT EXISTS validation_metrics (
    id SERIAL PRIMARY KEY,
    validator_id TEXT NOT NULL,
    epoch INTEGER NOT NULL,
    block_number BIGINT NOT NULL,
    phase TEXT NOT NULL,
    start_time TIMESTAMP NOT NULL,
    end_time TIMESTAMP NOT NULL,
    total_duration_ms INTEGER NOT NULL,
    success BOOLEAN NOT NULL DEFAULT TRUE,
    error_message TEXT,
    created_at TIMESTAMP NOT NULL DEFAULT NOW()
);

-- Create index on validator_id
CREATE INDEX IF NOT EXISTS validation_metrics_validator_idx ON validation_metrics (validator_id);

-- Create index on epoch
CREATE INDEX IF NOT EXISTS validation_metrics_epoch_idx ON validation_metrics (epoch);

-- Create index on block_number
CREATE INDEX IF NOT EXISTS validation_metrics_block_idx ON validation_metrics (block_number);

-- Create index on phase
CREATE INDEX IF NOT EXISTS validation_metrics_phase_idx ON validation_metrics (phase);

-- Create index on success
CREATE INDEX IF NOT EXISTS validation_metrics_success_idx ON validation_metrics (success);