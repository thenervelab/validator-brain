-- migrate:up
-- Add monitoring tables for performance metrics and validation tracking

CREATE TABLE IF NOT EXISTS performance_metrics (
    id SERIAL PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    value FLOAT NOT NULL,
    timestamp TIMESTAMP NOT NULL DEFAULT NOW(),
    unit VARCHAR(20) DEFAULT '',
    tags JSONB DEFAULT '{}',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS validation_metrics (
    id SERIAL PRIMARY KEY,
    validator_id VARCHAR(100) NOT NULL,
    epoch BIGINT NOT NULL,
    block_number BIGINT NOT NULL,
    phase VARCHAR(50) NOT NULL,
    start_time TIMESTAMP NOT NULL,
    end_time TIMESTAMP,
    total_duration_ms INTEGER NOT NULL DEFAULT 0,
    success BOOLEAN NOT NULL DEFAULT TRUE,
    error_message TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create indexes for performance
CREATE INDEX idx_performance_metrics_name ON performance_metrics(name);
CREATE INDEX idx_performance_metrics_timestamp ON performance_metrics(timestamp);
CREATE INDEX idx_validation_metrics_validator ON validation_metrics(validator_id);
CREATE INDEX idx_validation_metrics_epoch ON validation_metrics(epoch);

-- migrate:down
DROP INDEX IF EXISTS idx_validation_metrics_epoch;
DROP INDEX IF EXISTS idx_validation_metrics_validator;
DROP INDEX IF EXISTS idx_performance_metrics_timestamp;
DROP INDEX IF EXISTS idx_performance_metrics_name;
DROP TABLE IF EXISTS validation_metrics;
DROP TABLE IF EXISTS performance_metrics;