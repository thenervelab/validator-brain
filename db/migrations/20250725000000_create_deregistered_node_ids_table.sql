-- migrate:up
CREATE TABLE deregistered_node_ids (
    node_id TEXT PRIMARY KEY,
    unsuccessful_registration_checks INTEGER DEFAULT 0 NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_deregistered_node_ids_checks ON deregistered_node_ids(unsuccessful_registration_checks);

-- migrate:down
DROP TABLE IF EXISTS deregistered_node_ids;