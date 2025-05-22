-- IPFS Service Validator Database Schema

-- Latest block tracking
CREATE TABLE IF NOT EXISTS latest_block (
    id SERIAL PRIMARY KEY,
    block_number INTEGER NOT NULL,
    block_hash TEXT,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- Current epoch validator
CREATE TABLE IF NOT EXISTS current_epoch_validator (
    id SERIAL PRIMARY KEY,
    account_id TEXT NOT NULL,
    block_number INTEGER NOT NULL,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- Miner registration
CREATE TABLE IF NOT EXISTS registration (
    id SERIAL PRIMARY KEY,
    node_id TEXT NOT NULL UNIQUE,
    node_type TEXT NOT NULL,  -- 'StorageMiner', etc.
    ipfs_node_id TEXT,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- Miner information
CREATE TABLE IF NOT EXISTS miners (
    id SERIAL PRIMARY KEY,
    node_id TEXT NOT NULL UNIQUE REFERENCES registration(node_id),
    ipfs_storage_max BIGINT,
    ipfs_zfs_pool_size BIGINT,
    miner_total_files_pinned INTEGER DEFAULT 0,
    miner_total_files_size BIGINT DEFAULT 0,
    is_online BOOLEAN DEFAULT FALSE,
    last_online TIMESTAMP WITH TIME ZONE,
    miner_success_rate REAL DEFAULT 1.0,
    miner_profile_cid TEXT,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- Miner health metrics for each epoch
CREATE TABLE IF NOT EXISTS miner_epoch_health (
    id SERIAL PRIMARY KEY,
    node_id TEXT NOT NULL REFERENCES registration(node_id),
    epoch_number INTEGER,
    is_online BOOLEAN DEFAULT FALSE,
    ping_time_ms INTEGER,
    pin_check_successes INTEGER DEFAULT 0,
    pin_check_failures INTEGER DEFAULT 0,
    success_rate REAL DEFAULT 0.0,
    checked_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    UNIQUE (node_id, epoch_number)
);

-- Storage requests from blockchain
CREATE TABLE IF NOT EXISTS user_storage_requests (
    id SERIAL PRIMARY KEY,
    owner_account_id TEXT NOT NULL,
    file_hash TEXT NOT NULL,
    total_replicas INTEGER NOT NULL,
    file_name TEXT,
    last_charged_at INTEGER,
    created_at INTEGER,
    miner_ids TEXT[],
    selected_validator TEXT,
    is_assigned BOOLEAN DEFAULT FALSE,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    UNIQUE (owner_account_id, file_hash)
);

-- Pending storage request pool
CREATE TABLE IF NOT EXISTS pending_pool (
    id SERIAL PRIMARY KEY,
    owner TEXT NOT NULL,
    file_hash TEXT NOT NULL,
    file_name TEXT,
    selected_validator TEXT,
    main_req_hash TEXT,
    status TEXT NOT NULL, -- 'pending', 'processed', 'completed'
    selected_miners TEXT[],
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    UNIQUE (owner, file_hash)
);

-- User profile information
CREATE TABLE IF NOT EXISTS user_profile (
    id SERIAL PRIMARY KEY,
    user_id TEXT NOT NULL,
    owner TEXT NOT NULL,
    file_hash TEXT NOT NULL,
    file_name TEXT,
    file_size_in_bytes BIGINT DEFAULT 0,
    is_assigned BOOLEAN DEFAULT FALSE,
    last_charged_at INTEGER,
    main_req_hash TEXT,
    miner_ids TEXT[],
    selected_validator TEXT,
    total_replicas INTEGER DEFAULT 1,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    UNIQUE (user_id, file_hash)
);

-- Miner profile information
CREATE TABLE IF NOT EXISTS miner_profile (
    id SERIAL PRIMARY KEY,
    miner_node_id TEXT NOT NULL REFERENCES registration(node_id),
    file_hash TEXT NOT NULL,
    file_size_in_bytes BIGINT DEFAULT 0,
    selected_validator TEXT,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    UNIQUE (miner_node_id, file_hash)
);

-- Insert some test data
INSERT INTO latest_block (block_number, block_hash) 
VALUES (1000, '0x1234567890abcdef')
ON CONFLICT DO NOTHING;

INSERT INTO current_epoch_validator (account_id, block_number) 
VALUES ('5GrwvaEF5zXb26Fz9rcQpDWS57CtERHpNehXCPcNoHGKutQY', 1000)
ON CONFLICT DO NOTHING;

-- Test storage miner
INSERT INTO registration (node_id, node_type, ipfs_node_id)
VALUES 
    ('testminer1', 'StorageMiner', 'QmTest1'),
    ('testminer2', 'StorageMiner', 'QmTest2'),
    ('testminer3', 'StorageMiner', 'QmTest3'),
    ('testminer4', 'StorageMiner', 'QmTest4'),
    ('testminer5', 'StorageMiner', 'QmTest5')
ON CONFLICT DO NOTHING;

-- Miners table entries
INSERT INTO miners (node_id, ipfs_storage_max, ipfs_zfs_pool_size, is_online)
VALUES 
    ('testminer1', 1000000000, 2000000000, true),
    ('testminer2', 2000000000, 3000000000, true),
    ('testminer3', 3000000000, 4000000000, true),
    ('testminer4', 4000000000, 5000000000, true),
    ('testminer5', 5000000000, 6000000000, true)
ON CONFLICT DO NOTHING;

-- Test storage request
INSERT INTO user_storage_requests (owner_account_id, file_hash, total_replicas, file_name, selected_validator)
VALUES ('testowner', 'QmTestFile', 3, 'test_file.txt', '5GrwvaEF5zXb26Fz9rcQpDWS57CtERHpNehXCPcNoHGKutQY')
ON CONFLICT DO NOTHING;