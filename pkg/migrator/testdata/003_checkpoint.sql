-- housekeeper:checkpoint
-- version: 003_checkpoint
-- description: Test checkpoint for initial migrations
-- created_at: 2024-08-10T12:00:00Z
-- included_migrations: 001_init,002_users
-- cumulative_hash: abc123def456789

-- Cumulative SQL from all included migrations
CREATE DATABASE test ENGINE = Atomic COMMENT 'Test database for migrations';

CREATE TABLE test.users (
    id UInt64,
    email String,
    created_at DateTime DEFAULT now(),
    metadata Map(String, String) DEFAULT map()
) ENGINE = MergeTree() 
ORDER BY (id, created_at)
PARTITION BY toYYYYMM(created_at);