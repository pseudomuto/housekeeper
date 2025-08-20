-- Users table with comprehensive ClickHouse features
CREATE TABLE analytics.users (
    id UInt64,
    email String,
    name String,
    created_at DateTime DEFAULT now(),
    updated_at DateTime DEFAULT now(),
    is_active UInt8 DEFAULT 1,
    metadata Map(String, String) DEFAULT map(),
    tags Array(String) DEFAULT [],
    profile Nested(
        key String,
        value String
    )
) ENGINE = MergeTree()
ORDER BY (id, created_at)
PARTITION BY toYYYYMM(created_at)
SETTINGS index_granularity = 8192
COMMENT 'User accounts table for E2E testing';