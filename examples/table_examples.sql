-- Example ClickHouse table definitions demonstrating the CREATE TABLE parser

-- Basic table with simple columns
CREATE TABLE users (
    id UInt64,
    name String,
    email String
) ENGINE = MergeTree()
ORDER BY id;

-- Table with complex data types and engine parameters
CREATE OR REPLACE TABLE IF NOT EXISTS analytics.events ON CLUSTER production (
    id UInt64,
    user_id UInt64,
    event_type LowCardinality(String),
    timestamp DateTime,
    data Map(String, String),
    metadata Nullable(String) CODEC(ZSTD),
    tags Array(String),
    location Tuple(lat Float64, lon Float64),
    settings Nested(
        key String,
        value String
    )
) ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/events', '{replica}')
ORDER BY (user_id, timestamp)
PARTITION BY toYYYYMM(timestamp)
PRIMARY KEY user_id
SAMPLE BY id
TTL timestamp + INTERVAL 1 YEAR
SETTINGS index_granularity = 8192, merge_with_ttl_timeout = 3600
COMMENT 'User events table';

-- Table with parametric data types
CREATE TABLE measurements (
    id UInt64,
    device_id FixedString(16),
    value Decimal(10, 4),
    precision_value Decimal128(6),
    created_at DateTime64(3, 'UTC'),
    config_data String CODEC(LZ4HC(9))
) ENGINE = MergeTree()
ORDER BY (device_id, created_at);

-- Simple log table
CREATE TABLE logs (
    timestamp DateTime,
    level String,
    message String
) ENGINE = Log();