-- Create analytics database
CREATE DATABASE IF NOT EXISTS analytics ON CLUSTER my_cluster;

-- Events table for analytics data
CREATE TABLE IF NOT EXISTS analytics.events ON CLUSTER my_cluster (
    id UUID DEFAULT generateUUIDv4(),
    timestamp DateTime,
    event_type String,
    user_id UInt64,
    properties String CODEC(ZSTD(3)),
    created_at DateTime DEFAULT now() COMMENT 'Record creation timestamp'
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(timestamp)
ORDER BY (timestamp, event_type)
TTL timestamp + INTERVAL 90 DAY
SETTINGS index_granularity = 8192;

-- Users table with replacing merge tree
CREATE TABLE IF NOT EXISTS analytics.users ON CLUSTER my_cluster (
    id UInt64,
    email String,
    name String,
    created_at DateTime,
    updated_at DateTime,
    metadata Nullable(String) CODEC(LZ4) COMMENT 'Additional user metadata'
)
ENGINE = ReplacingMergeTree()
ORDER BY id
SETTINGS index_granularity = 8192;

-- Daily statistics view
CREATE VIEW IF NOT EXISTS analytics.daily_stats ON CLUSTER my_cluster AS
SELECT 
    toDate(timestamp) as date,
    event_type,
    count() as count,
    uniq(user_id) as unique_users
FROM analytics.events
GROUP BY date, event_type;