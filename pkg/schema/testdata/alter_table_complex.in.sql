-- Current state: events table with basic schema and partitioning
CREATE TABLE events (
    id UInt64,
    user_id UInt64,
    event_type String,
    timestamp DateTime
) ENGINE = MergeTree()
ORDER BY (user_id, timestamp)
PARTITION BY toYYYYMM(timestamp)
;
-- Target state: events table with enhanced schema, optimized types, and new columns
CREATE TABLE events (
    id UInt64,
    user_id UInt64,
    event_type LowCardinality(String),
    timestamp DateTime DEFAULT now(),
    data Map(String, String),
    metadata Nullable(String)
) ENGINE = MergeTree()
ORDER BY (user_id, timestamp)
PARTITION BY toYYYYMM(timestamp)
SETTINGS index_granularity = 8192;
