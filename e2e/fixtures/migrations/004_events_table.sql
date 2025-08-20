-- Events table with complex schema and TTL
CREATE TABLE analytics.events (
    id UInt64,
    user_id UInt64,
    session_id String,
    event_type LowCardinality(String),
    event_category Enum8('system' = 1, 'user' = 2, 'admin' = 3),
    timestamp DateTime DEFAULT now(),
    data String CODEC(ZSTD),
    properties Map(String, String) DEFAULT map(),
    ip_address IPv4,
    user_agent String,
    referer String
) ENGINE = MergeTree()
ORDER BY (user_id, timestamp, event_type)
PARTITION BY toYYYYMM(timestamp)
PRIMARY KEY (user_id, timestamp)
SAMPLE BY user_id
TTL timestamp + INTERVAL 90 DAY DELETE
SETTINGS index_granularity = 8192
COMMENT 'Event tracking table with TTL and sampling';