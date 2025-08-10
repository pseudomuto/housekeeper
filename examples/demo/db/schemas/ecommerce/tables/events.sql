-- Events table for tracking user interactions
CREATE TABLE ecommerce.events ON CLUSTER demo (
    id UInt64,
    user_id UInt64,
    event_type LowCardinality(String),
    timestamp DateTime DEFAULT now(),
    session_id String,
    page_url String,
    user_agent String DEFAULT '',
    country LowCardinality(String) DEFAULT 'Unknown',
    properties Map(String, String) DEFAULT map(),
    created_at DateTime DEFAULT now()
) ENGINE = MergeTree()
ORDER BY (user_id, timestamp)
PARTITION BY toYYYYMM(timestamp)
SETTINGS index_granularity = 8192
COMMENT 'User interaction events';