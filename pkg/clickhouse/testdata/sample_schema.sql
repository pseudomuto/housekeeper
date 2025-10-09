-- Sample ClickHouse schema for testing
-- Uses ON CLUSTER for all objects and Replicated tables where appropriate

-- Create databases with ON CLUSTER
CREATE DATABASE analytics ON CLUSTER test_cluster ENGINE = Atomic COMMENT 'Analytics database for events and metrics';

CREATE DATABASE user_data ON CLUSTER test_cluster ENGINE = Atomic COMMENT 'User-related data storage';

-- Create replicated tables with ON CLUSTER
CREATE TABLE analytics.events ON CLUSTER test_cluster (
    id UInt64,
    user_id UInt64,
    event_type LowCardinality(String),
    timestamp DateTime DEFAULT now(),
    properties Map(String, String) DEFAULT map(),
    session_id String,
    page_url String,
    user_agent String CODEC(ZSTD)
) ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/analytics/events', '{replica}')
ORDER BY (user_id, timestamp)
PARTITION BY toYYYYMM(timestamp)
TTL timestamp + INTERVAL 2 YEAR
SETTINGS index_granularity = 8192;

CREATE TABLE analytics.user_sessions ON CLUSTER test_cluster (
    session_id String,
    user_id UInt64,
    start_time DateTime,
    end_time DateTime,
    page_views UInt32 DEFAULT 0,
    duration_seconds UInt32 MATERIALIZED dateDiff('second', start_time, end_time),
    device_type LowCardinality(String),
    country_code FixedString(2)
) ENGINE = ReplicatedReplacingMergeTree('/clickhouse/tables/{shard}/analytics/user_sessions', '{replica}', end_time)
ORDER BY (user_id, start_time)
PARTITION BY toYYYYMM(start_time)
SETTINGS index_granularity = 4096;

CREATE TABLE user_data.profiles ON CLUSTER test_cluster (
    user_id UInt64,
    email String,
    name String,
    created_at DateTime,
    updated_at DateTime DEFAULT now(),
    status LowCardinality(String) DEFAULT 'active',
    metadata Nullable(String),
    tags Array(String) DEFAULT []
) ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/user_data/profiles', '{replica}')
ORDER BY user_id
SETTINGS index_granularity = 8192;

-- Create dictionaries with ON CLUSTER
CREATE DICTIONARY analytics.country_dict ON CLUSTER test_cluster (
    code String IS_OBJECT_ID,
    name String INJECTIVE,
    continent String,
    population UInt64 DEFAULT 0
) PRIMARY KEY code
SOURCE(HTTP(
    url 'http://example.com/countries.json'
    format 'JSONEachRow'
))
LAYOUT(HASHED(size_in_cells 1000))
LIFETIME(MIN 86400 MAX 172800)
COMMENT 'Country code mapping for analytics';

CREATE DICTIONARY user_data.user_segments_dict ON CLUSTER test_cluster (
    user_id UInt64 IS_OBJECT_ID,
    segment String,
    tier String DEFAULT 'free',
    score Float32 DEFAULT 0.0
) PRIMARY KEY user_id
SOURCE(CLICKHOUSE(
    host 'localhost'
    port 9000
    db 'user_data'
    table 'user_segments_source'
))
LAYOUT(COMPLEX_KEY_HASHED(size_in_cells 100000))
LIFETIME(3600)
SETTINGS(max_threads = 2)
COMMENT 'User segmentation for targeting';

-- Create regular views with ON CLUSTER
CREATE VIEW analytics.daily_events ON CLUSTER test_cluster AS
SELECT 
    toDate(timestamp) as date,
    event_type,
    count() as event_count,
    uniq(user_id) as unique_users,
    uniq(session_id) as unique_sessions
FROM analytics.events
GROUP BY date, event_type
ORDER BY date DESC, event_count DESC;

CREATE VIEW user_data.active_users ON CLUSTER test_cluster AS
SELECT 
    user_id,
    email,
    name,
    created_at,
    status
FROM user_data.profiles
WHERE status = 'active'
ORDER BY created_at DESC;

-- Create materialized views with ON CLUSTER using ReplicatedSummingMergeTree
CREATE MATERIALIZED VIEW analytics.mv_hourly_stats ON CLUSTER test_cluster
ENGINE = ReplicatedSummingMergeTree('/clickhouse/tables/{shard}/analytics/mv_hourly_stats', '{replica}')
ORDER BY (hour, event_type)
PARTITION BY toYYYYMM(hour)
POPULATE
AS SELECT 
    toStartOfHour(timestamp) as hour,
    event_type,
    count() as event_count,
    uniq(user_id) as unique_users
FROM analytics.events
GROUP BY hour, event_type;

CREATE MATERIALIZED VIEW analytics.mv_user_activity ON CLUSTER test_cluster
ENGINE = ReplicatedAggregatingMergeTree('/clickhouse/tables/{shard}/analytics/mv_user_activity', '{replica}')
ORDER BY (user_id, date)
PARTITION BY toYYYYMM(date)
AS SELECT 
    user_id,
    toDate(timestamp) as date,
    countState() as event_count,
    uniqState(session_id) as session_count,
    maxState(timestamp) as last_activity
FROM analytics.events
GROUP BY user_id, date;

-- Create destination table for materialized view TO pattern
CREATE TABLE analytics.daily_summary ON CLUSTER test_cluster (
    date Date,
    total_events UInt64,
    unique_users UInt64,
    avg_session_duration Float64
) ENGINE = ReplicatedSummingMergeTree('/clickhouse/tables/{shard}/analytics/daily_summary', '{replica}')
ORDER BY date
PARTITION BY toYYYYMM(date)
SETTINGS index_granularity = 8192;

-- Create materialized view using TO table pattern (more common approach)
CREATE MATERIALIZED VIEW analytics.mv_to_daily_summary ON CLUSTER test_cluster
TO analytics.daily_summary
AS SELECT
    toDate(timestamp) as date,
    count() as total_events,
    uniq(user_id) as unique_users,
    0.0 as avg_session_duration
FROM analytics.events
GROUP BY date;

-- Create test users with ON CLUSTER
CREATE USER test_analytics_reader ON CLUSTER test_cluster
    IDENTIFIED WITH plaintext_password BY 'reader_pass'
    HOST IP '192.168.0.0/16'
    DEFAULT ROLE analytics_reader
    DEFAULT DATABASE analytics;

CREATE USER test_admin ON CLUSTER test_cluster
    IDENTIFIED BY 'admin_secret'
    HOST ANY
    DEFAULT ROLE admin, operator;
