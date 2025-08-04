-- Basic table creation
CREATE TABLE users (
    id UInt64,
    name String,
    email String
) ENGINE = MergeTree()
ORDER BY id;

-- Table with database prefix and all options
CREATE OR REPLACE TABLE IF NOT EXISTS analytics.events ON CLUSTER production (
    id UInt64,
    user_id UInt64,
    event_type LowCardinality(String),
    timestamp DateTime DEFAULT now(),
    data Map(String, String) DEFAULT map(),
    metadata Nullable(String) CODEC(ZSTD),
    tags Array(String) DEFAULT array(),
    location Tuple(lat Float64, lon Float64),
    settings Nested(
        key String,
        value String
    ),
    temp_data String TTL timestamp + days(30) COMMENT 'Temporary data'
) ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/events', '{replica}')
ORDER BY (user_id, timestamp)
PARTITION BY toYYYYMM(timestamp)
PRIMARY KEY user_id
SAMPLE BY id
TTL timestamp + years(1)
SETTINGS index_granularity = 8192, merge_with_ttl_timeout = 3600
COMMENT 'User events table';

-- Simple table with just ENGINE
CREATE TABLE logs (
    timestamp DateTime,
    level String,
    message String
) ENGINE = Log();

-- Table with complex types and expressions
CREATE TABLE user_profiles (
    user_id UInt64,
    profile_data Map(String, Nullable(String)),
    tags Array(LowCardinality(String)),
    coordinates Nullable(Tuple(lat Float64, lon Float64)),
    computed_field String,
    age_alias UInt8,
    default_data String
) ENGINE = MergeTree()
ORDER BY user_id
PARTITION BY user_id % 100;

-- Table with parametric types
CREATE TABLE measurements (
    id UInt64,
    device_id FixedString(16),
    value Decimal(10, 4),
    precision_value Decimal128(6),
    created_at DateTime64(3, 'UTC'),
    config_data String CODEC(LZ4HC(9))
) ENGINE = MergeTree()
ORDER BY (device_id, created_at);

-- Table with backtick identifiers
CREATE TABLE `user-db`.`order-table` (
    `user-id` UInt64,
    `order-id` String,
    `order-date` Date,
    `select` String,  -- reserved keyword as column name
    `group` LowCardinality(String)  -- reserved keyword as column name
) ENGINE = MergeTree()
ORDER BY (`user-id`, `order-date`);

-- Table with mixed backtick and regular identifiers
CREATE TABLE IF NOT EXISTS `analytics-db`.`user-events` ON CLUSTER `prod-cluster` (
    id UInt64,
    `user-name` String,
    event_type String,
    `created-at` DateTime DEFAULT now()
) ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/user-events', '{replica}')
ORDER BY id;

-- Table with INDEX definitions
CREATE TABLE search_logs (
    id UInt64,
    query String,
    user_id UInt64,
    timestamp DateTime,
    category LowCardinality(String),
    response_time Float32,
    
    INDEX query_bloom query TYPE bloom_filter GRANULARITY 1,
    INDEX user_minmax user_id TYPE minmax GRANULARITY 2,
    INDEX category_set category TYPE set(1000) GRANULARITY 1,
    INDEX response_time_minmax response_time TYPE minmax GRANULARITY 1
    
) ENGINE = MergeTree()
ORDER BY (timestamp, user_id)
PARTITION BY toYYYYMM(timestamp);

-- Table with mixed elements (columns, indexes, constraints)
CREATE TABLE user_profiles (
    user_id UInt64,
    email String,
    age UInt8,
    profile_data Map(String, String),
    created_at DateTime DEFAULT now(),
    updated_at DateTime DEFAULT now(),
    
    INDEX email_bloom email TYPE bloom_filter GRANULARITY 1,
    INDEX age_minmax age TYPE minmax GRANULARITY 1,
    
    CONSTRAINT valid_age CHECK age BETWEEN 13 AND 120,
    CONSTRAINT valid_email CHECK email LIKE '%@%'
    
) ENGINE = MergeTree()
ORDER BY user_id;