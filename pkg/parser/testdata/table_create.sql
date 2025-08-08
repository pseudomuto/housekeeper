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
TTL timestamp + INTERVAL 1 YEAR
SETTINGS index_granularity = 8192, merge_with_ttl_timeout = 3600
COMMENT 'User events table';

-- Reordered clauses
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
PRIMARY KEY user_id
PARTITION BY toYYYYMM(timestamp)
ORDER BY (user_id, timestamp)
TTL timestamp + INTERVAL 1 YEAR
SAMPLE BY id
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

-- Table with simple projection
CREATE TABLE test_projections (
    id UInt64,
    name String,
    timestamp DateTime,
    
    PROJECTION by_time (
        SELECT *
        ORDER BY timestamp
    )
    
) ENGINE = MergeTree()
ORDER BY id;

-- Table with aggregating projection
CREATE TABLE analytics_data (
    user_id UInt64,
    event_type String,
    timestamp DateTime,
    revenue Decimal(10, 2),
    
    PROJECTION user_stats (
        SELECT 
            user_id,
            count() AS event_count,
            sum(revenue) AS total_revenue
        GROUP BY user_id
        ORDER BY user_id
    )
    
) ENGINE = MergeTree()
ORDER BY (user_id, timestamp);

-- Table with mixed elements (columns, indexes, projections, constraints)
CREATE TABLE complex_table (
    id UInt64,
    name String,
    category String,
    value Float64,
    created_at DateTime,
    
    INDEX name_bloom name TYPE bloom_filter GRANULARITY 1,
    
    PROJECTION by_category (
        SELECT 
            category,
            count() AS item_count,
            avg(value) AS avg_value
        GROUP BY category
        ORDER BY category
    ),
    
    PROJECTION timeline (
        SELECT *
        ORDER BY created_at DESC
    ),
    
    CONSTRAINT positive_value CHECK value >= 0
    
) ENGINE = MergeTree()
ORDER BY id;

-- CREATE TABLE with projection using window functions
CREATE TABLE analytics.sales_data (
    date Date,
    region String,
    product_id UInt64,
    sales_amount Decimal(10, 2),
    PROJECTION sales_rankings (
        SELECT 
            date,
            region,
            product_id,
            sales_amount,
            rank() OVER (PARTITION BY date, region ORDER BY sales_amount DESC) AS daily_rank,
            row_number() OVER (ORDER BY sales_amount DESC) AS overall_rank
        ORDER BY date, region, daily_rank
    )
) ENGINE = MergeTree()
ORDER BY (date, region, product_id);

-- Table with AggregateFunction types containing nested function calls
CREATE TABLE sessions.web_vital_events_by_hour (
    `received_at` DateTime CODEC(DoubleDelta),
    `pv_domain` LowCardinality(String),
    `pv_browser` LowCardinality(String),
    `pv_os` LowCardinality(String),
    `pv_device_type` LowCardinality(String),
    `pv_country_code` LowCardinality(String),
    `current_url` String,
    `current_ref_url` String,
    `vital_name` LowCardinality(String),
    `vital_rating` LowCardinality(String),
    `value_avg` AggregateFunction(avg, Float64),
    `value_min` AggregateFunction(min, Float64),
    `value_max` AggregateFunction(max, Float64),
    `value_quantiles` AggregateFunction(quantiles(0.5, 0.75, 0.9, 0.95, 0.99), Float64),
    `count` AggregateFunction(sum, UInt32),
    `users` AggregateFunction(uniq, UUID),
    `visits` AggregateFunction(uniq, UUID)
) ENGINE = Distributed('datawarehouse', 'sessions', 'web_vital_events_by_hour_local', rand());

-- Table with SimpleAggregateFunction
CREATE TABLE metrics.daily_aggregates (
    date Date,
    metric_sum SimpleAggregateFunction(sum, Float64),
    metric_max SimpleAggregateFunction(max, Float64),
    metric_any SimpleAggregateFunction(any, String)
) ENGINE = AggregatingMergeTree()
ORDER BY date;
