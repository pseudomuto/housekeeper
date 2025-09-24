-- Basic CREATE TABLE AS syntax
CREATE TABLE copy AS source ENGINE = MergeTree() ORDER BY id;

-- CREATE TABLE AS with database qualifiers
CREATE TABLE db1.table_copy AS db2.source_table ENGINE = Memory;

-- CREATE TABLE AS with ON CLUSTER
CREATE TABLE events_distributed ON CLUSTER production AS events_local 
ENGINE = Distributed(production, currentDatabase(), events_local, rand());

-- CREATE TABLE AS with IF NOT EXISTS
CREATE TABLE IF NOT EXISTS backup_users AS users ENGINE = MergeTree() ORDER BY user_id;

-- CREATE TABLE AS with OR REPLACE
CREATE OR REPLACE TABLE temp_copy AS original_table ENGINE = Memory;

-- CREATE TABLE AS with all options
CREATE OR REPLACE TABLE IF NOT EXISTS analytics.events_all ON CLUSTER analytics_cluster AS analytics.events_local
ENGINE = Distributed(analytics_cluster, analytics, events_local, cityHash64(user_id))
SETTINGS index_granularity = 8192
COMMENT 'Distributed view of events_local';

-- CREATE TABLE AS with backtick identifiers
CREATE TABLE `backup-table` AS `source-table` ENGINE = MergeTree() ORDER BY id;

-- CREATE TABLE AS with backtick database and table
CREATE TABLE `backup-db`.`table-copy` AS `source-db`.`source-table` 
ENGINE = MergeTree() 
ORDER BY `user-id`;

-- Mixed: CREATE TABLE AS followed by regular CREATE TABLE
CREATE TABLE base (
    id UInt64,
    name String,
    email String DEFAULT '',
    created_at DateTime DEFAULT now()
) ENGINE = MergeTree() ORDER BY id;

CREATE TABLE base_backup AS base ENGINE = MergeTree() ORDER BY id;

CREATE TABLE base_distributed ON CLUSTER cluster AS base 
ENGINE = Distributed(cluster, currentDatabase(), base, rand());

-- Complex distributed table example
CREATE TABLE local_metrics (
    timestamp DateTime,
    metric_name LowCardinality(String),
    value Float64,
    tags Map(String, String)
) ENGINE = MergeTree()
ORDER BY (metric_name, timestamp)
PARTITION BY toYYYYMM(timestamp)
TTL timestamp + INTERVAL 90 DAY;

CREATE TABLE metrics_all ON CLUSTER monitoring AS local_metrics
ENGINE = Distributed(monitoring, currentDatabase(), local_metrics, xxHash64(metric_name, timestamp))
SETTINGS 
    distributed_product_mode = 'global',
    skip_unavailable_shards = 1;