-- Basic ATTACH TABLE
ATTACH TABLE users;

-- ATTACH TABLE with database prefix
ATTACH TABLE analytics.events;

-- ATTACH TABLE with IF NOT EXISTS
ATTACH TABLE IF NOT EXISTS temp_table;

-- ATTACH TABLE with IF NOT EXISTS and database prefix
ATTACH TABLE IF NOT EXISTS staging.temporary_data;

-- ATTACH TABLE with ON CLUSTER
ATTACH TABLE measurements ON CLUSTER production;

-- ATTACH TABLE with ON CLUSTER and IF NOT EXISTS
ATTACH TABLE IF NOT EXISTS logs ON CLUSTER analytics_cluster;

-- ATTACH TABLE with all options
ATTACH TABLE IF NOT EXISTS analytics.old_events ON CLUSTER production;

-- Multiple tables in separate statements
ATTACH TABLE table1;
ATTACH TABLE table2;
ATTACH TABLE IF NOT EXISTS table3;

-- ATTACH TABLE with underscores and numbers
ATTACH TABLE data_2023_archive;
ATTACH TABLE temp_table_v2;

-- ATTACH TABLE with IF NOT EXISTS for multiple scenarios
ATTACH TABLE IF NOT EXISTS db1.missing_table;
ATTACH TABLE IF NOT EXISTS missing_table ON CLUSTER test_cluster;

-- Complex table names
ATTACH TABLE IF NOT EXISTS analytics.user_events_2023_01_backup ON CLUSTER production;

-- ATTACH TABLE for materialized views (same syntax)
ATTACH TABLE mv_daily_stats;
ATTACH TABLE analytics.mv_hourly_aggregates;
ATTACH TABLE IF NOT EXISTS reporting.mv_summary ON CLUSTER reporting_cluster;

-- Multiple database scenarios
ATTACH TABLE staging.users;
ATTACH TABLE production.users;
ATTACH TABLE analytics.users;

-- Cluster variations
ATTACH TABLE metrics ON CLUSTER shard1;
ATTACH TABLE metrics ON CLUSTER shard2;
ATTACH TABLE metrics ON CLUSTER all_shards;

-- Edge cases
ATTACH TABLE t;
ATTACH TABLE IF NOT EXISTS very_long_table_name_with_many_underscores_and_numbers_123456;
ATTACH TABLE db1.t1 ON CLUSTER c1;