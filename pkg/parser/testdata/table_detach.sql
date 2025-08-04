-- Basic DETACH TABLE
DETACH TABLE users;

-- DETACH TABLE with database prefix
DETACH TABLE analytics.events;

-- DETACH TABLE with IF EXISTS
DETACH TABLE IF EXISTS temp_table;

-- DETACH TABLE with IF EXISTS and database prefix
DETACH TABLE IF EXISTS staging.temporary_data;

-- DETACH TABLE with ON CLUSTER
DETACH TABLE measurements ON CLUSTER production;

-- DETACH TABLE with ON CLUSTER and IF EXISTS
DETACH TABLE IF EXISTS logs ON CLUSTER analytics_cluster;

-- DETACH TABLE with PERMANENTLY
DETACH TABLE old_data PERMANENTLY;

-- DETACH TABLE with SYNC
DETACH TABLE user_profiles SYNC;

-- DETACH TABLE with PERMANENTLY and SYNC
DETACH TABLE archived_events PERMANENTLY SYNC;

-- DETACH TABLE with all options
DETACH TABLE IF EXISTS analytics.old_events ON CLUSTER production PERMANENTLY SYNC;

-- Multiple tables in separate statements
DETACH TABLE table1;
DETACH TABLE table2;
DETACH TABLE IF EXISTS table3;

-- DETACH TABLE with underscores and numbers
DETACH TABLE data_2023_archive;
DETACH TABLE temp_table_v2;

-- DETACH TABLE with IF EXISTS for multiple scenarios
DETACH TABLE IF EXISTS db1.missing_table;
DETACH TABLE IF EXISTS missing_table ON CLUSTER test_cluster;
DETACH TABLE IF EXISTS missing_table PERMANENTLY;
DETACH TABLE IF EXISTS missing_table SYNC;

-- Complex table names with various options
DETACH TABLE IF EXISTS analytics.user_events_2023_01_backup ON CLUSTER production SYNC;
DETACH TABLE IF EXISTS reporting.temp_calculations PERMANENTLY;

-- DETACH TABLE for materialized views (same syntax)
DETACH TABLE mv_daily_stats;
DETACH TABLE analytics.mv_hourly_aggregates PERMANENTLY;
DETACH TABLE IF EXISTS reporting.mv_summary ON CLUSTER reporting_cluster SYNC;

-- Multiple database scenarios
DETACH TABLE staging.users;
DETACH TABLE production.users PERMANENTLY;
DETACH TABLE analytics.users SYNC;

-- Cluster variations with different options
DETACH TABLE metrics ON CLUSTER shard1;
DETACH TABLE metrics ON CLUSTER shard2 PERMANENTLY;
DETACH TABLE metrics ON CLUSTER all_shards SYNC;

-- Edge cases
DETACH TABLE t;
DETACH TABLE IF EXISTS very_long_table_name_with_many_underscores_and_numbers_123456 PERMANENTLY SYNC;
DETACH TABLE db1.t1 ON CLUSTER c1 PERMANENTLY;

-- Various combinations of PERMANENTLY and SYNC
DETACH TABLE test1 PERMANENTLY;
DETACH TABLE test2 SYNC;
DETACH TABLE test3 PERMANENTLY SYNC;
DETACH TABLE IF EXISTS test4 PERMANENTLY;
DETACH TABLE IF EXISTS test5 SYNC;
DETACH TABLE IF EXISTS test6 PERMANENTLY SYNC;