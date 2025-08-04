-- Basic DROP TABLE
DROP TABLE users;

-- DROP TABLE with database prefix
DROP TABLE analytics.events;

-- DROP TABLE with IF EXISTS
DROP TABLE IF EXISTS temp_table;

-- DROP TABLE with IF EXISTS and database prefix
DROP TABLE IF EXISTS staging.temporary_data;

-- DROP TABLE with ON CLUSTER
DROP TABLE measurements ON CLUSTER production;

-- DROP TABLE with ON CLUSTER and IF EXISTS
DROP TABLE IF EXISTS logs ON CLUSTER analytics_cluster;

-- DROP TABLE with SYNC
DROP TABLE user_profiles SYNC;

-- DROP TABLE with all options
DROP TABLE IF EXISTS analytics.old_events ON CLUSTER production SYNC;

-- Multiple tables in separate statements
DROP TABLE table1;
DROP TABLE table2;
DROP TABLE IF EXISTS table3;

-- DROP TABLE with special names (would need backticks in real ClickHouse)
-- Note: Parser currently doesn't support backtick-quoted identifiers

-- DROP TABLE with underscores and numbers
DROP TABLE data_2023_archive;
DROP TABLE temp_table_v2;

-- DROP TABLE with IF EXISTS for multiple scenarios
DROP TABLE IF EXISTS db1.missing_table;
DROP TABLE IF EXISTS missing_table ON CLUSTER test_cluster;
DROP TABLE IF EXISTS missing_table SYNC;

-- Complex table names
DROP TABLE IF EXISTS analytics.user_events_2023_01_backup ON CLUSTER production SYNC;

-- DROP TABLE for temporary tables (same syntax)
DROP TABLE tmp_calculations;
DROP TABLE IF EXISTS tmp_results ON CLUSTER compute_cluster SYNC;