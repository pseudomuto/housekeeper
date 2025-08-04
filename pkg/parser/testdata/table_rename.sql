-- Basic RENAME TABLE
RENAME TABLE users TO users_old;

-- RENAME TABLE with database prefix
RENAME TABLE analytics.events TO analytics.events_archive;

-- RENAME TABLE across databases
RENAME TABLE staging.logs TO production.logs;

-- Multiple RENAME TABLE operations
RENAME TABLE table1 TO table1_backup, table2 TO table2_backup;

-- Multiple renames with database prefixes
RENAME TABLE db1.users TO db1.users_old, analytics.events TO analytics.events_2023;

-- RENAME TABLE with ON CLUSTER
RENAME TABLE measurements TO measurements_legacy ON CLUSTER production;

-- Multiple renames with ON CLUSTER
RENAME TABLE old_table TO new_table, temp_data TO archived_data ON CLUSTER analytics_cluster;

-- Complex multiple renames with different databases and cluster
RENAME TABLE db1.t1 TO db2.t1, t2 TO t3, analytics.old_events TO analytics.archived_events ON CLUSTER main_cluster;

-- RENAME TABLE with IF EXISTS (Note: IF EXISTS is not standard in ClickHouse RENAME TABLE)
-- This might not be valid syntax, but including for completeness
-- RENAME TABLE IF EXISTS missing_table TO new_name;

-- Cross-database rename with cluster
RENAME TABLE staging.user_profiles TO production.user_profiles ON CLUSTER production;

-- Multiple cross-database renames
RENAME TABLE staging.logs TO production.logs, staging.metrics TO analytics.metrics, temp.data TO archive.data;

-- Rename with underscores and numbers
RENAME TABLE table_2023_01 TO table_2024_01, data_v1 TO data_v2;

-- RENAME TABLE with backtick identifiers for reserved keywords and special chars
RENAME TABLE `old-table` TO `new-table` ON CLUSTER `prod-cluster`;

-- Multiple renames with backtick identifiers
RENAME TABLE `user-db`.`old-users` TO `user-db`.`users-archive`, `order-db`.`select` TO `order-db`.`orders-data`;

-- Cross-database rename with backtick identifiers
RENAME TABLE `staging-db`.`user-profiles` TO `production-db`.`user-profiles`;