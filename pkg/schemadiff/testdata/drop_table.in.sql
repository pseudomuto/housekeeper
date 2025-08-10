-- Current state: temporary table exists
CREATE TABLE temp_users (id UInt64, name String) ENGINE = MergeTree() ORDER BY id;
-- Target state: table should be removed
-- empty target state