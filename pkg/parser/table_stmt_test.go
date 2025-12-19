package parser_test

import "testing"

func TestCreateTable(t *testing.T) {
	t.Parallel()

	tests := []statementTest{
		// Basic
		{name: "basic", sql: `CREATE TABLE users (id UInt64, name String, email String) ENGINE = MergeTree() ORDER BY id;`},
		{name: "simple_engine", sql: `CREATE TABLE logs (timestamp DateTime, level String, message String) ENGINE = Log();`},
		{name: "with_database", sql: `CREATE TABLE analytics.events (id UInt64, user_id UInt64) ENGINE = MergeTree() ORDER BY id;`},

		// Complex types
		{name: "complex_types", sql: `CREATE TABLE user_profiles (
			user_id UInt64,
			profile_data Map(String, Nullable(String)),
			tags Array(LowCardinality(String)),
			coordinates Nullable(Tuple(lat Float64, lon Float64)),
			computed_field String,
			age_alias UInt8,
			default_data String
		) ENGINE = MergeTree() ORDER BY user_id PARTITION BY user_id % 100;`},

		// Parametric types
		{name: "parametric_types", sql: `CREATE TABLE measurements (
			id UInt64,
			device_id FixedString(16),
			value Decimal(10, 4),
			precision_value Decimal128(6),
			created_at DateTime64(3, 'UTC'),
			config_data String CODEC(LZ4HC(9))
		) ENGINE = MergeTree() ORDER BY (device_id, created_at);`},

		// Full options
		{name: "full_options", sql: `CREATE OR REPLACE TABLE IF NOT EXISTS analytics.events ON CLUSTER production (
			id UInt64,
			user_id UInt64,
			event_type LowCardinality(String),
			timestamp DateTime DEFAULT now(),
			data Map(String, String) DEFAULT map(),
			metadata Nullable(String) CODEC(ZSTD),
			tags Array(String) DEFAULT array(),
			location Tuple(lat Float64, lon Float64),
			settings Nested(key String, value String),
			temp_data String TTL timestamp + days(30) COMMENT 'Temporary data'
		) ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/events', '{replica}')
		ORDER BY (user_id, timestamp)
		PARTITION BY toYYYYMM(timestamp)
		PRIMARY KEY user_id
		SAMPLE BY id
		TTL timestamp + INTERVAL 1 YEAR
		SETTINGS index_granularity = 8192, merge_with_ttl_timeout = 3600
		COMMENT 'User events table';`},

		// TTL DELETE syntax
		{name: "ttl_delete", sql: `CREATE TABLE logs (
			id UInt64,
			message String,
			timestamp DateTime
		) ENGINE = MergeTree()
		ORDER BY (id, timestamp)
		TTL timestamp + INTERVAL 30 DAY DELETE;`},

		// Backticks
		{name: "with_backticks", sql: "CREATE TABLE `user-db`.`order-table` (`user-id` UInt64, `order-id` String, `order-date` Date, `select` String, `group` LowCardinality(String)) ENGINE = MergeTree() ORDER BY (`user-id`, `order-date`);"},

		// Indexes
		{name: "with_indexes", sql: `CREATE TABLE search_logs (
			id UInt64,
			query String,
			user_id UInt64,
			timestamp DateTime,
			category LowCardinality(String),
			response_time Float32,
			INDEX query_bloom query TYPE bloom_filter GRANULARITY 1,
			INDEX user_minmax user_id TYPE minmax GRANULARITY 2,
			INDEX category_set category TYPE set(1000) GRANULARITY 1
		) ENGINE = MergeTree() ORDER BY (timestamp, user_id) PARTITION BY toYYYYMM(timestamp);`},

		// Constraints
		{name: "with_constraints", sql: `CREATE TABLE user_profiles (
			user_id UInt64,
			email String,
			age UInt8,
			profile_data Map(String, String),
			created_at DateTime DEFAULT now(),
			INDEX email_bloom email TYPE bloom_filter GRANULARITY 1,
			CONSTRAINT valid_age CHECK age BETWEEN 13 AND 120,
			CONSTRAINT valid_email CHECK email LIKE '%@%'
		) ENGINE = MergeTree() ORDER BY user_id;`},

		// Projections
		{name: "with_projection", sql: `CREATE TABLE test_projections (
			id UInt64,
			name String,
			timestamp DateTime,
			PROJECTION by_time (SELECT * ORDER BY timestamp)
		) ENGINE = MergeTree() ORDER BY id;`},

		// Aggregate functions
		{name: "aggregate_functions", sql: `CREATE TABLE sessions.web_vital_events_by_hour (
			received_at DateTime CODEC(DoubleDelta),
			pv_domain LowCardinality(String),
			vital_name LowCardinality(String),
			value_avg AggregateFunction(avg, Float64),
			value_quantiles AggregateFunction(quantiles(0.5, 0.75, 0.9, 0.95, 0.99), Float64),
			count AggregateFunction(sum, UInt32),
			users AggregateFunction(uniq, UUID)
		) ENGINE = Distributed('datawarehouse', 'sessions', 'web_vital_events_by_hour_local', rand());`},

		// CREATE TABLE AS
		{name: "as_basic", sql: `CREATE TABLE copy AS source ENGINE = MergeTree() ORDER BY id;`},
		{name: "as_with_database", sql: `CREATE TABLE db1.table_copy AS db2.source_table ENGINE = Memory;`},
		{name: "as_on_cluster", sql: `CREATE TABLE events_distributed ON CLUSTER production AS events_local ENGINE = Distributed(production, currentDatabase(), events_local, rand());`},
		{name: "as_full_options", sql: `CREATE OR REPLACE TABLE IF NOT EXISTS analytics.events_all ON CLUSTER analytics_cluster AS analytics.events_local ENGINE = Distributed(analytics_cluster, analytics, events_local, cityHash64(user_id)) SETTINGS index_granularity = 8192 COMMENT 'Distributed view of events_local';`},

		// CREATE TABLE AS with table functions
		{name: "as_remote", sql: `CREATE TABLE remote_copy AS remote('host:9000', 'db', 'table') ENGINE = MergeTree() ORDER BY id;`},
		{name: "as_cluster", sql: `CREATE TABLE cluster_data AS cluster('my_cluster', 'default', 'events') ENGINE = MergeTree() ORDER BY id;`},
		{name: "as_s3", sql: `CREATE TABLE s3_import AS s3Table('https://bucket.s3.amazonaws.com/data.csv', 'CSV') ENGINE = MergeTree() ORDER BY tuple();`},
		{name: "as_numbers", sql: `CREATE TABLE test_numbers AS numbers(1000000) ENGINE = Memory();`},
		{name: "as_postgresql", sql: `CREATE TABLE pg_import AS postgresql('host:5432', 'database', 'table', 'user', 'password') ENGINE = MergeTree() ORDER BY id;`},
	}

	runStatementTests(t, "table/create", tests)
}

func TestAlterTable(t *testing.T) {
	t.Parallel()

	tests := []statementTest{
		// Column operations
		{name: "add_column", sql: `ALTER TABLE users ADD COLUMN age UInt8;`},
		{name: "add_column_if_not_exists", sql: `ALTER TABLE users ADD COLUMN IF NOT EXISTS age UInt8;`},
		{name: "add_column_after", sql: `ALTER TABLE users ADD COLUMN middle_name String AFTER first_name;`},
		{name: "add_column_first", sql: `ALTER TABLE users ADD COLUMN id UInt64 FIRST;`},
		{name: "drop_column", sql: `ALTER TABLE user_profiles DROP COLUMN computed_field;`},
		{name: "drop_column_if_exists", sql: `ALTER TABLE users DROP COLUMN IF EXISTS non_existent_column;`},
		{name: "rename_column", sql: `ALTER TABLE measurements RENAME COLUMN device_id TO device_identifier;`},
		{name: "comment_column", sql: `ALTER TABLE users COMMENT COLUMN email 'User email address';`},
		{name: "modify_column", sql: `ALTER TABLE users MODIFY COLUMN name String;`},
		{name: "modify_column_codec", sql: `ALTER TABLE events MODIFY COLUMN timestamp DateTime64(3, UTC) CODEC(DoubleDelta);`},

		// Index operations
		{name: "add_index", sql: `ALTER TABLE logs ADD INDEX level_idx level TYPE minmax GRANULARITY 1;`},
		{name: "add_index_complex", sql: `ALTER TABLE events ADD INDEX user_date_idx (user_id, toDate(timestamp)) TYPE minmax GRANULARITY 1;`},
		{name: "drop_index", sql: `ALTER TABLE logs DROP INDEX level_idx;`},

		// Constraint operations
		{name: "add_constraint", sql: `ALTER TABLE users ADD CONSTRAINT id_check CHECK id > 0;`},
		{name: "drop_constraint", sql: `ALTER TABLE users DROP CONSTRAINT id_check;`},

		// Data operations
		{name: "update", sql: `ALTER TABLE users UPDATE age = age + 1 WHERE id < 1000;`},
		{name: "delete", sql: `ALTER TABLE logs DELETE WHERE timestamp < now();`},

		// TTL operations
		{name: "modify_ttl", sql: `ALTER TABLE analytics.events MODIFY TTL timestamp + days(30);`},
		{name: "delete_ttl", sql: `ALTER TABLE analytics.events DELETE TTL;`},

		// Structure operations
		{name: "modify_order_by", sql: `ALTER TABLE measurements MODIFY ORDER BY (device_identifier, created_at, id);`},
		{name: "modify_sample_by", sql: `ALTER TABLE analytics.events MODIFY SAMPLE BY user_id;`},
		{name: "remove_sample_by", sql: `ALTER TABLE analytics.events REMOVE SAMPLE BY;`},

		// Settings operations
		{name: "modify_setting", sql: `ALTER TABLE analytics.events MODIFY SETTING index_granularity = 16384;`},
		{name: "reset_setting", sql: `ALTER TABLE analytics.events RESET SETTING index_granularity;`},

		// Partition operations
		{name: "attach_partition", sql: `ALTER TABLE analytics.events ATTACH PARTITION '202301';`},
		{name: "detach_partition", sql: `ALTER TABLE analytics.events DETACH PARTITION '202301';`},
		{name: "drop_partition", sql: `ALTER TABLE analytics.events DROP PARTITION '202301';`},
		{name: "freeze", sql: `ALTER TABLE analytics.events FREEZE;`},
		{name: "freeze_partition", sql: `ALTER TABLE analytics.events FREEZE PARTITION '202301';`},
		{name: "freeze_with_name", sql: `ALTER TABLE analytics.events FREEZE WITH NAME 'backup_20240101';`},
		{name: "fetch_partition", sql: `ALTER TABLE analytics.events FETCH PARTITION '202301' FROM '/clickhouse/tables/events';`},
		{name: "move_partition_to_table", sql: `ALTER TABLE analytics.events MOVE PARTITION '202301' TO TABLE analytics.events_archive;`},
		{name: "move_partition_to_disk", sql: `ALTER TABLE analytics.events MOVE PARTITION '202301' TO DISK 'cold_storage';`},
		{name: "replace_partition", sql: `ALTER TABLE analytics.events REPLACE PARTITION '202301' FROM analytics.events_backup;`},

		// Projection operations
		{name: "add_projection", sql: `ALTER TABLE analytics.events ADD PROJECTION user_stats (SELECT user_id, count() AS event_count GROUP BY user_id);`},
		{name: "drop_projection", sql: `ALTER TABLE analytics.events DROP PROJECTION user_stats;`},

		// Multiple operations
		{name: "multiple_operations", sql: `ALTER TABLE analytics.events ADD COLUMN session_id UUID, DROP COLUMN tags, RENAME COLUMN data TO event_data, COMMENT COLUMN timestamp 'Event timestamp';`},

		// On cluster
		{name: "on_cluster", sql: `ALTER TABLE logs ON CLUSTER production ADD COLUMN server_id String;`},
	}

	runStatementTests(t, "table/alter", tests)
}

func TestAttachTable(t *testing.T) {
	t.Parallel()

	tests := []statementTest{
		{name: "basic", sql: `ATTACH TABLE users;`},
		{name: "with_database", sql: `ATTACH TABLE analytics.events;`},
		{name: "if_not_exists", sql: `ATTACH TABLE IF NOT EXISTS temp_table;`},
		{name: "on_cluster", sql: `ATTACH TABLE measurements ON CLUSTER production;`},
		{name: "full_options", sql: `ATTACH TABLE IF NOT EXISTS analytics.old_events ON CLUSTER production;`},
	}

	runStatementTests(t, "table/attach", tests)
}

func TestDetachTable(t *testing.T) {
	t.Parallel()

	tests := []statementTest{
		{name: "basic", sql: `DETACH TABLE users;`},
		{name: "if_exists", sql: `DETACH TABLE IF EXISTS temp_table;`},
		{name: "permanently", sql: `DETACH TABLE old_data PERMANENTLY;`},
		{name: "sync", sql: `DETACH TABLE user_profiles SYNC;`},
		{name: "full_options", sql: `DETACH TABLE IF EXISTS analytics.old_events ON CLUSTER production PERMANENTLY SYNC;`},
	}

	runStatementTests(t, "table/detach", tests)
}

func TestDropTable(t *testing.T) {
	t.Parallel()

	tests := []statementTest{
		{name: "basic", sql: `DROP TABLE users;`},
		{name: "if_exists", sql: `DROP TABLE IF EXISTS temp_table;`},
		{name: "on_cluster", sql: `DROP TABLE measurements ON CLUSTER production;`},
		{name: "sync", sql: `DROP TABLE user_profiles SYNC;`},
		{name: "full_options", sql: `DROP TABLE IF EXISTS analytics.old_events ON CLUSTER production SYNC;`},
		{name: "with_backticks", sql: "DROP TABLE IF EXISTS `analytics-db`.`user-events` ON CLUSTER `prod-cluster`;"},
	}

	runStatementTests(t, "table/drop", tests)
}

func TestRenameTable(t *testing.T) {
	t.Parallel()

	tests := []statementTest{
		{name: "single", sql: `RENAME TABLE users TO users_old;`},
		{name: "with_database", sql: `RENAME TABLE analytics.events TO analytics.events_archive;`},
		{name: "across_databases", sql: `RENAME TABLE staging.logs TO production.logs;`},
		{name: "multiple", sql: `RENAME TABLE table1 TO table1_backup, table2 TO table2_backup;`},
		{name: "on_cluster", sql: `RENAME TABLE measurements TO measurements_legacy ON CLUSTER production;`},
		{name: "with_backticks", sql: "RENAME TABLE `old-table` TO `new-table` ON CLUSTER `prod-cluster`;"},
	}

	runStatementTests(t, "table/rename", tests)
}
