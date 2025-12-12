package parser_test

import "testing"

func TestCreateDatabase(t *testing.T) {
	t.Parallel()

	tests := []statementTest{
		{name: "basic", sql: "CREATE DATABASE basic_db;"},
		{name: "if_not_exists", sql: "CREATE DATABASE IF NOT EXISTS conditional_db;"},
		{name: "on_cluster", sql: "CREATE DATABASE IF NOT EXISTS cluster_db ON CLUSTER my_cluster;"},
		{name: "with_engine", sql: "CREATE DATABASE engine_db ENGINE = Atomic;"},
		{name: "with_engine_params", sql: "CREATE DATABASE remote_db ENGINE = MySQL('localhost:3306', 'database', 'user', 'password');"},
		{name: "with_engine_numeric_params", sql: "CREATE DATABASE materialized_db ENGINE = MaterializedMySQL('localhost:3306', 'database', 'user', 'password', 5000);"},
		{name: "with_comment", sql: "CREATE DATABASE comment_db COMMENT 'This is a test database';"},
		{name: "full_options", sql: "CREATE DATABASE IF NOT EXISTS full_db ON CLUSTER production ENGINE = Atomic COMMENT 'Full featured database';"},
		{name: "with_backticks", sql: "CREATE DATABASE `user-database` ENGINE = Atomic;"},
		{name: "backticks_full", sql: "CREATE DATABASE IF NOT EXISTS `order-db` ON CLUSTER `prod-cluster` COMMENT 'Database with special chars';"},
	}

	runStatementTests(t, "database/create", tests)
}

func TestAlterDatabase(t *testing.T) {
	t.Parallel()

	tests := []statementTest{
		{name: "basic", sql: "ALTER DATABASE basic_alter_db MODIFY COMMENT 'Updated comment';"},
		{name: "on_cluster", sql: "ALTER DATABASE cluster_alter_db ON CLUSTER production MODIFY COMMENT 'Production database';"},
	}

	runStatementTests(t, "database/alter", tests)
}

func TestAttachDatabase(t *testing.T) {
	t.Parallel()

	tests := []statementTest{
		{name: "basic", sql: "ATTACH DATABASE basic_attach_db;"},
		{name: "if_not_exists", sql: "ATTACH DATABASE IF NOT EXISTS conditional_attach_db;"},
		{name: "with_engine", sql: "ATTACH DATABASE engine_attach_db ENGINE = MySQL('localhost:3306', 'database', 'user', 'password');"},
		{name: "on_cluster", sql: "ATTACH DATABASE cluster_attach_db ON CLUSTER production;"},
		{name: "full_options", sql: "ATTACH DATABASE IF NOT EXISTS full_attach_db ENGINE = Atomic ON CLUSTER production;"},
	}

	runStatementTests(t, "database/attach", tests)
}

func TestDetachDatabase(t *testing.T) {
	t.Parallel()

	tests := []statementTest{
		{name: "basic", sql: "DETACH DATABASE basic_detach_db;"},
		{name: "if_exists", sql: "DETACH DATABASE IF EXISTS conditional_detach_db;"},
		{name: "on_cluster", sql: "DETACH DATABASE cluster_detach_db ON CLUSTER production;"},
		{name: "permanently", sql: "DETACH DATABASE permanent_detach_db PERMANENTLY;"},
		{name: "sync", sql: "DETACH DATABASE sync_detach_db SYNC;"},
		{name: "full_options", sql: "DETACH DATABASE IF EXISTS full_detach_db ON CLUSTER production PERMANENTLY SYNC;"},
	}

	runStatementTests(t, "database/detach", tests)
}

func TestDropDatabase(t *testing.T) {
	t.Parallel()

	tests := []statementTest{
		{name: "basic", sql: "DROP DATABASE basic_drop_db;"},
		{name: "if_exists", sql: "DROP DATABASE IF EXISTS conditional_drop_db;"},
		{name: "on_cluster", sql: "DROP DATABASE cluster_drop_db ON CLUSTER production;"},
		{name: "sync", sql: "DROP DATABASE sync_drop_db SYNC;"},
		{name: "full_options", sql: "DROP DATABASE IF EXISTS full_drop_db ON CLUSTER production SYNC;"},
	}

	runStatementTests(t, "database/drop", tests)
}

func TestRenameDatabase(t *testing.T) {
	t.Parallel()

	tests := []statementTest{
		{name: "single", sql: "RENAME DATABASE old_db TO new_db;"},
		{name: "multiple", sql: "RENAME DATABASE db1 TO db2, db3 TO db4;"},
		{name: "on_cluster", sql: "RENAME DATABASE old_db TO new_db ON CLUSTER my_cluster;"},
		{name: "multiple_on_cluster", sql: "RENAME DATABASE db1 TO db2, db3 TO db4 ON CLUSTER production;"},
		{name: "with_backticks", sql: "RENAME DATABASE `old-name` TO `new-name` ON CLUSTER `prod-cluster`;"},
	}

	runStatementTests(t, "database/rename", tests)
}
