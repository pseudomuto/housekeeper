package parser_test

import "testing"

func TestCreateRole(t *testing.T) {
	t.Parallel()

	tests := []statementTest{
		{name: "basic", sql: `CREATE ROLE admin;`},
		{name: "if_not_exists", sql: `CREATE ROLE IF NOT EXISTS developer;`},
		{name: "or_replace", sql: `CREATE OR REPLACE ROLE analyst;`},
		{name: "on_cluster", sql: `CREATE ROLE reader ON CLUSTER production;`},
		{name: "with_settings", sql: `CREATE ROLE writer SETTINGS max_memory_usage = 10000000000;`},
		{name: "with_multiple_settings", sql: `CREATE ROLE poweruser SETTINGS max_memory_usage = 10000000000, readonly = 0;`},
		{name: "full_options", sql: `CREATE ROLE IF NOT EXISTS superuser ON CLUSTER production SETTINGS max_memory_usage = 10000000000, readonly = 0;`},
		{name: "or_replace_full", sql: `CREATE OR REPLACE ROLE dbadmin ON CLUSTER staging SETTINGS max_threads = 8;`},
	}

	runStatementTests(t, "role/create", tests)
}

func TestAlterRole(t *testing.T) {
	t.Parallel()

	tests := []statementTest{
		{name: "rename", sql: `ALTER ROLE admin RENAME TO administrator;`},
		{name: "if_exists", sql: `ALTER ROLE IF EXISTS developer RENAME TO dev;`},
		{name: "on_cluster", sql: `ALTER ROLE analyst ON CLUSTER production RENAME TO data_analyst;`},
		{name: "settings", sql: `ALTER ROLE reader SETTINGS max_memory_usage = 5000000000;`},
		{name: "multiple_settings", sql: `ALTER ROLE writer SETTINGS max_memory_usage = 10000000000, readonly = 0;`},
		{name: "rename_and_settings", sql: `ALTER ROLE poweruser RENAME TO super_user SETTINGS max_threads = 4;`},
		{name: "full_options", sql: `ALTER ROLE IF EXISTS dbadmin ON CLUSTER staging RENAME TO database_admin SETTINGS max_memory_usage = 20000000000;`},
	}

	runStatementTests(t, "role/alter", tests)
}

func TestDropRole(t *testing.T) {
	t.Parallel()

	tests := []statementTest{
		{name: "basic", sql: `DROP ROLE admin;`},
		{name: "if_exists", sql: `DROP ROLE IF EXISTS developer;`},
		{name: "multiple", sql: `DROP ROLE analyst, writer, reader;`},
		{name: "on_cluster", sql: `DROP ROLE poweruser ON CLUSTER production;`},
		{name: "if_exists_multiple", sql: `DROP ROLE IF EXISTS admin, developer, tester;`},
		{name: "full_options", sql: `DROP ROLE IF EXISTS dbadmin, superuser ON CLUSTER staging;`},
	}

	runStatementTests(t, "role/drop", tests)
}

func TestGrantRole(t *testing.T) {
	t.Parallel()

	tests := []statementTest{
		{name: "basic", sql: `GRANT admin TO john;`},
		{name: "multiple", sql: `GRANT reader, writer TO alice, bob;`},
		{name: "with_admin_option", sql: `GRANT developer TO lead WITH ADMIN OPTION;`},
		{name: "privileges", sql: `GRANT SELECT ON *.* TO reader;`},
	}

	runStatementTests(t, "role/grant", tests)
}

func TestRevokeRole(t *testing.T) {
	t.Parallel()

	tests := []statementTest{
		{name: "basic", sql: `REVOKE admin FROM john;`},
		{name: "multiple", sql: `REVOKE reader, writer FROM alice, bob;`},
	}

	runStatementTests(t, "role/revoke", tests)
}

func TestSetRole(t *testing.T) {
	t.Parallel()

	tests := []statementTest{
		{name: "default", sql: `SET ROLE DEFAULT;`},
		{name: "none", sql: `SET ROLE NONE;`},
		{name: "all", sql: `SET ROLE ALL;`},
		{name: "specific", sql: `SET ROLE admin;`},
		{name: "multiple", sql: `SET ROLE reader, writer;`},
	}

	runStatementTests(t, "role/set", tests)
}

func TestSetDefaultRole(t *testing.T) {
	t.Parallel()

	tests := []statementTest{
		{name: "none", sql: `SET DEFAULT ROLE NONE TO john;`},
		{name: "all", sql: `SET DEFAULT ROLE ALL TO alice;`},
		{name: "specific", sql: `SET DEFAULT ROLE reader TO bob;`},
		{name: "to_multiple", sql: `SET DEFAULT ROLE developer TO alice, bob, charlie;`},
	}

	runStatementTests(t, "role/set_default", tests)
}
