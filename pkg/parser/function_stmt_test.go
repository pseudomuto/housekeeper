package parser_test

import "testing"

func TestCreateFunction(t *testing.T) {
	t.Parallel()

	tests := []statementTest{
		{name: "basic", sql: `CREATE FUNCTION linear_equation AS (x, k, b) -> plus(multiply(k, x), b);`},
		{name: "single_param", sql: `CREATE FUNCTION parity_str AS (n) -> if(modulo(n, 2), 'odd', 'even');`},
		{name: "multiple_params", sql: `CREATE FUNCTION safe_divide AS (a, b) -> if(equals(b, 0), 0, divide(a, b));`},
		{name: "on_cluster", sql: `CREATE FUNCTION truncate_string ON CLUSTER production AS (str, max_len) -> if(greater(length(str), max_len), concat(substring(str, 1, minus(max_len, 3)), '...'), str);`},
		{name: "complex_conditions", sql: `CREATE FUNCTION is_valid_date_range ON CLUSTER staging AS (start_date, end_date) -> and(and(lessOrEquals(start_date, end_date), greaterOrEquals(start_date, '1900-01-01')), lessOrEquals(end_date, '2100-12-31'));`},
		{name: "no_params", sql: `CREATE FUNCTION current_timestamp_utc AS () -> now();`},
		{name: "with_backticks", sql: "CREATE FUNCTION `my-special-function` AS (value) -> multiply(value, 2);"},
		{name: "backticked_cluster", sql: "CREATE FUNCTION calc_percentage ON CLUSTER `prod-cluster` AS (part, total) -> if(equals(total, 0), 0, divide(multiply(part, 100.0), total));"},
		{name: "clickhouse_format", sql: `CREATE FUNCTION normalizedBrowser AS br -> multiIf(lower(br) = 'firefox', 'Firefox', lower(br) = 'edge', 'Edge', lower(br) = 'safari', 'Safari', lower(br) = 'chrome', 'Chrome', lower(br) = 'webview', 'Webview', 'Other');`},
		{name: "clickhouse_format_on_cluster", sql: `CREATE FUNCTION normalizedOS ON CLUSTER warehouse AS os -> multiIf(startsWith(lower(os), 'windows'), 'Windows', startsWith(lower(os), 'mac'), 'Mac', 'Other');`},
	}

	runStatementTests(t, "function/create", tests)
}

func TestDropFunction(t *testing.T) {
	t.Parallel()

	tests := []statementTest{
		{name: "basic", sql: `DROP FUNCTION linear_equation;`},
		{name: "if_exists", sql: `DROP FUNCTION IF EXISTS parity_str;`},
		{name: "on_cluster", sql: `DROP FUNCTION IF EXISTS safe_divide ON CLUSTER production;`},
		{name: "with_backticks", sql: "DROP FUNCTION IF EXISTS `my-special-function`;"},
		{name: "backticked_cluster", sql: "DROP FUNCTION truncate_string ON CLUSTER `prod-cluster`;"},
	}

	runStatementTests(t, "function/drop", tests)
}
