package parser_test

import "testing"

func TestSelectBasic(t *testing.T) {
	t.Parallel()

	tests := []statementTest{
		{name: "literal", sql: `SELECT 1;`},
		{name: "star", sql: `SELECT *;`},
		{name: "columns", sql: `SELECT id, name, email;`},
		{name: "with_alias", sql: `SELECT id AS user_id, name;`},
		{name: "distinct", sql: `SELECT DISTINCT category;`},
		{name: "count_star", sql: `SELECT count(*);`},
		{name: "aggregates", sql: `SELECT sum(amount), avg(price);`},
		{name: "function_alias", sql: `SELECT count(*) AS total;`},
	}

	runStatementTests(t, "query/select", tests)
}

func TestSelectFrom(t *testing.T) {
	t.Parallel()

	tests := []statementTest{
		{name: "table", sql: `SELECT * FROM users;`},
		{name: "qualified", sql: `SELECT * FROM db.users;`},
		{name: "alias", sql: `SELECT * FROM users AS u;`},
		{name: "numbers", sql: `SELECT * FROM numbers(10);`},
		{name: "numbers_with_expr", sql: `SELECT toFloat32(number % 10) AS n FROM numbers(10) WHERE number % 3 = 1;`},
		{name: "numbers_alias", sql: `SELECT * FROM numbers(100) AS n;`},
		{name: "remote", sql: `SELECT * FROM remote('localhost:9000', 'db', 'table');`},
		{name: "cluster", sql: `SELECT * FROM cluster('my_cluster', 'db', 'table') AS t;`},
	}

	runStatementTests(t, "query/from", tests)
}

func TestSelectWhere(t *testing.T) {
	t.Parallel()

	tests := []statementTest{
		{name: "equals", sql: `SELECT * FROM users WHERE active = 1;`},
		{name: "and", sql: `SELECT * FROM users WHERE active = 1 AND age > 18;`},
		{name: "in", sql: `SELECT * FROM users WHERE id IN (1, 2, 3);`},
		{name: "like", sql: `SELECT * FROM users WHERE name LIKE 'John%';`},
		{name: "is_not_null", sql: `SELECT * FROM users WHERE email IS NOT NULL;`},
	}

	runStatementTests(t, "query/where", tests)
}

func TestSelectJoin(t *testing.T) {
	t.Parallel()

	tests := []statementTest{
		{name: "inner", sql: `SELECT * FROM users AS u INNER JOIN orders AS o ON u.id = o.user_id;`},
		{name: "left", sql: `SELECT * FROM users AS u LEFT JOIN orders AS o ON u.id = o.user_id;`},
		{name: "right", sql: `SELECT * FROM users AS u RIGHT JOIN orders AS o ON u.id = o.user_id;`},
		{name: "full", sql: `SELECT * FROM users AS u FULL JOIN orders AS o ON u.id = o.user_id;`},
		{name: "cross", sql: `SELECT * FROM users AS u CROSS JOIN categories AS c;`},
		{name: "using", sql: `SELECT * FROM users AS u JOIN orders AS o USING (user_id);`},
	}

	runStatementTests(t, "query/join", tests)
}

func TestSelectGroupBy(t *testing.T) {
	t.Parallel()

	tests := []statementTest{
		{name: "single", sql: `SELECT category, count(*) FROM products GROUP BY category;`},
		{name: "multiple", sql: `SELECT category, brand, count(*) FROM products GROUP BY category, brand;`},
		{name: "with_cube", sql: `SELECT category, count(*) FROM products GROUP BY category WITH CUBE;`},
		{name: "with_rollup", sql: `SELECT category, count(*) FROM products GROUP BY category WITH ROLLUP;`},
		{name: "with_totals", sql: `SELECT category, count(*) FROM products GROUP BY category WITH TOTALS;`},
		{name: "all", sql: `SELECT domain, browser, count(*) AS total FROM events WHERE date >= '2024-01-01' GROUP BY ALL;`},
		{name: "having", sql: `SELECT category, count(*) FROM products GROUP BY category HAVING count(*) > 10;`},
	}

	runStatementTests(t, "query/group_by", tests)
}

func TestSelectOrderBy(t *testing.T) {
	t.Parallel()

	tests := []statementTest{
		{name: "single", sql: `SELECT * FROM users ORDER BY name;`},
		{name: "desc", sql: `SELECT * FROM users ORDER BY age DESC;`},
		{name: "multiple", sql: `SELECT * FROM users ORDER BY name ASC, age DESC;`},
		{name: "nulls_first", sql: `SELECT * FROM users ORDER BY name NULLS FIRST;`},
		{name: "collate", sql: `SELECT * FROM users ORDER BY name COLLATE 'utf8_bin';`},
		{name: "with_fill_basic", sql: `SELECT n FROM numbers ORDER BY n WITH FILL;`},
		{name: "with_fill_range", sql: `SELECT n FROM numbers ORDER BY n WITH FILL FROM 0 TO 10 STEP 1;`},
		{name: "with_fill_float", sql: `SELECT n, source FROM (SELECT toFloat32(number % 10) AS n, 'original' AS source FROM numbers WHERE number % 3 = 1) ORDER BY n WITH FILL FROM 0 TO 5.51 STEP 0.5;`},
		{name: "with_fill_interpolate", sql: `SELECT date, value FROM metrics ORDER BY date WITH FILL INTERPOLATE;`},
		{name: "with_fill_interpolate_columns", sql: `SELECT date, value FROM metrics ORDER BY date WITH FILL INTERPOLATE (value);`},
		{name: "with_fill_interpolate_as", sql: `SELECT date, value FROM metrics ORDER BY date WITH FILL INTERPOLATE (value AS value);`},
		{name: "with_fill_interval", sql: `SELECT date, value FROM metrics ORDER BY date WITH FILL STEP INTERVAL 1 DAY;`},
		{name: "with_fill_date_range", sql: `SELECT date, value FROM metrics ORDER BY date WITH FILL FROM '2024-01-01' TO '2024-12-31' STEP INTERVAL 1 DAY;`},
		{name: "with_fill_staleness", sql: `SELECT date, value FROM metrics ORDER BY date WITH FILL STALENESS INTERVAL 1 HOUR;`},
		{name: "mixed_fill", sql: `SELECT a, b FROM t ORDER BY a ASC WITH FILL FROM 0 TO 100, b DESC;`},
		{name: "both_fill", sql: `SELECT a, b FROM t ORDER BY a WITH FILL FROM 0 TO 100 STEP 10, b WITH FILL FROM 0 TO 50 STEP 5;`},
		{name: "with_fill_all_modifiers", sql: `SELECT date, value, count FROM metrics ORDER BY date WITH FILL FROM '2024-01-01' TO '2024-12-31' STEP INTERVAL 1 DAY STALENESS INTERVAL 2 DAY INTERPOLATE (value AS value, count AS 0);`},
	}

	runStatementTests(t, "query/order_by", tests)
}

func TestSelectLimit(t *testing.T) {
	t.Parallel()

	tests := []statementTest{
		{name: "basic", sql: `SELECT * FROM users LIMIT 10;`},
		{name: "offset", sql: `SELECT * FROM users LIMIT 10 OFFSET 20;`},
		{name: "by", sql: `SELECT * FROM users LIMIT 5 BY category;`},
	}

	runStatementTests(t, "query/limit", tests)
}

func TestSelectWindow(t *testing.T) {
	t.Parallel()

	tests := []statementTest{
		{name: "rank", sql: `SELECT name, salary, rank() OVER (ORDER BY salary DESC) AS salary_rank FROM employees;`},
		{name: "partition", sql: `SELECT name, rank() OVER (PARTITION BY department ORDER BY salary DESC) FROM employees;`},
	}

	runStatementTests(t, "query/window", tests)
}

func TestSelectSubquery(t *testing.T) {
	t.Parallel()

	tests := []statementTest{
		{name: "from", sql: `SELECT * FROM (SELECT id, name FROM users WHERE active = 1) AS active_users;`},
		{name: "in", sql: `SELECT * FROM users WHERE id IN (SELECT user_id FROM orders);`},
	}

	runStatementTests(t, "query/subquery", tests)
}

func TestSelectCTE(t *testing.T) {
	t.Parallel()

	tests := []statementTest{
		{name: "single", sql: `WITH active_users AS (SELECT * FROM users WHERE active = 1) SELECT * FROM active_users;`},
		{name: "multiple", sql: `WITH active_users AS (SELECT * FROM users WHERE active = 1), recent_orders AS (SELECT * FROM orders WHERE date > '2023-01-01') SELECT * FROM active_users JOIN recent_orders ON active_users.id = recent_orders.user_id;`},
	}

	runStatementTests(t, "query/cte", tests)
}

func TestSelectSettings(t *testing.T) {
	t.Parallel()

	tests := []statementTest{
		{name: "single", sql: `SELECT * FROM users SETTINGS max_threads = 4;`},
		{name: "multiple", sql: `SELECT * FROM users SETTINGS max_threads = 4, use_index = 1;`},
	}

	runStatementTests(t, "query/settings", tests)
}

func TestSelectComplex(t *testing.T) {
	t.Parallel()

	tests := []statementTest{
		{name: "full_query", sql: `SELECT u.id, u.name, count(o.id) AS order_count, sum(o.amount) AS total_spent FROM users AS u LEFT JOIN orders AS o ON u.id = o.user_id WHERE u.active = 1 AND u.created_at >= '2023-01-01' GROUP BY u.id, u.name HAVING count(o.id) > 0 ORDER BY total_spent DESC LIMIT 100;`},
	}

	runStatementTests(t, "query/complex", tests)
}
