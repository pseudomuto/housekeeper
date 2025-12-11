-- Basic SELECT statements
SELECT 1;

SELECT *;

SELECT id, name, email;

SELECT id AS user_id, name;

SELECT DISTINCT category;

-- FROM clause
SELECT * FROM users;

SELECT * FROM db.users;

SELECT * FROM users AS u;

-- Function calls
SELECT count(*);

SELECT sum(amount), avg(price);

SELECT count(*) AS total;

-- WHERE clause
SELECT * FROM users WHERE active = 1;

SELECT * FROM users WHERE active = 1 AND age > 18;

SELECT * FROM users WHERE id IN (1, 2, 3);

SELECT * FROM users WHERE name LIKE 'John%';

SELECT * FROM users WHERE email IS NOT NULL;

-- JOIN operations
SELECT * FROM users AS u INNER JOIN orders AS o ON u.id = o.user_id;

SELECT * FROM users AS u LEFT JOIN orders AS o ON u.id = o.user_id;

SELECT * FROM users AS u RIGHT JOIN orders AS o ON u.id = o.user_id;

SELECT * FROM users AS u FULL JOIN orders AS o ON u.id = o.user_id;

SELECT * FROM users AS u CROSS JOIN categories AS c;

SELECT * FROM users AS u JOIN orders AS o USING (user_id);

-- GROUP BY clause
SELECT category, count(*) FROM products GROUP BY category;

SELECT category, brand, count(*) FROM products GROUP BY category, brand;

SELECT category, count(*) FROM products GROUP BY category WITH CUBE;

SELECT category, count(*) FROM products GROUP BY category WITH ROLLUP;

SELECT category, count(*) FROM products GROUP BY category WITH TOTALS;

-- HAVING clause
SELECT category, count(*) FROM products GROUP BY category HAVING count(*) > 10;

-- ORDER BY clause
SELECT * FROM users ORDER BY name;

SELECT * FROM users ORDER BY age DESC;

SELECT * FROM users ORDER BY name ASC, age DESC;

SELECT * FROM users ORDER BY name NULLS FIRST;

SELECT * FROM users ORDER BY name COLLATE 'utf8_bin';

-- LIMIT clause
SELECT * FROM users LIMIT 10;

SELECT * FROM users LIMIT 10 OFFSET 20;

SELECT * FROM users LIMIT 5 BY category;

-- Window functions
SELECT name, salary, rank() OVER (ORDER BY salary DESC) AS salary_rank FROM employees;

SELECT name, rank() OVER (PARTITION BY department ORDER BY salary DESC) FROM employees;

-- Subqueries
SELECT * FROM (SELECT id, name FROM users WHERE active = 1) AS active_users;

SELECT * FROM users WHERE id IN (SELECT user_id FROM orders);

-- WITH (CTE) clauses
WITH active_users AS (SELECT * FROM users WHERE active = 1) SELECT * FROM active_users;

WITH active_users AS (SELECT * FROM users WHERE active = 1), recent_orders AS (SELECT * FROM orders WHERE date > '2023-01-01') SELECT * FROM active_users JOIN recent_orders ON active_users.id = recent_orders.user_id;

-- SETTINGS clauses
SELECT * FROM users SETTINGS max_threads = 4;

SELECT * FROM users SETTINGS max_threads = 4, use_index = 1;

-- Complex comprehensive query
SELECT
  u.id,
  u.name,
  count(o.id) AS order_count,
  sum(o.amount) AS total_spent
FROM users AS u
LEFT JOIN orders AS o ON u.id = o.user_id
WHERE u.active = 1 AND u.created_at >= '2023-01-01'
GROUP BY u.id, u.name
HAVING count(o.id) > 0
ORDER BY total_spent DESC
LIMIT 100;

-- ORDER BY WITH FILL basic
SELECT n FROM numbers ORDER BY n WITH FILL;

-- ORDER BY WITH FILL with FROM, TO, STEP
SELECT n FROM numbers ORDER BY n WITH FILL FROM 0 TO 10 STEP 1;

-- ORDER BY WITH FILL with float values
SELECT n, source FROM (
 SELECT toFloat32(number % 10) AS n, 'original' AS source
   FROM numbers WHERE number % 3 = 1
) ORDER BY n WITH FILL FROM 0 TO 5.51 STEP 0.5;

-- ORDER BY WITH FILL with INTERPOLATE (no columns)
SELECT date, value FROM metrics ORDER BY date WITH FILL INTERPOLATE;

-- ORDER BY WITH FILL with INTERPOLATE columns
SELECT date, value FROM metrics ORDER BY date WITH FILL INTERPOLATE (value);

-- ORDER BY WITH FILL with INTERPOLATE column and AS expression
SELECT date, value FROM metrics ORDER BY date WITH FILL INTERPOLATE (value AS value);

-- ORDER BY WITH FILL with INTERVAL step
SELECT date, value FROM metrics ORDER BY date WITH FILL STEP INTERVAL 1 DAY;

-- ORDER BY WITH FILL with date range and INTERVAL
SELECT date, value FROM metrics ORDER BY date WITH FILL FROM '2024-01-01' TO '2024-12-31' STEP INTERVAL 1 DAY;

-- ORDER BY WITH FILL with STALENESS
SELECT date, value FROM metrics ORDER BY date WITH FILL STALENESS INTERVAL 1 HOUR;

-- Multiple columns with mixed WITH FILL
SELECT a, b FROM t ORDER BY a ASC WITH FILL FROM 0 TO 100, b DESC;

-- Multiple columns both WITH FILL
SELECT a, b FROM t ORDER BY a WITH FILL FROM 0 TO 100 STEP 10, b WITH FILL FROM 0 TO 50 STEP 5;

-- WITH FILL with all modifiers and INTERPOLATE
SELECT date, value, count FROM metrics ORDER BY date WITH FILL FROM '2024-01-01' TO '2024-12-31' STEP INTERVAL 1 DAY STALENESS INTERVAL 2 DAY INTERPOLATE (value AS value, count AS 0);