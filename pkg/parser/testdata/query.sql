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