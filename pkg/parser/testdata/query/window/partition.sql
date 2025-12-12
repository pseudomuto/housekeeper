SELECT
    `name`,
    rank() OVER (PARTITION BY department ORDER BY salary DESC)
FROM `employees`;
