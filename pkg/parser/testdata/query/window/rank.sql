SELECT
    `name`,
    `salary`,
    rank() OVER (ORDER BY salary DESC) AS `salary_rank`
FROM `employees`;
