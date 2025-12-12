SELECT
    `date`,
    `value`
FROM `metrics`
ORDER BY `date` WITH FILL FROM '2024-01-01' TO '2024-12-31' STEP INTERVAL 1 DAY;
