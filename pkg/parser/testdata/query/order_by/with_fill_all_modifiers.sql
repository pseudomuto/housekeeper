SELECT
    `date`,
    `value`,
    `count`
FROM `metrics`
ORDER BY `date` WITH FILL FROM '2024-01-01' TO '2024-12-31' STEP INTERVAL 1 DAY STALENESS INTERVAL 2 DAY INTERPOLATE (`value` AS `value`, `count` AS 0);
