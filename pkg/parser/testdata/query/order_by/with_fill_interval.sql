SELECT
    `date`,
    `value`
FROM `metrics`
ORDER BY `date` WITH FILL STEP INTERVAL 1 DAY;
