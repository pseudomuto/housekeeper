SELECT
    `date`,
    `value`
FROM `metrics`
ORDER BY `date` WITH FILL STALENESS INTERVAL 1 HOUR;
