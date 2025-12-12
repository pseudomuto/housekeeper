SELECT
    `date`,
    `value`
FROM `metrics`
ORDER BY `date` WITH FILL INTERPOLATE (`value`);
