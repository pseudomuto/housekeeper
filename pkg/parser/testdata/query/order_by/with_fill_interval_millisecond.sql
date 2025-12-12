SELECT
    `ts`,
    `value`
FROM `metrics`
ORDER BY `ts` WITH FILL FROM toDateTime64(`ts`, 3) TO toDateTime64(`ts`, 3) + INTERVAL 1 MILLISECOND STEP toIntervalMillisecond(100);
