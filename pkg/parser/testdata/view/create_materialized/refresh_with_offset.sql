CREATE MATERIALIZED VIEW `mv_offset`
REFRESH EVERY 1 DAY OFFSET 6 HOURS
AS SELECT
    toDate(`ts`) AS `dt`,
    count()
FROM `events`
GROUP BY `dt`;
