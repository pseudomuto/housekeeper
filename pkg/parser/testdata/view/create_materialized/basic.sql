CREATE MATERIALIZED VIEW `mv_daily_stats`
AS SELECT
    toDate(`timestamp`) AS `date`,
    count() AS `cnt`
FROM `events`
GROUP BY `date`;
