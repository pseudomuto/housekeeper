CREATE MATERIALIZED VIEW `mv_refresh_after`
REFRESH AFTER 1 HOUR
AS SELECT count() AS `cnt`
FROM `events`;
