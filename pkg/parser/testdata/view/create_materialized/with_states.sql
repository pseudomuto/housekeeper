CREATE MATERIALIZED VIEW `metrics`.`mv_user_stats_state`
TO `metrics`.`user_stats_aggregated`
AS SELECT
    toDate(`timestamp`) AS `date`,
    `user_id`,
    sumState(`amount`) AS `total_amount_state`,
    avgState(`duration`) AS `avg_duration_state`,
    uniqState(`session_id`) AS `unique_sessions_state`
FROM `raw_events`
GROUP BY `date`, `user_id`;
