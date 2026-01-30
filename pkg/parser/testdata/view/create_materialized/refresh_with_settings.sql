CREATE MATERIALIZED VIEW `mv_refresh_settings`
REFRESH EVERY 4 MINUTE
APPEND TO `target_table`
AS SELECT
    `domain`,
    `session_id`
FROM `events`
GROUP BY `domain`, `session_id`
SETTINGS max_bytes_before_external_group_by = 3000000000;
