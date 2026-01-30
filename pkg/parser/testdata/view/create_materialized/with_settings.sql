CREATE MATERIALIZED VIEW `mv_with_settings`
AS SELECT
    `domain`,
    `session_id`,
    count() AS `cnt`
FROM `events`
GROUP BY `domain`, `session_id`
SETTINGS max_bytes_before_external_group_by = 3000000000, max_memory_usage = 10000000000;
