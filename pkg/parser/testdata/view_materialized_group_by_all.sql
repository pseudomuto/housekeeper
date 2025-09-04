CREATE MATERIALIZED VIEW `demo`.`events_by_hour_mv` ON CLUSTER `test`
TO `demo`.`events_by_hour_local`
AS SELECT
    toStartOfHour(`event_received_at`) AS `received_at`,
    `domain` AS `pv_domain`,
    `browser` AS `pv_browser`,
    `browser_version` AS `pv_browser_version`,
    `os` AS `pv_os`,
    `os_version` AS `pv_os_version`,
    `country_code` AS `pv_country_code`,
    `region_code` AS `pv_region_code`,
    `event_url` AS `current_url`,
    sumState(toUInt32(1)) AS `count`,
    uniqState(`browser_id`) AS `users`,
    uniqState(`page_visit_id`) AS `visits`
FROM `demo`.`events`
WHERE `event_type` = 'browser'
GROUP BY ALL;
