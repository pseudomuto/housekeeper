CREATE MATERIALIZED VIEW `analytics`.`mv_joins`
ENGINE = MergeTree() ORDER BY (`date`, `category`)
AS SELECT
    toDate(`e`.`timestamp`) AS `date`,
    `e`.`user_id`,
    `u`.`name` AS `user_name`,
    `e`.`category`,
    count() AS `event_count`,
    sum(`e`.`value`) AS `total_value`
FROM `events` AS `e`
LEFT JOIN `users` AS `u` ON `e`.`user_id` = `u`.`id`
WHERE `e`.`status` = 'completed'
GROUP BY `date`, `e`.`user_id`, `u`.`name`, `e`.`category`;
