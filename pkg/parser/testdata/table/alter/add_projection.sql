ALTER TABLE `analytics`.`events`
    ADD PROJECTION `user_stats` (SELECT
    `user_id`,
    count() AS `event_count`
GROUP BY `user_id`);
