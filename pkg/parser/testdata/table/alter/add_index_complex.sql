ALTER TABLE `events`
    ADD INDEX `user_date_idx` (`user_id`, toDate(`timestamp`)) TYPE `minmax` GRANULARITY 1;
