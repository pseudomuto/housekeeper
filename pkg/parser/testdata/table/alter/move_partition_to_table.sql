ALTER TABLE `analytics`.`events`
    MOVE PARTITION '202301' TO TABLE `analytics`.`events_archive`;
