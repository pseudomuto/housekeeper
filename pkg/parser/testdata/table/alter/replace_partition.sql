ALTER TABLE `analytics`.`events`
    REPLACE PARTITION '202301' FROM `analytics`.`events_backup`;
