ALTER TABLE `analytics`.`events`
    FETCH PARTITION '202301' FROM '/clickhouse/tables/events';
