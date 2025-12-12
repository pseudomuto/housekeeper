ALTER TABLE `analytics`.`events`
    MODIFY TTL `timestamp` + days(30);
