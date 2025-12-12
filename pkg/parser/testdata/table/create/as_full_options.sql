CREATE OR REPLACE TABLE IF NOT EXISTS `analytics`.`events_all` ON CLUSTER `analytics_cluster` AS `analytics`.`events_local`
ENGINE = Distributed(`analytics_cluster`, `analytics`, `events_local`, cityHash64(`user_id`))
SETTINGS index_granularity = 8192
COMMENT 'Distributed view of events_local';
