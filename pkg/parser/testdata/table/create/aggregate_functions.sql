CREATE TABLE `sessions`.`web_vital_events_by_hour` (
    `received_at`     DateTime CODEC(DoubleDelta),
    `pv_domain`       LowCardinality(String),
    `vital_name`      LowCardinality(String),
    `value_avg`       AggregateFunction(avg, Float64),
    `value_quantiles` AggregateFunction(quantiles(0.5, 0.75, 0.9, 0.95, 0.99), Float64),
    `count`           AggregateFunction(sum, UInt32),
    `users`           AggregateFunction(uniq, UUID)
)
ENGINE = Distributed('datawarehouse', 'sessions', 'web_vital_events_by_hour_local', rand());
