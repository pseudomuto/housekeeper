CREATE OR REPLACE TABLE IF NOT EXISTS `analytics`.`events` ON CLUSTER `production` (
    `id`         UInt64,
    `user_id`    UInt64,
    `event_type` LowCardinality(String),
    `timestamp`  DateTime DEFAULT now(),
    `data`       Map(String, String) DEFAULT map(),
    `metadata`   Nullable(String) CODEC(ZSTD),
    `tags`       Array(String) DEFAULT array(),
    `location`   Tuple(`lat` Float64, `lon` Float64),
    `settings`   Nested(`key` String, `value` String),
    `temp_data`  String TTL `timestamp` + days(30) COMMENT 'Temporary data'
)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/events', '{replica}')
ORDER BY (`user_id`, `timestamp`)
PARTITION BY toYYYYMM(`timestamp`)
PRIMARY KEY `user_id`
SAMPLE BY `id`
TTL `timestamp` + INTERVAL 1 YEAR
SETTINGS index_granularity = 8192, merge_with_ttl_timeout = 3600
COMMENT 'User events table';
