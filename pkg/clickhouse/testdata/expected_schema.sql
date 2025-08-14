CREATE DATABASE `analytics` ON CLUSTER `test_cluster` ENGINE = Atomic COMMENT 'Analytics database for events and metrics';

CREATE DATABASE `user_data` ON CLUSTER `test_cluster` ENGINE = Atomic COMMENT 'User-related data storage';

CREATE TABLE `analytics`.`daily_summary` ON CLUSTER `test_cluster` (
    `date`                 Date,
    `total_events`         UInt64,
    `unique_users`         UInt64,
    `avg_session_duration` Float64
)
ENGINE = ReplicatedSummingMergeTree('/clickhouse/tables/{shard}/analytics/daily_summary', '{replica}')
ORDER BY `date`
PARTITION BY toYYYYMM(`date`)
SETTINGS index_granularity = 8192;

CREATE TABLE `analytics`.`events` ON CLUSTER `test_cluster` (
    `id`         UInt64,
    `user_id`    UInt64,
    `event_type` LowCardinality(String),
    `timestamp`  DateTime DEFAULT now(),
    `properties` Map(String, String) DEFAULT map(),
    `session_id` String,
    `page_url`   String,
    `user_agent` String CODEC(ZSTD(1))
)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/analytics/events', '{replica}')
ORDER BY (`user_id`, `timestamp`)
PARTITION BY toYYYYMM(`timestamp`)
TTL `timestamp` + toIntervalYear(2)
SETTINGS index_granularity = 8192;

CREATE TABLE `analytics`.`user_sessions` ON CLUSTER `test_cluster` (
    `session_id`       String,
    `user_id`          UInt64,
    `start_time`       DateTime,
    `end_time`         DateTime,
    `page_views`       UInt32 DEFAULT 0,
    `duration_seconds` UInt32 MATERIALIZED dateDiff('second', `start_time`, `end_time`),
    `device_type`      LowCardinality(String),
    `country_code`     FixedString(2)
)
ENGINE = ReplicatedReplacingMergeTree('/clickhouse/tables/{shard}/analytics/user_sessions', '{replica}', `end_time`)
ORDER BY (`user_id`, `start_time`)
PARTITION BY toYYYYMM(`start_time`)
SETTINGS index_granularity = 4096;

CREATE TABLE `user_data`.`profiles` ON CLUSTER `test_cluster` (
    `user_id`    UInt64,
    `email`      String,
    `name`       String,
    `created_at` DateTime,
    `updated_at` DateTime DEFAULT now(),
    `status`     LowCardinality(String) DEFAULT 'active',
    `metadata`   Nullable(String),
    `tags`       Array(String) DEFAULT []
)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/user_data/profiles', '{replica}')
ORDER BY `user_id`
SETTINGS index_granularity = 8192;

CREATE DICTIONARY `analytics`.`country_dict` ON CLUSTER `test_cluster` (
    `code`       String IS_OBJECT_ID,
    `name`       String INJECTIVE,
    `continent`  String,
    `population` UInt64 DEFAULT 0
)
PRIMARY KEY `code`
SOURCE(HTTP(URL 'http://example.com/countries.json' FORMAT 'JSONEachRow'))
LAYOUT(COMPLEX_KEY_HASHED(SIZE_IN_CELLS 1000))
LIFETIME(MIN 86400 MAX 172800)
COMMENT 'Country code mapping for analytics';

CREATE DICTIONARY `user_data`.`user_segments_dict` ON CLUSTER `test_cluster` (
    `user_id` UInt64 IS_OBJECT_ID,
    `segment` String,
    `tier`    String DEFAULT 'free',
    `score`   Float32 DEFAULT 0.0
)
PRIMARY KEY `user_id`
SOURCE(CLICKHOUSE(HOST 'localhost' PORT 9000 DB 'user_data' TABLE 'user_segments_source'))
LAYOUT(COMPLEX_KEY_HASHED(SIZE_IN_CELLS 100000))
LIFETIME(3600)
SETTINGS(max_threads=2)
COMMENT 'User segmentation for targeting';

CREATE VIEW `analytics`.`daily_events` ON CLUSTER `test_cluster`
AS SELECT
    toDate(`timestamp`) AS `date`,
    `event_type`,
    count() AS `event_count`,
    uniq(`user_id`) AS `unique_users`,
    uniq(`session_id`) AS `unique_sessions`
FROM `analytics`.`events`
GROUP BY `date`, `event_type`
ORDER BY `date` DESC, `event_count` DESC;

CREATE MATERIALIZED VIEW `analytics`.`mv_hourly_stats` ON CLUSTER `test_cluster`
AS SELECT
    toStartOfHour(`timestamp`) AS `hour`,
    `event_type`,
    count() AS `event_count`,
    uniq(`user_id`) AS `unique_users`
FROM `analytics`.`events`
GROUP BY `hour`, `event_type`;

CREATE MATERIALIZED VIEW `analytics`.`mv_to_daily_summary` ON CLUSTER `test_cluster`
TO `analytics`.`daily_summary`
AS SELECT
    toDate(`timestamp`) AS `date`,
    count() AS `total_events`,
    uniq(`user_id`) AS `unique_users`,
    0. AS `avg_session_duration`
FROM `analytics`.`events`
GROUP BY `date`;

CREATE MATERIALIZED VIEW `analytics`.`mv_user_activity` ON CLUSTER `test_cluster`
AS SELECT
    `user_id`,
    toDate(`timestamp`) AS `date`,
    countState() AS `event_count`,
    uniqState(`session_id`) AS `session_count`,
    maxState(`timestamp`) AS `last_activity`
FROM `analytics`.`events`
GROUP BY `user_id`, `date`;

CREATE VIEW `user_data`.`active_users` ON CLUSTER `test_cluster`
AS SELECT
    `user_id`,
    `email`,
    `name`,
    `created_at`,
    `status`
FROM `user_data`.`profiles`
WHERE `status` = 'active'
ORDER BY `created_at` DESC;