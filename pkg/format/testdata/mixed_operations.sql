CREATE DATABASE `analytics` ENGINE = Atomic;

CREATE TABLE `analytics`.`raw_events` (
    `id`   UInt64,
    `data` String,
    `ts`   DateTime
)
ENGINE = MergeTree()
ORDER BY `ts`;

CREATE DICTIONARY `analytics`.`lookup` (
    `key`   UInt64,
    `value` String
)
PRIMARY KEY `key`
SOURCE(HTTP(url 'http://api.test.com/data'))
LAYOUT(HASHED())
LIFETIME(3600);

CREATE MATERIALIZED VIEW `analytics`.`processed_events`
ENGINE = MergeTree() ORDER BY (`date`, `id`)
AS SELECT
    `id`,
    JSONExtractString(`data`, 'event') AS `event_type`,
    toDate(`ts`) AS `date`
FROM `analytics`.`raw_events`
WHERE `event_type` != '';

ALTER TABLE `analytics`.`raw_events`
    ADD COLUMN `processed` UInt8 DEFAULT 0;

RENAME TABLE `analytics`.`raw_events` TO `analytics`.`events_raw`;

DROP VIEW IF EXISTS `analytics`.`old_view`;
