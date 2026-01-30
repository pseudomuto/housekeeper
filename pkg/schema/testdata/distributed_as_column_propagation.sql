ALTER TABLE `analytics`.`events_local`
    ADD COLUMN `clicked_text` String;

-- Recreate to match schema changes from analytics.events_local

DROP TABLE `analytics`.`events`;

CREATE TABLE `analytics`.`events` (
    `id`           UInt64,
    `timestamp`    DateTime,
    `name`         String,
    `clicked_text` String
)
ENGINE = Distributed('cluster', 'analytics', 'events_local', rand());