CREATE TABLE `archived_events` (
    `id` UInt64,
    `ts` DateTime
)
ENGINE = MergeTree()
ORDER BY `id`
TTL `ts` + INTERVAL 1 YEAR DELETE;
