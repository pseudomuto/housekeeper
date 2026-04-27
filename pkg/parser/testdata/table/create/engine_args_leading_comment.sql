CREATE TABLE `replicated_events` (
    `id`  UInt64,
    `ver` UInt64
)
ENGINE = ReplicatedReplacingMergeTree(
    -- shared keeper path keeps replicas in sync
    '/clickhouse/tables/{shard}/{database}/replicated_events',
    '{replica}',
    `ver`
)
ORDER BY `id`;
