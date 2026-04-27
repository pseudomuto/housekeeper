CREATE TABLE `replicated_events` (
    `id`  UInt64,
    `ver` UInt64
)
ENGINE = ReplicatedReplacingMergeTree(
    '/clickhouse/tables/{shard}/replicated_events',
    -- per-replica identity from server macros
    '{replica}',
    -- monotonic version column for replacement
    `ver`
)
ORDER BY `id`;
