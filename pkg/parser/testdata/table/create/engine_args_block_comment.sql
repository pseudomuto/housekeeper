CREATE TABLE `replicated_events` (
    `id`  UInt64,
    `ver` UInt64
)
ENGINE = ReplicatedReplacingMergeTree(
    /* keeper path */
    '/p',
    '{replica}',
    `ver`
)
ORDER BY `id`;
