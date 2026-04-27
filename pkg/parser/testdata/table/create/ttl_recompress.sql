CREATE TABLE `compressed_events` (
    `id` UInt64,
    `ts` DateTime
)
ENGINE = MergeTree()
ORDER BY `id`
TTL `ts` + INTERVAL 1 DAY RECOMPRESS CODEC(ZSTD(9));
