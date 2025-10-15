CREATE TABLE `events` (
    `id`       UInt64,
    `metadata` Nested(`source` String, `tags` Array(String))
)
ENGINE = MergeTree()
ORDER BY `id`;