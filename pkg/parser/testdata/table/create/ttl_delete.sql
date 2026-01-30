CREATE TABLE `logs` (
    `id`        UInt64,
    `message`   String,
    `timestamp` DateTime
)
ENGINE = MergeTree()
ORDER BY (`id`, `timestamp`)
TTL `timestamp` + INTERVAL 30 DAY DELETE;
