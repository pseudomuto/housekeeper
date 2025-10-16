DROP TABLE `events`;

CREATE TABLE `events` (
    `id`   UInt64,
    `data` String
)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/new_path/{shard}', '{replica}')
ORDER BY `id`;