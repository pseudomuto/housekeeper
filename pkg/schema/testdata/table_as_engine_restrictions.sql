CREATE TABLE `dist_copy` (
    `id`        UInt64,
    `timestamp` DateTime,
    `name`      String
)
ENGINE = Distributed(`cluster`, currentDatabase(), `source_table`, rand());

CREATE TABLE `memory_copy` (
    `id`        UInt64,
    `timestamp` DateTime,
    `name`      String
)
ENGINE = Memory()
ORDER BY (`id`, `timestamp`)
PRIMARY KEY `id`;

CREATE TABLE `merge_copy` (
    `id`        UInt64,
    `timestamp` DateTime,
    `name`      String
)
ENGINE = MergeTree()
ORDER BY (`id`, `timestamp`)
PARTITION BY toYYYYMM(`timestamp`)
PRIMARY KEY `id`
SAMPLE BY `id`;