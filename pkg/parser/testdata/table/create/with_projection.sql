CREATE TABLE `test_projections` (
    `id`        UInt64,
    `name`      String,
    `timestamp` DateTime,
    
)
ENGINE = MergeTree()
ORDER BY `id`;
