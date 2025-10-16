-- Test ReplicatedMergeTree parameter handling
-- When target schema has ReplicatedMergeTree() with no parameters,
-- it should be considered equal to any ReplicatedMergeTree with parameters from ClickHouse

-- Current state (simulating what ClickHouse returns):
CREATE TABLE users (
    id UInt64,
    name String
) ENGINE = ReplicatedMergeTree('/clickhouse/tables/{uuid}/{shard}', '{replica}')
ORDER BY id;

-- Target state:
CREATE TABLE users (
    id UInt64,
    name String
) ENGINE = ReplicatedMergeTree()
ORDER BY id;