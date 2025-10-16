-- Test ReplicatedMergeTree with explicit different parameters
-- When target schema has explicit ReplicatedMergeTree parameters that differ from current,
-- it should generate a migration

-- Current state:
CREATE TABLE events (
    id UInt64,
    data String
) ENGINE = ReplicatedMergeTree('/clickhouse/tables/old_path/{shard}', '{replica}')
ORDER BY id;

-- Target state:
CREATE TABLE events (
    id UInt64,
    data String
) ENGINE = ReplicatedMergeTree('/clickhouse/tables/new_path/{shard}', '{replica}')
ORDER BY id;