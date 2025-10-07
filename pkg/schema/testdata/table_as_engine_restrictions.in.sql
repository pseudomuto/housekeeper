-- Test CREATE TABLE AS with engine restrictions
-- Current state: Only source table exists
CREATE TABLE source_table (
    id UInt64,
    timestamp DateTime,
    name String
) ENGINE = MergeTree()
ORDER BY (id, timestamp)
PARTITION BY toYYYYMM(timestamp)
PRIMARY KEY id
SAMPLE BY id;

-- Target state: Add AS tables with different engines
CREATE TABLE source_table (
    id UInt64,
    timestamp DateTime,
    name String
) ENGINE = MergeTree()
ORDER BY (id, timestamp)
PARTITION BY toYYYYMM(timestamp)
PRIMARY KEY id
SAMPLE BY id;

-- Distributed engine - should NOT copy ORDER BY, PARTITION BY, PRIMARY KEY, or SAMPLE BY
CREATE TABLE dist_copy AS source_table 
ENGINE = Distributed(cluster, currentDatabase(), source_table, rand());

-- Memory engine - should copy ORDER BY and PRIMARY KEY but NOT PARTITION BY or SAMPLE BY
CREATE TABLE memory_copy AS source_table
ENGINE = Memory;

-- MergeTree engine - should copy all clauses from source
CREATE TABLE merge_copy AS source_table
ENGINE = MergeTree();