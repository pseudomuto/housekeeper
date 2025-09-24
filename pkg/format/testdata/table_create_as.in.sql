-- Test CREATE TABLE AS formatting

-- Basic AS syntax
CREATE TABLE copy AS source ENGINE = MergeTree() ORDER BY id;

-- AS with ON CLUSTER
CREATE TABLE events_all ON CLUSTER prod AS events_local ENGINE = Distributed(prod, db, events_local, rand());

-- AS with all options
CREATE OR REPLACE TABLE IF NOT EXISTS db.table_all ON CLUSTER cluster AS db.table_local
ENGINE = Distributed(cluster, db, table_local, xxHash64(id))
SETTINGS distributed_product_mode = 'global'
COMMENT 'Distributed table';

-- AS with backticks
CREATE TABLE `backup-table` AS `source-table` ENGINE = MergeTree() ORDER BY `id`;

-- Mixed regular and AS tables
CREATE TABLE base (
    id UInt64,
    name String,
    email String DEFAULT ''
) ENGINE = MergeTree() ORDER BY id;

CREATE TABLE base_copy AS base ENGINE = MergeTree() ORDER BY id;