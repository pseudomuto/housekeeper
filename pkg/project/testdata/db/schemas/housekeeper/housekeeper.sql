-- This database is used internally by housekeeper to track migration execution
-- The expected case is that you choose a particular shard in your cluster for this table. Internally, housekeeper will
-- query this table via `SELECT ... FROM housekeeper.revisions` (note the lack of table functions here - cluster,
-- clusterAllReplicas, etc.). This means it's safe to update this table to be ReplicatedMergeTree if you like, but it
-- should be shard-local and not defined `ON CLUSTER`. For distributed setups, only one shard needs this tracking table.

CREATE DATABASE `housekeeper`
ENGINE = Atomic
COMMENT 'Housekeeper migration tracking database';

CREATE TABLE `housekeeper`.`revisions` (
    `version` String COMMENT 'The version (e.g. 20250101123045)',
    `executed_at` DateTime(3, 'UTC') COMMENT 'The UTC time at which this attempt was executed',
    `execution_time_ms` UInt64 COMMENT 'How long the migration took to run',
    `type` String COMMENT 'The type of migration this is (normal, checkpoint, etc)',
    `error` Nullable(String) COMMENT 'The error message from the last attempt (if any)',
    `applied` UInt32 COMMENT 'The number of applied statements',
    `total` UInt32 COMMENT 'The total number of statements in the migration',
    `hash` String COMMENT 'The h1 hash of the migration',
    `partial_hashes` Array(String) COMMENT 'h1 hashes for each statement in the migration',
    `housekeeper_version` String COMMENT 'The version of housekeeper used to run the migration'
)
ENGINE = MergeTree()
ORDER BY version
PARTITION BY toYYYYMM(executed_at)
COMMENT 'Table used to track migrations';
