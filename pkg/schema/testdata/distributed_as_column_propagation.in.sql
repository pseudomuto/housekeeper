-- Test case: Distributed table using AS should get DROP+CREATE via propagation (not ALTER from main loop)
-- This prevents duplicate diffs for the same Distributed table when the source table has column changes
-- Current state: local table and distributed table without the new column
CREATE TABLE analytics.events_local (
    id UInt64,
    timestamp DateTime,
    name String
) ENGINE = MergeTree()
ORDER BY (id, timestamp);

CREATE TABLE analytics.events AS analytics.events_local
ENGINE = Distributed('cluster', 'analytics', 'events_local', rand());

-- Target state: local table has a new column, distributed table should be recreated to match
CREATE TABLE analytics.events_local (
    id UInt64,
    timestamp DateTime,
    name String,
    clicked_text String
) ENGINE = MergeTree()
ORDER BY (id, timestamp);

CREATE TABLE analytics.events AS analytics.events_local
ENGINE = Distributed('cluster', 'analytics', 'events_local', rand());
