# Complex Migrations

Advanced migration scenarios and patterns for complex ClickHouse schema changes.

## Overview

This guide covers advanced migration scenarios that go beyond simple table additions or column modifications. These patterns help handle complex schema transformations safely and efficiently.

## Multi-Database Migrations

### Cross-Database Dependencies

When migrations involve multiple databases with dependencies:

```sql
-- schemas/analytics/schema.sql
CREATE DATABASE analytics ENGINE = Atomic;

-- housekeeper:import tables/events.sql
-- housekeeper:import tables/users.sql
-- housekeeper:import views/user_metrics.sql
```

```sql
-- schemas/reporting/schema.sql
CREATE DATABASE reporting ENGINE = Atomic;

-- This view depends on analytics database
-- housekeeper:import views/executive_dashboard.sql
```

### Migration Ordering

Ensure proper database creation order in main entrypoint:

```sql
-- db/main.sql
-- 1. Reference data (no dependencies)
-- housekeeper:import schemas/reference/schema.sql

-- 2. Core data (limited dependencies) 
-- housekeeper:import schemas/analytics/schema.sql

-- 3. Derived data (depends on analytics)
-- housekeeper:import schemas/reporting/schema.sql
```

## Large Table Restructuring

### Column Type Changes

For large tables, column type changes require careful planning:

```sql
-- Phase 1: Add new column
ALTER TABLE events ADD COLUMN user_id_new UInt64;

-- Phase 2: Populate new column (done in batches by application)
-- UPDATE events SET user_id_new = toUInt64(user_id) WHERE user_id_new = 0;

-- Phase 3: Update application to use new column

-- Phase 4: Drop old column (separate migration)
-- ALTER TABLE events DROP COLUMN user_id;

-- Phase 5: Rename new column (separate migration)
-- ALTER TABLE events RENAME COLUMN user_id_new TO user_id;
```

### Table Engine Changes

Engine changes require full table recreation:

```sql
-- Current: Standard MergeTree
CREATE TABLE events (...) ENGINE = MergeTree() ORDER BY timestamp;

-- Target: ReplacingMergeTree (requires recreation)
-- 1. Create temporary table with new engine
CREATE TABLE events_new (...) 
ENGINE = ReplacingMergeTree(updated_at) ORDER BY timestamp;

-- 2. Copy data (done by application/ETL)
-- INSERT INTO events_new SELECT *, now() FROM events;

-- 3. Atomic swap (single migration)
-- RENAME TABLE events TO events_old, events_new TO events;

-- 4. Clean up old table (separate migration after validation)
-- DROP TABLE events_old;
```

## Materialized View Migrations

### Query Changes

Materialized views require DROP+CREATE for query modifications:

```sql
-- Current materialized view
CREATE MATERIALIZED VIEW daily_stats
ENGINE = SummingMergeTree((event_count))
ORDER BY date
AS SELECT 
    toDate(timestamp) as date,
    count() as event_count
FROM events
GROUP BY date;

-- New requirement: Add user segmentation
-- This requires DROP+CREATE approach

-- Generated migration:
DROP TABLE daily_stats;

CREATE MATERIALIZED VIEW daily_stats
ENGINE = SummingMergeTree((event_count, user_count))  
ORDER BY (date, segment)
AS SELECT 
    toDate(timestamp) as date,
    getUserSegment(user_id) as segment,
    count() as event_count,
    uniq(user_id) as user_count
FROM events
GROUP BY date, segment;
```

### Data Backfill

When changing materialized views, consider data backfill:

```sql
-- 1. Create new materialized view
CREATE MATERIALIZED VIEW daily_stats_v2 ...

-- 2. Backfill historical data (application responsibility)
-- INSERT INTO daily_stats_v2 
-- SELECT ... FROM events WHERE date < today();

-- 3. Switch application to new view

-- 4. Drop old view (separate migration)
-- DROP TABLE daily_stats;

-- 5. Rename new view (separate migration)
-- RENAME TABLE daily_stats_v2 TO daily_stats;
```

## Dictionary Migrations

### Source Changes

Dictionary modifications use CREATE OR REPLACE:

```sql
-- Current dictionary
CREATE DICTIONARY user_segments_dict (
    user_id UInt64,
    segment String
) PRIMARY KEY user_id
SOURCE(HTTP(url 'http://api-v1.example.com/segments'))
LAYOUT(HASHED())
LIFETIME(3600);

-- API endpoint change requires dictionary update
CREATE OR REPLACE DICTIONARY user_segments_dict (
    user_id UInt64, 
    segment String,
    updated_at DateTime  -- New field
) PRIMARY KEY user_id
SOURCE(HTTP(url 'http://api-v2.example.com/segments'))
LAYOUT(HASHED())
LIFETIME(1800);  -- Shorter refresh
```

## Cluster Migrations

### Rolling Updates

For cluster deployments, coordinate migrations carefully:

```sql
-- Phase 1: Schema changes (all nodes)
ALTER TABLE events ON CLUSTER production 
ADD COLUMN session_id String DEFAULT '';

-- Phase 2: Application deployment
-- Deploy application changes to use new column

-- Phase 3: Data migration (if needed)
-- Populate historical data in new column

-- Phase 4: Make column required (if needed)
-- ALTER TABLE events ON CLUSTER production
-- MODIFY COLUMN session_id String;  -- Remove DEFAULT
```

### Replication Considerations

For replicated tables, ensure consistency:

```sql
-- Replicated table modifications
ALTER TABLE events ON CLUSTER production
ADD COLUMN new_field String DEFAULT '';

-- Verify replication status
-- SELECT * FROM system.replicated_fetches;
-- SELECT * FROM system.replication_queue;
```

## Performance Optimization Migrations

### Index Addition

Add indexes without blocking:

```sql
-- Add skipping index
ALTER TABLE events 
ADD INDEX idx_user_type user_type TYPE set(1000) GRANULARITY 3;

-- Index builds in background, no downtime
```

### Partitioning Changes

Modify partitioning for better performance:

```sql
-- Current: Monthly partitioning
PARTITION BY toYYYYMM(timestamp)

-- Target: Daily partitioning for recent data
-- Requires table recreation for existing data
-- New data uses new partitioning automatically

-- 1. Create new table with daily partitioning
CREATE TABLE events_new (...) 
ENGINE = MergeTree()
PARTITION BY toDate(timestamp)  -- Daily partitioning
ORDER BY timestamp;

-- 2. Migrate recent data (application managed)
-- 3. Historical data can stay in old partitioning
-- 4. Atomic switch when ready
```

## Error Recovery

### Failed Migrations

Recovery strategies for failed migrations:

```sql
-- If ALTER operation fails:
-- 1. Check system.mutations for status
SELECT * FROM system.mutations WHERE table = 'events';

-- 2. Cancel stuck mutations if needed  
KILL MUTATION WHERE mutation_id = 'mutation_123';

-- 3. Retry or rollback as appropriate
```

### Rollback Procedures

```sql
-- For additive changes (safe rollback)
ALTER TABLE events DROP COLUMN IF EXISTS new_column;

-- For destructive changes (requires backup restore)
-- 1. Stop application writes
-- 2. Restore from backup
-- 3. Replay recent transactions from logs
-- 4. Resume application
```

## Testing Complex Migrations

### Integration Testing

```bash
# 1. Start test environment
housekeeper dev up

# 2. Apply migration
housekeeper diff  

# 3. Validate schema
housekeeper schema dump --url localhost:9000 > applied.sql

# 4. Run application tests
npm test  # or your test suite

# 5. Performance testing
clickhouse-benchmark --queries-file test_queries.sql
```

### Production Validation

```bash
# Pre-migration checklist:
# 1. Full backup completed
# 2. Migration tested in staging  
# 3. Application code deployed and ready
# 4. Rollback procedure documented
# 5. Monitoring alerts configured

# Apply migration
housekeeper diff

# Post-migration validation:
# 1. Schema matches expectation
# 2. Application functionality verified
# 3. Query performance acceptable
# 4. Replication lag normal (if applicable)
```

## Best Practices

### Planning Complex Migrations

1. **Break into phases** - Split large changes into smaller, reversible steps
2. **Test thoroughly** - Use staging environments that mirror production
3. **Monitor actively** - Watch performance and error metrics during migration
4. **Have rollback plan** - Document exact rollback procedures before starting
5. **Coordinate with application** - Ensure application changes align with schema changes

### Communication

1. **Document impact** - Clearly describe what will change and potential downtime
2. **Schedule appropriately** - Plan migrations during low-traffic periods
3. **Notify stakeholders** - Keep teams informed of migration progress
4. **Post-migration review** - Document lessons learned for future migrations

Complex migrations require careful planning, testing, and coordination. Always prioritize data safety and system stability over migration speed.