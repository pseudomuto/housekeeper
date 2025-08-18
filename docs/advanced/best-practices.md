# Best Practices

Production-proven guidelines for using Housekeeper effectively in real-world ClickHouse deployments.

## Project Organization

### Schema Structure

#### Use Modular Organization
```
db/
├── main.sql                    # Orchestration file
├── schemas/
│   ├── core/                   # Essential business objects
│   │   ├── databases.sql
│   │   ├── users.sql
│   │   └── events.sql
│   ├── analytics/              # Analytical objects
│   │   ├── aggregations.sql
│   │   ├── reports.sql
│   │   └── materialized_views.sql
│   ├── reference/              # Lookup data
│   │   ├── countries.sql
│   │   ├── categories.sql
│   │   └── segments.sql
│   └── experimental/           # New features
│       ├── ml_features.sql
│       └── test_tables.sql
└── migrations/                 # Generated migrations
    ├── 20240101120000.sql
    └── housekeeper.sum
```

#### Import Order Matters
```sql
-- db/main.sql - Order by dependencies
-- 1. Databases first
-- housekeeper:import schemas/core/databases.sql

-- 2. Core tables (no dependencies)
-- housekeeper:import schemas/core/users.sql
-- housekeeper:import schemas/core/events.sql

-- 3. Reference data and dictionaries
-- housekeeper:import schemas/reference/countries.sql
-- housekeeper:import schemas/reference/categories.sql

-- 4. Dependent objects (views, materialized views)
-- housekeeper:import schemas/analytics/aggregations.sql
-- housekeeper:import schemas/analytics/materialized_views.sql
```

### Environment Management

#### Environment-Specific Configurations
```yaml
# environments/production.yaml
clickhouse:
  version: "25.7"                    # Pin specific version
  cluster: "production_cluster"

connection:
  host: clickhouse-prod.example.com
  port: 9440
  secure: true                       # Always use TLS in production
  username: migration_user
  password: "${CH_PROD_PASSWORD}"    # Environment variable

migration:
  auto_approve: false                # Never auto-approve in production
  backup_before: true                # Always backup production
  timeout: 1800s                     # Longer timeout for large migrations
```

#### CI/CD Integration
```yaml
# .github/workflows/schema-validation.yml
name: Schema Validation

on:
  pull_request:
    paths:
      - 'db/**'
      - 'housekeeper.yaml'

jobs:
  validate:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      
      - name: Setup Housekeeper
        run: |
          go install github.com/pseudomuto/housekeeper@latest
          
      - name: Validate Schema Syntax
        run: housekeeper schema compile
        
      - name: Test Migration Generation
        run: |
          # Start test ClickHouse
          docker run -d --name test-ch -p 9000:9000 clickhouse/clickhouse-server:25.7
          sleep 10
          
          # Generate and validate migration
          housekeeper diff --url localhost:9000 --dry-run
          
      - name: Cleanup
        run: docker rm -f test-ch
```

## Schema Design

### Table Design Patterns

#### Time-Series Tables
```sql
-- Optimal design for high-volume event data
CREATE TABLE analytics.events (
    timestamp DateTime64(3),              -- Millisecond precision
    user_id UInt64,
    event_type LowCardinality(String),    -- Limited set of values
    session_id String,
    properties Map(String, String),
    
    -- Materialized columns for performance
    date Date MATERIALIZED toDate(timestamp),
    hour UInt8 MATERIALIZED toHour(timestamp)
)
ENGINE = MergeTree()
PARTITION BY (toYYYYMM(timestamp), event_type)  -- Multi-dimensional partitioning
ORDER BY (timestamp, user_id)                   -- Most selective columns first
TTL timestamp + INTERVAL 90 DAY                 -- Data lifecycle management
SETTINGS index_granularity = 8192;
```

#### Mutable Data Tables
```sql
-- User profiles with update tracking
CREATE TABLE users.profiles (
    id UInt64,
    email String,
    name String,
    status LowCardinality(String),
    metadata Map(String, String),
    created_at DateTime,
    updated_at DateTime                   -- Version column for ReplacingMergeTree
)
ENGINE = ReplacingMergeTree(updated_at)
ORDER BY id
SETTINGS index_granularity = 8192;
```

#### Aggregation Tables
```sql
-- Pre-aggregated data for fast queries
CREATE TABLE analytics.daily_user_stats (
    date Date,
    user_id UInt64,
    event_count UInt32,
    session_count UInt32,
    revenue Decimal64(2)
)
ENGINE = SummingMergeTree((event_count, session_count, revenue))
ORDER BY (date, user_id)
SETTINGS index_granularity = 8192;
```

### Performance Optimization

#### Ordering Key Strategy
```sql
-- Query pattern: Filter by user, then time range
-- Good: ORDER BY (user_id, timestamp)
SELECT * FROM events WHERE user_id = 123 AND timestamp >= '2024-01-01';

-- Query pattern: Time range analysis across users  
-- Good: ORDER BY (timestamp, event_type, user_id)
SELECT event_type, count(*) FROM events 
WHERE timestamp BETWEEN '2024-01-01' AND '2024-01-02' 
GROUP BY event_type;
```

#### Partitioning Strategy
```sql
-- Good: Balanced partition sizes (100M-1B rows per partition)
PARTITION BY toYYYYMM(timestamp)          -- Monthly for moderate volume

-- Good: Multi-dimensional for query pruning
PARTITION BY (toYYYYMM(timestamp), event_type)  -- When filtering by both

-- Avoid: Too granular (too many small partitions)
-- PARTITION BY toDate(timestamp)         -- Daily might be too granular

-- Avoid: Unbalanced partitions
-- PARTITION BY user_id                   -- Likely very unbalanced
```

#### Index Optimization
```sql
-- Add skipping indexes for common filters
ALTER TABLE analytics.events 
ADD INDEX idx_user_id user_id TYPE minmax GRANULARITY 4;

ALTER TABLE analytics.events 
ADD INDEX idx_event_type event_type TYPE set(100) GRANULARITY 1;

-- For string prefix searches
ALTER TABLE analytics.events 
ADD INDEX idx_url_prefix page_url TYPE tokenbf_v1(32768, 3, 0) GRANULARITY 1;
```

### Data Types Best Practices

#### Choose Optimal Types
```sql
CREATE TABLE optimized_table (
    -- Use smallest sufficient numeric types
    id UInt64,                            -- Large range needed
    age UInt8,                           -- 0-255 sufficient
    price Decimal64(2),                  -- Financial precision
    
    -- Optimize string storage
    status LowCardinality(String),       -- Limited values
    country_code FixedString(2),         -- Known length
    description String,                  -- Variable length
    
    -- Use appropriate temporal precision
    created_at DateTime,                 -- Second precision
    event_time DateTime64(3),           -- Millisecond precision
    birth_date Date,                     -- Day precision
    
    -- Complex types for flexibility
    tags Array(String),                  -- Multiple values
    metadata Map(String, String),       -- Key-value pairs
    location Tuple(Float64, Float64),   -- Structured data
    
    -- Nullable only when necessary
    phone Nullable(String),             -- Optional field
    email String                        -- Required field (not nullable)
);
```

#### Avoid Anti-Patterns
```sql
-- ❌ Don't use Nullable(LowCardinality(...))
-- column Nullable(LowCardinality(String))  -- Invalid combination

-- ❌ Don't use String for numeric data
-- user_id String                           -- Should be UInt64

-- ❌ Don't use overly precise types when not needed
-- price Decimal128(38)                     -- Decimal64(2) usually sufficient

-- ❌ Don't use DateTime64 when DateTime suffices
-- created_at DateTime64(9)                 -- Usually DateTime is enough
```

## Migration Management

### Migration Workflow

#### Development Process
```bash
# 1. Make schema changes
vim db/schemas/analytics/events.sql

# 2. Validate syntax
housekeeper schema compile

# 3. Generate migration
housekeeper diff --config environments/development.yaml

# 4. Review generated SQL
cat db/migrations/20240315143000.sql

# 5. Test in development
housekeeper migrate --config environments/development.yaml

# 6. Commit schema and migration together
git add db/schemas/ db/migrations/
git commit -m "Add user_segment column to events table"
```

#### Production Deployment
```bash
# 1. Validate in staging
housekeeper migrate --config environments/staging.yaml --dry-run
housekeeper migrate --config environments/staging.yaml

# 2. Backup production
housekeeper backup --config environments/production.yaml

# 3. Deploy to production with approval
housekeeper migrate --config environments/production.yaml
```

### Migration Safety

#### Pre-Migration Checks
```bash
#!/bin/bash
# pre-migration-check.sh

set -e

echo "Running pre-migration checks..."

# 1. Validate configuration
housekeeper config validate --config production.yaml

# 2. Test database connection
housekeeper schema dump --config production.yaml --limit 1

# 3. Validate migration syntax
housekeeper diff --config production.yaml --dry-run

# 4. Check migration file integrity
housekeeper status --config production.yaml

# 5. Verify backup is recent
if [ ! -f "backup_$(date +%Y%m%d).sql" ]; then
    echo "ERROR: No recent backup found"
    exit 1
fi

echo "Pre-migration checks passed ✓"
```

#### Post-Migration Validation
```bash
#!/bin/bash
# post-migration-check.sh

set -e

echo "Running post-migration validation..."

# 1. Verify schema matches expectation
housekeeper schema dump --config production.yaml > current_schema.sql
housekeeper schema compile > expected_schema.sql

if ! diff -q current_schema.sql expected_schema.sql; then
    echo "WARNING: Schema differs from expectation"
    diff current_schema.sql expected_schema.sql
fi

# 2. Run basic queries to verify data integrity
clickhouse-client --query "SELECT count() FROM analytics.events"
clickhouse-client --query "SELECT max(timestamp) FROM analytics.events"

# 3. Check for any errors in ClickHouse logs
docker logs clickhouse-server 2>&1 | grep -i error | tail -10

echo "Post-migration validation complete ✓"
```

### Rollback Procedures

#### Rollback Strategy
```sql
-- For simple additions (safe to ignore)
ALTER TABLE analytics.events DROP COLUMN IF EXISTS new_column;

-- For complex changes (may require data migration)
-- 1. Stop application writes
-- 2. Restore from backup
-- 3. Apply reverse migration
-- 4. Verify data integrity
-- 5. Resume application
```

#### Rollback Script Template
```bash
#!/bin/bash
# rollback-migration.sh

MIGRATION_TIMESTAMP=$1
BACKUP_FILE=$2

if [ -z "$MIGRATION_TIMESTAMP" ] || [ -z "$BACKUP_FILE" ]; then
    echo "Usage: $0 <migration_timestamp> <backup_file>"
    exit 1
fi

echo "Rolling back migration $MIGRATION_TIMESTAMP..."

# 1. Create current state backup
housekeeper backup --output "pre_rollback_$(date +%Y%m%d_%H%M%S).sql"

# 2. Apply reverse migration
housekeeper migrate --rollback-to "$MIGRATION_TIMESTAMP"

# 3. Verify rollback
housekeeper status

echo "Rollback complete. Please verify data integrity."
```

## Cluster Management

### Cluster Configuration

#### Multi-Shard Setup
```xml
<!-- db/config.d/cluster.xml -->
<clickhouse>
    <remote_servers>
        <production_cluster>
            <shard>
                <replica>
                    <host>ch-shard1-replica1.internal</host>
                    <port>9000</port>
                </replica>
                <replica>
                    <host>ch-shard1-replica2.internal</host>
                    <port>9000</port>
                </replica>
            </shard>
            <shard>
                <replica>
                    <host>ch-shard2-replica1.internal</host>
                    <port>9000</port>
                </replica>
                <replica>
                    <host>ch-shard2-replica2.internal</host>
                    <port>9000</port>
                </replica>
            </shard>
        </production_cluster>
    </remote_servers>
</clickhouse>
```

#### Cluster-Aware Schema
```sql
-- All DDL includes ON CLUSTER for distributed execution
CREATE DATABASE analytics ON CLUSTER production_cluster 
ENGINE = Atomic;

CREATE TABLE analytics.events ON CLUSTER production_cluster (
    -- table definition
) ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/events', '{replica}')
ORDER BY timestamp;

-- Distributed table for queries across shards
CREATE TABLE analytics.events_distributed ON CLUSTER production_cluster
AS analytics.events
ENGINE = Distributed(production_cluster, analytics, events, rand());
```

### Cluster Deployment

#### Rolling Updates
```bash
#!/bin/bash
# rolling-cluster-update.sh

CLUSTER_NODES=("ch-node1" "ch-node2" "ch-node3" "ch-node4")

for node in "${CLUSTER_NODES[@]}"; do
    echo "Updating node: $node"
    
    # 1. Apply migration to single node
    housekeeper migrate --host "$node" --config production.yaml
    
    # 2. Verify node health
    clickhouse-client --host "$node" --query "SELECT version()"
    
    # 3. Wait for replication to catch up
    sleep 30
    
    echo "Node $node updated successfully"
done

echo "Cluster update complete"
```

## Monitoring and Observability

### Migration Monitoring

#### Log Monitoring
```yaml
# monitoring/migration-alerts.yaml
apiVersion: monitoring.coreos.com/v1
kind: PrometheusRule
metadata:
  name: housekeeper-alerts
spec:
  groups:
  - name: housekeeper
    rules:
    - alert: MigrationFailed
      expr: housekeeper_migration_status != 1
      for: 0m
      annotations:
        summary: "Housekeeper migration failed"
        description: "Migration {{ $labels.migration }} failed on {{ $labels.instance }}"
        
    - alert: MigrationTimeout
      expr: housekeeper_migration_duration_seconds > 1800
      for: 0m
      annotations:
        summary: "Migration taking too long"
        description: "Migration {{ $labels.migration }} has been running for {{ $value }} seconds"
```

#### Health Checks
```bash
#!/bin/bash
# health-check.sh

# Check migration status
STATUS=$(housekeeper status --config production.yaml --format json)
PENDING_MIGRATIONS=$(echo "$STATUS" | jq '.pending_migrations | length')

if [ "$PENDING_MIGRATIONS" -gt 0 ]; then
    echo "WARNING: $PENDING_MIGRATIONS pending migrations"
    exit 1
fi

# Check schema consistency across cluster
NODES=("node1" "node2" "node3")
REFERENCE_SCHEMA=""

for node in "${NODES[@]}"; do
    SCHEMA=$(housekeeper schema dump --host "$node" --format hash)
    
    if [ -z "$REFERENCE_SCHEMA" ]; then
        REFERENCE_SCHEMA="$SCHEMA"
    elif [ "$SCHEMA" != "$REFERENCE_SCHEMA" ]; then
        echo "ERROR: Schema mismatch on $node"
        exit 1
    fi
done

echo "All nodes have consistent schema ✓"
```

### Performance Monitoring

#### Query Performance
```sql
-- Monitor query performance after migrations
SELECT 
    query_duration_ms,
    query,
    user,
    type,
    event_time
FROM system.query_log
WHERE event_time >= now() - INTERVAL 1 HOUR
  AND query_duration_ms > 10000  -- Queries taking >10s
ORDER BY query_duration_ms DESC
LIMIT 10;
```

#### Table Statistics
```sql
-- Monitor table growth and health
SELECT 
    database,
    table,
    formatReadableSize(total_bytes) as size,
    total_rows,
    active_parts,
    total_parts
FROM system.parts
WHERE active = 1
  AND database NOT IN ('system', 'information_schema')
ORDER BY total_bytes DESC;
```

## Security Best Practices

### Access Control

#### Migration User Setup
```xml
<!-- Dedicated migration user with minimal privileges -->
<users>
    <migration_user>
        <password_sha256_hex><!-- hashed password --></password_sha256_hex>
        <profile>default</profile>
        <quota>default</quota>
        <allow_databases>
            <database>analytics</database>
            <database>reporting</database>
        </allow_databases>
        <access_management>1</access_management>
    </migration_user>
</users>
```

#### Environment Variable Security
```bash
# Use secure environment variable management
export CH_MIGRATION_PASSWORD="$(vault kv get -field=password secret/clickhouse/migration)"

# Rotate passwords regularly
vault kv put secret/clickhouse/migration password="$(openssl rand -base64 32)"
```

### Backup Strategy

#### Automated Backups
```bash
#!/bin/bash
# backup-schema.sh

DATE=$(date +%Y%m%d_%H%M%S)
BACKUP_DIR="/backups/clickhouse"

# 1. Create schema backup
housekeeper schema dump --config production.yaml > "$BACKUP_DIR/schema_$DATE.sql"

# 2. Create data backup (for critical tables)
clickhouse-client --query "BACKUP TABLE analytics.users TO S3('s3://backups/users_$DATE.tar')"

# 3. Verify backup integrity
if [ -s "$BACKUP_DIR/schema_$DATE.sql" ]; then
    echo "Schema backup created: schema_$DATE.sql"
else
    echo "ERROR: Schema backup failed"
    exit 1
fi

# 4. Cleanup old backups (keep 30 days)
find "$BACKUP_DIR" -name "schema_*.sql" -mtime +30 -delete
```

## Documentation

### Schema Documentation

#### Inline Documentation
```sql
-- Use comprehensive comments
CREATE DATABASE analytics 
ENGINE = Atomic 
COMMENT 'Analytics database v2.1 - Contains user behavior and business metrics';

CREATE TABLE analytics.events (
    id UUID DEFAULT generateUUIDv4() COMMENT 'Unique event identifier',
    user_id UInt64 COMMENT 'Reference to users.profiles.id',
    event_type LowCardinality(String) COMMENT 'Event category: page_view, click, purchase, etc.',
    timestamp DateTime COMMENT 'Event occurrence time in UTC',
    
    -- Derived fields for analytics
    date Date MATERIALIZED toDate(timestamp) COMMENT 'Partition key and aggregation dimension',
    hour UInt8 MATERIALIZED toHour(timestamp) COMMENT 'Hourly aggregation dimension'
) 
ENGINE = MergeTree()
PARTITION BY (toYYYYMM(timestamp), event_type)
ORDER BY (timestamp, user_id)
TTL timestamp + INTERVAL 90 DAY DELETE
COMMENT 'User interaction events with 90-day retention policy';
```

#### Migration Documentation
```sql
-- Migration header template
-- Schema migration generated at 2024-03-15 14:30:22 UTC
-- 
-- Changes:
-- - Add user_segment column to events table for ML-based user categorization
-- - Add index on user_segment for filtering performance
-- - Update daily_stats view to include segmentation metrics
--
-- Impact:
-- - New column is nullable and backwards compatible
-- - Index creation may take 10-15 minutes on production
-- - View changes affect reporting queries (verify downstream systems)
--
-- Rollback:
-- - DROP COLUMN user_segment from events table
-- - DROP INDEX idx_user_segment
-- - Restore previous view definition from git history
```

### Team Documentation

#### Schema Change Process
```markdown
# Schema Change Process

## 1. Planning
- [ ] Document business requirement
- [ ] Design schema changes
- [ ] Estimate impact and downtime
- [ ] Plan rollback strategy

## 2. Development
- [ ] Create feature branch
- [ ] Implement schema changes
- [ ] Add tests for new functionality
- [ ] Update documentation

## 3. Review
- [ ] Code review by senior engineer
- [ ] DBA review for performance impact
- [ ] Security review for sensitive data
- [ ] Documentation review

## 4. Testing
- [ ] Test migration in development
- [ ] Performance test with realistic data
- [ ] Test rollback procedure
- [ ] Validate downstream systems

## 5. Deployment
- [ ] Deploy to staging environment
- [ ] Validate staging deployment
- [ ] Schedule production maintenance window
- [ ] Deploy to production
- [ ] Monitor for issues

## 6. Post-Deployment
- [ ] Verify all systems operational
- [ ] Monitor performance metrics
- [ ] Update team on completion
- [ ] Document lessons learned
```

These best practices ensure reliable, maintainable, and performant ClickHouse deployments with Housekeeper. Adapt them to your specific environment and requirements.