# Migration Process

Learn how Housekeeper generates, manages, and applies ClickHouse schema migrations.

## Overview

Housekeeper's migration system compares your desired schema (defined in your schema files) with the current state of your ClickHouse database and generates the necessary SQL statements to transform the current state to match your desired schema.

> **📝 Migration Tracking**  
> Housekeeper automatically manages migration tracking infrastructure. When you run your first migration against any ClickHouse instance, Housekeeper creates a `housekeeper.revisions` table to track which migrations have been applied. This happens automatically - no manual setup required.

## How Migrations Work

### 1. Development Server Workflow

Housekeeper uses a development workflow with Docker containers:

```bash
# Start development server with existing migrations applied
housekeeper dev up
```

This process:
- Starts a ClickHouse Docker container
- Applies all existing migrations from `db/migrations/`
- Provides connection details for the running server

### 2. Schema Compilation

When comparing schemas, Housekeeper compiles your target schema:

```bash
# Compile and view your complete schema
housekeeper schema compile
```

The compilation process:
- Processes all `-- housekeeper:import` directives recursively
- Resolves relative paths from each file's location
- Combines all SQL into a single output with proper ordering
- Validates all DDL syntax through the robust parser

### 3. Intelligent Comparison

The diff command compares your target schema with the current database state:

```bash
# Generate migration by comparing schema with current database
housekeeper diff
```

The comparison algorithm:
- **Current State**: Reads schema from the running development server
- **Target State**: Compiles your schema files 
- **Object Detection**: Identifies new, modified, and removed objects
- **Rename Detection**: Recognizes when objects are renamed (avoiding DROP+CREATE)
- **Property Analysis**: Compares all properties of each object type
- **Dependency Resolution**: Understands relationships between objects

### 4. Migration Generation

Based on the comparison, Housekeeper generates optimal migration strategies:

- **Databases**: CREATE, ALTER (comments), RENAME, DROP
- **Tables**: CREATE, ALTER (columns), RENAME, DROP
- **Dictionaries**: CREATE OR REPLACE (dictionaries can't be altered)
- **Views**: CREATE OR REPLACE for regular views, DROP+CREATE for materialized views

## Migration Strategies

### Database Operations

#### Creating Databases
When a database exists in target schema but not current state:
```sql
CREATE DATABASE analytics ENGINE = Atomic COMMENT 'Analytics database';
```

#### Modifying Database Comments
When database properties change:
```sql
ALTER DATABASE analytics MODIFY COMMENT 'Updated analytics database';
```

#### Renaming Databases
When a database has identical properties but different name:
```sql
RENAME DATABASE old_analytics TO analytics;
```

### Table Operations

#### Creating Tables
```sql
CREATE TABLE analytics.events (
    id UUID DEFAULT generateUUIDv4(),
    timestamp DateTime,
    event_type String
) ENGINE = MergeTree() ORDER BY timestamp;
```

#### Altering Tables
For standard table engines, Housekeeper generates precise ALTER statements:
```sql
-- Add new column
ALTER TABLE analytics.events ADD COLUMN user_id UInt64;

-- Modify column type
ALTER TABLE analytics.events MODIFY COLUMN event_type LowCardinality(String);

-- Drop column
ALTER TABLE analytics.events DROP COLUMN old_column;
```

#### Integration Engine Tables
For integration engines (Kafka, MySQL, PostgreSQL, etc.), Housekeeper automatically uses DROP+CREATE strategy:
```sql
-- Integration engines require recreation for modifications
DROP TABLE integration.kafka_events;
CREATE TABLE integration.kafka_events (...) ENGINE = Kafka(...);
```

### Dictionary Operations

Dictionaries use CREATE OR REPLACE for all modifications since ClickHouse doesn't support ALTER DICTIONARY:

```sql
-- Any dictionary change becomes CREATE OR REPLACE
CREATE OR REPLACE DICTIONARY analytics.users_dict (
    id UInt64 IS_OBJECT_ID,
    name String INJECTIVE
) PRIMARY KEY id
SOURCE(HTTP(url 'http://api.example.com/users'))
LAYOUT(HASHED())
LIFETIME(3600);
```

### View Operations

#### Regular Views
Use CREATE OR REPLACE for modifications:
```sql
CREATE OR REPLACE VIEW analytics.daily_summary 
AS SELECT date, count() FROM events GROUP BY date;
```

#### Materialized Views
Use DROP+CREATE for query modifications (more reliable than ALTER TABLE MODIFY QUERY):
```sql
-- Drop existing materialized view
DROP TABLE analytics.mv_daily_stats;

-- Recreate with new query
CREATE MATERIALIZED VIEW analytics.mv_daily_stats
ENGINE = MergeTree() ORDER BY date
AS SELECT date, count(), sum(amount) FROM events GROUP BY date;
```

## Migration Ordering

Housekeeper ensures proper operation ordering to handle dependencies:

### UP Migrations (Create)
1. **Databases** - Create databases first
2. **Tables** - Create tables that other objects depend on
3. **Dictionaries** - Create dictionaries after source tables
4. **Views** - Create views last (depend on tables and dictionaries)

Within each type:
1. **CREATE** - Create new objects
2. **ALTER/REPLACE** - Modify existing objects
3. **RENAME** - Rename objects
4. **DROP** - Remove objects

### DOWN Migrations (Destroy)
Reverse order of UP migrations for safe teardown.

## Rename Detection

Housekeeper includes intelligent rename detection to avoid unnecessary DROP+CREATE operations:

### How Rename Detection Works

1. **Property Comparison**: Compares all properties except names
2. **Exact Match**: Properties must match exactly (except name/database)
3. **Generate RENAME**: Creates efficient RENAME statements instead of DROP+CREATE

### Example Rename Detection

**Current Schema:**
```sql
CREATE DATABASE old_analytics ENGINE = Atomic COMMENT 'Analytics DB';
CREATE TABLE old_analytics.user_events (...) ENGINE = MergeTree() ORDER BY timestamp;
```

**Target Schema:**
```sql
CREATE DATABASE analytics ENGINE = Atomic COMMENT 'Analytics DB';
CREATE TABLE analytics.events (...) ENGINE = MergeTree() ORDER BY timestamp;
```

**Generated Migration:**
```sql
-- Efficient renames instead of DROP+CREATE
RENAME DATABASE old_analytics TO analytics;
RENAME TABLE analytics.user_events TO analytics.events;
```

## Migration Files

### File Naming

Migration files use UTC timestamps for consistent ordering:
- Format: `yyyyMMddHHmmss.sql`
- Example: `20240806143022.sql`

### File Structure

Each migration file includes:

```sql
-- Schema migration generated at 2024-08-06 14:30:22 UTC
-- Down migration: swap current and target schemas and regenerate

-- Create database 'analytics'
CREATE DATABASE analytics ENGINE = Atomic COMMENT 'Analytics database';

-- Create table 'analytics.events'
CREATE TABLE analytics.events (
    id UUID DEFAULT generateUUIDv4(),
    timestamp DateTime,
    event_type String
) ENGINE = MergeTree() ORDER BY timestamp;
```

### Migration Integrity

Housekeeper generates a `housekeeper.sum` file for integrity checking:

```
h1:TotalHashOfAllMigrations=
20240101120000.sql h1:HashOfMigration1=
20240101130000.sql h1:ChainedHashWithPrevious=
```

This ensures:
- **Tamper Detection**: Unauthorized changes are detected
- **Consistency**: Same migrations across environments
- **Chained Verification**: Each migration builds on the previous

## Automatic Partial Progress Tracking

Housekeeper automatically tracks the progress of migration execution at the statement level, enabling seamless recovery from failures without manual intervention.

### How Partial Progress Works

When applying migrations, Housekeeper:

1. **Statement-Level Tracking**: Records progress after each successful statement
2. **Hash Validation**: Stores cryptographic hashes of each statement for integrity
3. **Automatic Resume**: Automatically detects and resumes from failure points
4. **No User Action Required**: No flags or commands needed - happens automatically

### Migration Execution Tracking

Each migration execution is tracked in the `housekeeper.revisions` table:

```sql
-- Example revision record
INSERT INTO housekeeper.revisions (
    version,           -- Migration version (e.g., '20240101120000_create_users')
    executed_at,       -- UTC timestamp of execution
    execution_time_ms, -- Total execution time
    kind,             -- 'migration' or 'snapshot'
    error,            -- Error message if failed (NULL if successful)
    applied,          -- Number of statements successfully applied
    total,            -- Total statements in migration
    hash,             -- h1 hash of entire migration
    partial_hashes,   -- Array of h1 hashes for each statement
    housekeeper_version -- Version of Housekeeper that ran the migration
);
```

### Automatic Recovery Examples

#### Partial Failure Scenario

If a migration fails partway through:

```sql
-- Migration: 20240101120000_setup_analytics.sql (5 statements total)
CREATE DATABASE analytics;                    -- ✅ Statement 1: Success
CREATE TABLE analytics.events (...);          -- ✅ Statement 2: Success  
CREATE TABLE analytics.users (...);           -- ✅ Statement 3: Success
CREATE DICTIONARY analytics.locations (...);  -- ❌ Statement 4: Failed (network error)
CREATE VIEW analytics.summary (...);          -- ⏸  Statement 5: Not executed
```

**Revision Record Created:**
- `applied`: 3 (statements 1-3 completed)
- `total`: 5 (migration has 5 statements)
- `error`: "Network timeout connecting to dictionary source"
- `partial_hashes`: ["h1:stmt1hash=", "h1:stmt2hash=", "h1:stmt3hash=", ...]

#### Automatic Resume

When you run migrations again:

```bash
# Simply run migrate - no special flags needed
housekeeper migrate --dsn localhost:9000
```

Housekeeper automatically:

1. **Detects Partial State**: Finds revision with `applied < total`
2. **Validates Integrity**: Confirms statements 1-3 haven't changed using hashes
3. **Shows Progress**: Displays what will be resumed
4. **Resumes Execution**: Starts from statement 4 (the failed statement)

```bash
Found 1 partially applied migration(s) that will be resumed:

  ⚠️  20240101120000_setup_analytics: 3/5 statements applied
     Last error: Network timeout connecting to dictionary source
     Will resume with 2 remaining statement(s)

Migration execution results:
  ⚠️  20240101120000_setup_analytics resumed from statement 4 in 1.234s (2/2 remaining statements)
```

### Safety Features

#### Hash Validation

Before resuming, Housekeeper validates migration file integrity:

```bash
# ❌ If migration file was modified after partial execution
Error: statement 2 hash mismatch: migration file may have been modified 
since partial execution (expected h1:abc123=, got h1:def456=)

# ✅ If migration file is unchanged, resume proceeds safely
Found partial migration, resuming from statement 4...
```

#### Statement Count Validation

```bash
# ❌ If statements were added/removed from migration file
Error: migration statement count changed: expected 5 statements, 
found 7 in revision

# This prevents resuming with a different migration file
```

## Development Workflow

### Development Cycle

The typical development workflow is:

```bash
# 1. Start development server (applies existing migrations)
housekeeper dev up

# 2. Make schema changes in your files
# Edit db/main.sql or imported files

# 3. Generate migration from changes
housekeeper diff

# 4. Restart server to apply new migration
housekeeper dev down
housekeeper dev up
```

### Working with Existing Databases

Extract schema from an existing ClickHouse instance:

```bash
# Bootstrap project from existing database
housekeeper bootstrap --url localhost:9000

# This creates initial project structure with current schema
```

When working with existing databases, you can exclude certain databases from the extraction process:

```bash
# Extract schema while ignoring test databases
housekeeper schema dump --url localhost:9000 \
  --ignore-databases testing_db \
  --ignore-databases temp_analytics

# Or configure in housekeeper.yaml for permanent exclusion
clickhouse:
  ignore_databases:
    - testing_db
    - temp_analytics
```

This is useful when you have test or temporary databases that shouldn't be part of your managed schema.

## Validation and Safety

### Pre-Migration Validation

Before generating migrations, Housekeeper validates:

1. **Schema Syntax**: All SQL must parse correctly
2. **Forbidden Operations**: Prevents unsupported operations
3. **Dependency Check**: Ensures proper object dependencies

### Forbidden Operations

Some operations require manual intervention:

```bash
# These operations will return validation errors:

# ❌ Engine changes
# Current: ENGINE = MergeTree()
# Target:  ENGINE = ReplacingMergeTree()
# Error: engine type changes not supported

# ❌ Cluster changes  
# Current: CREATE TABLE users (...);
# Target:  CREATE TABLE users (...) ON CLUSTER prod;
# Error: cluster configuration changes not supported

# ❌ System object modifications
# Error: system object modifications not supported
```

### Automatic Handling

Some operations are automatically handled with optimal strategies:

```bash
# ✅ Integration engine modifications
# Automatically uses DROP+CREATE strategy

# ✅ Materialized view query changes
# Automatically uses DROP+CREATE strategy

# ✅ Dictionary modifications
# Automatically uses CREATE OR REPLACE strategy
```

## Best Practices

### Development Workflow

1. **Make Schema Changes**: Edit your schema files
2. **Generate Migration**: Run `housekeeper diff`
3. **Review Output**: Examine the generated SQL carefully
4. **Test in Development**: Apply to development environment first
5. **Commit Together**: Commit schema files and migration together
6. **Deploy Systematically**: Apply to staging, then production

### Migration Safety

1. **Backup First**: Always backup before applying migrations to production
2. **Test Thoroughly**: Test migrations in non-production environments
3. **Monitor Application**: Watch for application errors after migration
4. **Have Rollback Plan**: Prepare rollback procedures for critical changes

### Team Collaboration

1. **Code Reviews**: Review migrations like application code
2. **Sequential Migrations**: Avoid parallel schema changes
3. **Communication**: Coordinate schema changes with team
4. **Documentation**: Document complex migrations and their purpose

## Troubleshooting

### Common Issues

#### Migration Generation Fails
```bash
# Check schema syntax
housekeeper schema compile

# Validate connection
housekeeper schema dump --url localhost:9000
```

#### Migration Application Fails
```bash
# Check ClickHouse logs
docker logs clickhouse-container

# Check development server status
housekeeper dev up --help
```

#### Forbidden Operation Errors
```bash
# These require manual intervention:
# 1. Engine changes: Manually DROP+CREATE
# 2. Cluster changes: Manually recreate objects
# 3. System modifications: Not allowed
```

### Recovery Procedures

#### Corrupted Migration State
```bash
# Regenerate sum file
housekeeper rehash

# Compare current database with expected schema
housekeeper diff
```

#### Failed Migration

Housekeeper automatically handles partial migration failures:

```bash
# ✅ Simply run migrate again - automatic resume
housekeeper migrate --dsn localhost:9000

# ✅ Check migration status to see partial progress
housekeeper status --dsn localhost:9000 --verbose

# ✅ Use dry-run to see what would be resumed
housekeeper migrate --dsn localhost:9000 --dry-run
```

If you need to manually investigate:

```bash
# Check what was applied using schema dump
housekeeper schema dump --url localhost:9000

# Check revision table for detailed failure info
SELECT version, applied, total, error, executed_at 
FROM housekeeper.revisions 
WHERE error IS NOT NULL 
ORDER BY executed_at DESC;
```

## Next Steps

- **[Schema Management](schema-management.md)** - Learn best practices for schema design
- **[Configuration](configuration.md)** - Configure Housekeeper for your environment  
- **[Cluster Management](../advanced/cluster-management.md)** - Handle distributed ClickHouse deployments
- **[Troubleshooting](../advanced/troubleshooting.md)** - Solve common migration issues