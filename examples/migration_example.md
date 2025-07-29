# Database Migration Example

This example demonstrates how the migration system works with actual ClickHouse database statements.

## Current Database State

Let's say your ClickHouse instance currently has this database:

```sql
CREATE DATABASE analytics ENGINE = Atomic COMMENT 'Old analytics database';
```

## Target Schema Definition

You want to update your schema to:

```sql
-- Updated comment
CREATE DATABASE analytics ENGINE = Atomic COMMENT 'Updated analytics database';

-- New database
CREATE DATABASE logs ON CLUSTER production ENGINE = Atomic COMMENT 'Logs database';
```

## Generated Migration

The migration system will generate the following:

### UP Migration (20241229123456_update_databases.up.sql)

```sql
-- Database migration: update_databases
-- Generated at: 2024-12-29 12:34:56

-- Create database 'logs'
CREATE DATABASE logs ON CLUSTER production ENGINE = Atomic COMMENT 'Logs database';

-- Alter database 'analytics'  
ALTER DATABASE analytics MODIFY COMMENT 'Updated analytics database';
```

### DOWN Migration (20241229123456_update_databases.down.sql)

```sql
-- Database rollback: update_databases
-- Generated at: 2024-12-29 12:34:56

-- Rollback: Alter database 'analytics'
ALTER DATABASE analytics MODIFY COMMENT 'Old analytics database';

-- Rollback: Create database 'logs'
DROP DATABASE IF EXISTS logs ON CLUSTER production;
```

## Migration Features

### 1. **CREATE Operations**
- Generates `CREATE DATABASE` statements for new databases
- Includes all properties: ENGINE, COMMENT, ON CLUSTER
- Rollback generates corresponding `DROP DATABASE IF EXISTS`

### 2. **ALTER Operations** 
- Detects comment changes and generates `ALTER DATABASE ... MODIFY COMMENT`
- Rollback restores original comment
- Engine/cluster changes require manual intervention (noted in migration)

### 3. **DROP Operations**
- Generates `DROP DATABASE IF EXISTS` for databases no longer in schema
- Rollback recreates the database with original properties

### 4. **Proper Ordering**
- UP migrations: CREATE → ALTER → DROP
- DOWN migrations: Reverse order for safe rollback
- Cluster-aware operations maintain ON CLUSTER clauses

## Complex Example

### Current State
```sql
CREATE DATABASE old_analytics ENGINE = Atomic COMMENT 'Old system';
CREATE DATABASE temp_data ENGINE = Atomic;
```

### Target State
```sql
CREATE DATABASE analytics ENGINE = Atomic COMMENT 'New analytics system';
CREATE DATABASE logs ON CLUSTER prod ENGINE = MySQL('host:3306', 'logs', 'user', 'pass');
```

### Generated Migration
```sql
-- UP Migration
-- Create database 'analytics'
CREATE DATABASE analytics ENGINE = Atomic COMMENT 'New analytics system';

-- Create database 'logs'  
CREATE DATABASE logs ON CLUSTER prod ENGINE = MySQL('host:3306', 'logs', 'user', 'pass');

-- Drop database 'old_analytics'
DROP DATABASE IF EXISTS old_analytics;

-- Drop database 'temp_data'
DROP DATABASE IF EXISTS temp_data;
```

This provides a complete, executable migration that transforms your database schema safely and reversibly.