# Housekeeper

A command-line tool for managing ClickHouse schema migrations, supporting database, dictionary, and view operations.

## Features

- Define schemas in SQL format (DDL statements) for databases, dictionaries, and views
- Compare schema definitions with current ClickHouse state
- Generate migration files (up/down) for detected differences
- Full support for ClickHouse database operations:
  - `ON CLUSTER` for distributed DDL
  - Database engines (Atomic, MySQL, etc.)
  - Database comments
  - Conditional operations (IF NOT EXISTS, IF EXISTS)
- Full support for ClickHouse dictionary operations:
  - CREATE DICTIONARY with complex attributes (IS_OBJECT_ID, HIERARCHICAL, INJECTIVE)
  - CREATE OR REPLACE for dictionary modifications (since dictionaries can't be altered)
  - ATTACH/DETACH/DROP DICTIONARY operations
  - All dictionary features: sources, layouts, lifetimes, settings
- Full support for ClickHouse view operations:
  - CREATE VIEW and CREATE MATERIALIZED VIEW with full syntax support
  - ENGINE specification, POPULATE option, TO table for materialized views
  - ALTER TABLE MODIFY QUERY for materialized view modifications
  - ATTACH/DETACH operations (VIEW for regular views, TABLE for materialized views)
  - RENAME TABLE for both view types
  - Intelligent migration strategies for different view types

## Installation

```bash
go get github.com/pseudomuto/housekeeper
```

## Usage

### Define Your Schema

Create SQL files in your schema directory with ClickHouse DDL statements:

```sql
-- databases.sql
CREATE DATABASE analytics ENGINE = Atomic COMMENT 'Analytics database';
CREATE DATABASE logs ON CLUSTER my_cluster ENGINE = Atomic COMMENT 'Logs database';

-- dictionaries.sql
CREATE DICTIONARY analytics.users_dict (
  user_id UInt64 IS_OBJECT_ID,
  parent_id UInt64 DEFAULT 0 HIERARCHICAL,
  name String,
  email String INJECTIVE
) PRIMARY KEY user_id, parent_id
SOURCE(MySQL(host 'localhost' port 3306 user 'root' password 'secret' db 'users' table 'user_data'))
LAYOUT(COMPLEX_KEY_HASHED(size_in_cells 1000000))
LIFETIME(MIN 300 MAX 3600)
SETTINGS(max_threads = 4)
COMMENT 'User dictionary with hierarchical structure';

-- views.sql
CREATE VIEW analytics.daily_summary 
AS SELECT date, count(*) as total_events FROM events GROUP BY date;

CREATE MATERIALIZED VIEW analytics.mv_daily_stats
ENGINE = MergeTree() ORDER BY date
POPULATE
AS SELECT 
    toDate(timestamp) as date, 
    count() as event_count,
    uniq(user_id) as unique_users
FROM events 
GROUP BY date;

CREATE OR REPLACE MATERIALIZED VIEW analytics.mv_user_stats
TO analytics.user_statistics
AS SELECT 
    user_id,
    count() as total_events,
    max(timestamp) as last_activity
FROM events
GROUP BY user_id;
```

### Generate Migrations

Compare your schema definition with the current database state:

```bash
housekeeper diff --dsn localhost:9000 --schema ./schema --migrations ./migrations --name setup_databases
```

This will:
1. Connect to your ClickHouse instance
2. Read the current schema (databases, dictionaries, and views)
3. Parse your SQL schema files
4. Compare them and detect differences
5. Generate migration files if differences are found

### Migration Files

The tool generates timestamped migration files:
- `20240101120000_setup_databases.up.sql` - Apply changes
- `20240101120000_setup_databases.down.sql` - Rollback changes

Example migration output:

**UP Migration:**
```sql
-- Schema migration: setup_databases
-- Generated at: 2024-01-01 12:00:00

-- Create database 'analytics'
CREATE DATABASE analytics ENGINE = Atomic COMMENT 'Analytics database';

-- Alter database 'logs'
ALTER DATABASE logs MODIFY COMMENT 'Updated logs database';

-- Create dictionary 'analytics.users_dict'
CREATE DICTIONARY analytics.users_dict (
  user_id UInt64 IS_OBJECT_ID,
  name String INJECTIVE
) PRIMARY KEY user_id
SOURCE(MySQL(host 'localhost' port 3306 user 'root' password 'secret' db 'users' table 'user_data'))
LAYOUT(HASHED())
LIFETIME(3600)
COMMENT 'User dictionary';

-- Rename database 'temp_db' to 'staging_db'
RENAME DATABASE temp_db TO staging_db;

-- Rename dictionary 'old_users.users_dict' to 'users.users_dict'
RENAME DICTIONARY old_users.users_dict TO users.users_dict;

-- Drop database 'unused_db'
DROP DATABASE IF EXISTS unused_db;
```

**DOWN Migration:**
```sql
-- Schema rollback: setup_databases
-- Generated at: 2024-01-01 12:00:00

-- Rollback: Drop database 'unused_db'
CREATE DATABASE unused_db ENGINE = Atomic COMMENT 'Unused database';

-- Rollback: Rename dictionary 'old_users.users_dict' to 'users.users_dict'
RENAME DICTIONARY users.users_dict TO old_users.users_dict;

-- Rollback: Rename database 'temp_db' to 'staging_db'
RENAME DATABASE staging_db TO temp_db;

-- Rollback: Create dictionary 'analytics.users_dict'
DROP DICTIONARY IF EXISTS analytics.users_dict;

-- Rollback: Alter database 'logs'
ALTER DATABASE logs MODIFY COMMENT 'Logs database';

-- Rollback: Create database 'analytics'
DROP DATABASE IF EXISTS analytics;
```

## Schema Definition

### Supported SQL Statements

Housekeeper parses and understands the following ClickHouse DDL statements:

#### Create Database
```sql
CREATE DATABASE [IF NOT EXISTS] database_name [ON CLUSTER cluster_name] [ENGINE = engine_name[(params)]] [COMMENT 'comment'];
```

#### Alter Database
```sql
ALTER DATABASE database_name [ON CLUSTER cluster_name] MODIFY COMMENT 'new comment';
```

#### Attach Database
```sql
ATTACH DATABASE [IF NOT EXISTS] database_name [ENGINE = engine_name[(params)]] [ON CLUSTER cluster_name];
```

#### Detach Database
```sql
DETACH DATABASE [IF EXISTS] database_name [ON CLUSTER cluster_name] [PERMANENTLY] [SYNC];
```

#### Drop Database
```sql
DROP DATABASE [IF EXISTS] database_name [ON CLUSTER cluster_name] [SYNC];
```

#### Rename Database
```sql
RENAME DATABASE old_name1 TO new_name1 [, old_name2 TO new_name2, ...] [ON CLUSTER cluster_name];
```

#### Create Dictionary
```sql
CREATE [OR REPLACE] DICTIONARY [IF NOT EXISTS] [database.]dictionary_name [ON CLUSTER cluster_name]
(
    column_name1 column_type1 [DEFAULT|EXPRESSION expr1] [IS_OBJECT_ID|HIERARCHICAL|INJECTIVE],
    column_name2 column_type2 [DEFAULT|EXPRESSION expr2] [IS_OBJECT_ID|HIERARCHICAL|INJECTIVE],
    ...
)
PRIMARY KEY key1 [, key2, ...]
SOURCE(source_type(param1 value1 [param2 value2 ...]))
LAYOUT(layout_type[(param1 value1 [param2 value2 ...])])
LIFETIME([MIN min_val MAX max_val] | single_val)
[SETTINGS(setting1 = value1 [, setting2 = value2, ...])]
[COMMENT 'comment'];
```

#### Attach Dictionary
```sql
ATTACH DICTIONARY [IF NOT EXISTS] [database.]dictionary_name [ON CLUSTER cluster_name];
```

#### Detach Dictionary
```sql
DETACH DICTIONARY [IF EXISTS] [database.]dictionary_name [ON CLUSTER cluster_name] [PERMANENTLY] [SYNC];
```

#### Drop Dictionary
```sql
DROP DICTIONARY [IF EXISTS] [database.]dictionary_name [ON CLUSTER cluster_name] [SYNC];
```

#### Rename Dictionary
```sql
RENAME DICTIONARY [database.]old_name1 TO [database.]new_name1 [, [database.]old_name2 TO [database.]new_name2, ...] [ON CLUSTER cluster_name];
```

#### Create View
```sql
CREATE [OR REPLACE] VIEW [IF NOT EXISTS] [database.]view_name [ON CLUSTER cluster_name] AS SELECT ...;
```

#### Create Materialized View
```sql
CREATE [OR REPLACE] MATERIALIZED VIEW [IF NOT EXISTS] [database.]view_name [ON CLUSTER cluster_name]
[TO [database.]table_name] [ENGINE = engine] [POPULATE]
AS SELECT ...;
```

#### Attach View
```sql
ATTACH VIEW [IF NOT EXISTS] [database.]view_name [ON CLUSTER cluster_name];
```

#### Attach Table (for Materialized Views)
```sql
ATTACH TABLE [IF NOT EXISTS] [database.]table_name [ON CLUSTER cluster_name];
```

#### Detach View
```sql
DETACH VIEW [IF EXISTS] [database.]view_name [ON CLUSTER cluster_name] [PERMANENTLY] [SYNC];
```

#### Detach Table (for Materialized Views)
```sql
DETACH TABLE [IF EXISTS] [database.]table_name [ON CLUSTER cluster_name] [PERMANENTLY] [SYNC];
```

#### Drop View
```sql
DROP VIEW [IF EXISTS] [database.]view_name [ON CLUSTER cluster_name] [SYNC];
```

#### Drop Table (for Materialized Views)
```sql
DROP TABLE [IF EXISTS] [database.]table_name [ON CLUSTER cluster_name] [SYNC];
```

#### Rename Table (for Views and Materialized Views)
```sql
RENAME TABLE [database.]old_name1 TO [database.]new_name1 [, [database.]old_name2 TO [database.]new_name2, ...] [ON CLUSTER cluster_name];
```

### Supported Database Engines

- **Atomic** (default) - ClickHouse's default transactional database engine
- **MySQL** - For connecting to external MySQL instances
- **PostgreSQL** - For connecting to external PostgreSQL instances
- **Dictionary** - For dictionary databases
- And other ClickHouse database engines

## Supported Operations

The tool generates appropriate DDL for:

### Database Operations
- **Creating databases**: When a database exists in target schema but not in current state
- **Dropping databases**: When a database exists in current state but not in target schema
- **Database comment modifications**: Generates ALTER DATABASE statements for comment changes
- **Renaming databases**: When a database has identical properties but different name
- **Cluster-aware operations**: Preserves ON CLUSTER clauses in generated DDL

### Dictionary Operations
- **Creating dictionaries**: When a dictionary exists in target schema but not in current state
- **Dropping dictionaries**: When a dictionary exists in current state but not in target schema
- **Replacing dictionaries**: Uses CREATE OR REPLACE when any dictionary properties change (dictionaries cannot be altered)
- **Renaming dictionaries**: When a dictionary has identical properties but different name/database
- **Complex dictionary features**: Supports all ClickHouse dictionary attributes, sources, layouts, and lifetimes

### View Operations
- **Creating views**: When a view exists in target schema but not in current state
- **Dropping views**: When a view exists in current state but not in target schema
- **Altering views**: 
  - **Regular views**: Uses CREATE OR REPLACE for modifications
  - **Materialized views**: Uses ALTER TABLE MODIFY QUERY for query changes (ClickHouse limitation)
- **Renaming views**: Uses RENAME TABLE for both regular and materialized views when properties match but names differ
- **Complex view features**: Supports ENGINE clauses, POPULATE option, TO table for materialized views

### Unsupported Operations

The following operations will return an error:

- **Engine changes**: Changing a database engine requires manual recreation
- **Cluster changes**: Adding/removing ON CLUSTER requires manual intervention

## Examples

See the `examples/` directory for sample schema files showing database definitions.

## Current Limitations

- **Limited table operations**: Only basic CREATE TABLE parsing implemented (full support coming soon)
- **Engine/cluster changes not supported**: Database engine or cluster changes require manual intervention
- **Dictionary structure changes**: Since dictionaries can't be altered, any change requires CREATE OR REPLACE
- **Materialized view limitations**: Only query changes supported via ALTER TABLE MODIFY QUERY; other changes require DROP+CREATE
- **SELECT clause formatting**: Whitespace formatting may not be preserved in parsed queries

## Development

```bash
# Run tests
go test ./...

# Build
go build -o housekeeper

# Run locally
./housekeeper diff --dsn localhost:9000
```

## Architecture

- **SQL Parser**: Modern participle-based parser for ClickHouse DDL statements
- **Schema Comparison**: Detects differences between desired and current schema state (databases, dictionaries, and views)
- **Migration Generator**: Creates executable up/down SQL migrations with proper operation ordering
- **ClickHouse Client**: Connects and reads current schema (databases, dictionaries, and views)
- **Error Handling**: Uses wrapped errors with sentinel values for unsupported operations

### Parser Features

The participle-based parser provides robust support for:

#### Database Operations (Complete)
- `CREATE DATABASE [IF NOT EXISTS] name [ON CLUSTER cluster] [ENGINE = engine(...)] [COMMENT 'comment']`
- `ALTER DATABASE name [ON CLUSTER cluster] MODIFY COMMENT 'comment'`
- `ATTACH DATABASE [IF NOT EXISTS] name [ENGINE = engine(...)] [ON CLUSTER cluster]`
- `DETACH DATABASE [IF EXISTS] name [ON CLUSTER cluster] [PERMANENTLY] [SYNC]`
- `DROP DATABASE [IF EXISTS] name [ON CLUSTER cluster] [SYNC]`
- `RENAME DATABASE old_name TO new_name [, ...] [ON CLUSTER cluster]`

#### Dictionary Operations (Complete)
- `CREATE [OR REPLACE] DICTIONARY [IF NOT EXISTS] [db.]name [ON CLUSTER cluster] (...) PRIMARY KEY ... SOURCE(...) LAYOUT(...) LIFETIME(...) [SETTINGS(...)] [COMMENT 'comment']`
- `ATTACH DICTIONARY [IF NOT EXISTS] [db.]name [ON CLUSTER cluster]`
- `DETACH DICTIONARY [IF EXISTS] [db.]name [ON CLUSTER cluster] [PERMANENTLY] [SYNC]`
- `DROP DICTIONARY [IF EXISTS] [db.]name [ON CLUSTER cluster] [SYNC]`
- `RENAME DICTIONARY [db.]old_name TO [db.]new_name [, ...] [ON CLUSTER cluster]`
- Complex column attributes: `IS_OBJECT_ID`, `HIERARCHICAL`, `INJECTIVE`, `DEFAULT`, `EXPRESSION`
- All source types, layout types, lifetime configurations, and settings

#### View Operations (Complete)
- `CREATE [OR REPLACE] VIEW [IF NOT EXISTS] [db.]name [ON CLUSTER cluster] AS SELECT ...`
- `CREATE [OR REPLACE] MATERIALIZED VIEW [IF NOT EXISTS] [db.]name [ON CLUSTER cluster] [TO table] [ENGINE = engine] [POPULATE] AS SELECT ...`
- `ATTACH VIEW [IF NOT EXISTS] [db.]name [ON CLUSTER cluster]` (regular views)
- `ATTACH TABLE [IF NOT EXISTS] [db.]name [ON CLUSTER cluster]` (materialized views)
- `DETACH VIEW [IF EXISTS] [db.]name [ON CLUSTER cluster] [PERMANENTLY] [SYNC]` (regular views)
- `DETACH TABLE [IF EXISTS] [db.]name [ON CLUSTER cluster] [PERMANENTLY] [SYNC]` (materialized views)
- `DROP VIEW [IF EXISTS] [db.]name [ON CLUSTER cluster] [SYNC]` (regular views)
- `DROP TABLE [IF EXISTS] [db.]name [ON CLUSTER cluster] [SYNC]` (materialized views)
- `RENAME TABLE [db.]old_name TO [db.]new_name [, ...] [ON CLUSTER cluster]` (both view types)
- ENGINE specifications, POPULATE option, TO table clause for materialized views
- Complex SELECT query parsing with AS SELECT clause

#### Future Work
- Full table operations (CREATE, ALTER, DROP TABLE with column definitions)
- Index operations (CREATE INDEX, DROP INDEX)

See [pkg/parser/README.md](pkg/parser/README.md) for detailed parser documentation.

## Migration Strategy

This tool provides comprehensive schema management for ClickHouse databases, dictionaries, and views with intelligent migration strategies tailored to each object type.

The current approach provides:
- **Complete database lifecycle management** with full ClickHouse syntax support
- **Full dictionary lifecycle management** with CREATE OR REPLACE for modifications (since dictionaries can't be altered)
- **Complete view lifecycle management** with different strategies:
  - **Regular views**: CREATE OR REPLACE for modifications
  - **Materialized views**: ALTER TABLE MODIFY QUERY for query changes, proper DDL for other operations
- **Cluster-aware operations** for distributed deployments
- **Proper migration ordering**: databases → dictionaries → views (UP), reverse order (DOWN)
- **Intelligent rename detection** to avoid unnecessary DROP+CREATE operations
- **Foundation for future table operations** with extensible architecture