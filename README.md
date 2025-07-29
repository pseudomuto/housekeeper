# Housekeeper

A command-line tool for managing ClickHouse database schema migrations, currently focused on database operations.

## Features

- Define database schemas in SQL format (DDL statements)
- Compare database schema definitions with current database state
- Generate migration files (up/down) for detected differences
- Full support for ClickHouse database features:
  - `ON CLUSTER` for distributed DDL
  - Database engines (Atomic, MySQL, etc.)
  - Database comments
  - Conditional operations (IF NOT EXISTS, IF EXISTS)

## Installation

```bash
go get github.com/pseudomuto/housekeeper
```

## Usage

### Define Your Database Schema

Create SQL files in your schema directory with ClickHouse database DDL statements:

```sql
-- databases.sql
CREATE DATABASE analytics ENGINE = Atomic COMMENT 'Analytics database';
CREATE DATABASE logs ON CLUSTER my_cluster ENGINE = Atomic COMMENT 'Logs database';
```

### Generate Migrations

Compare your schema definition with the current database state:

```bash
housekeeper diff --dsn localhost:9000 --schema ./schema --migrations ./migrations --name setup_databases
```

This will:
1. Connect to your ClickHouse instance
2. Read the current database schema
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
-- Migration: setup_databases
-- Generated at: 2024-01-01 12:00:00

CREATE DATABASE analytics ENGINE = Atomic COMMENT 'Analytics database';
ALTER DATABASE logs MODIFY COMMENT 'Updated logs database';
DROP DATABASE IF EXISTS temp_db;
```

**DOWN Migration:**
```sql
-- Rollback migration: setup_databases
-- Generated at: 2024-01-01 12:00:00

DROP DATABASE IF EXISTS analytics;
ALTER DATABASE logs MODIFY COMMENT 'Logs database';
CREATE DATABASE temp_db ENGINE = Atomic COMMENT 'Temporary database';
```

## Schema Definition

### Supported SQL Statements

Housekeeper currently parses and understands the following ClickHouse database DDL statements:

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

### Supported Database Engines

- **Atomic** (default) - ClickHouse's default transactional database engine
- **MySQL** - For connecting to external MySQL instances
- **PostgreSQL** - For connecting to external PostgreSQL instances
- **Dictionary** - For dictionary databases
- And other ClickHouse database engines

## Supported Operations

The tool currently generates appropriate DDL for:

- **Creating databases**: When a database exists in target schema but not in current state
- **Dropping databases**: When a database exists in current state but not in target schema
- **Database comment modifications**: Generates ALTER DATABASE statements for comment changes
- **Cluster-aware operations**: Preserves ON CLUSTER clauses in generated DDL

### Unsupported Operations

The following operations will return an error:

- **Engine changes**: Changing a database engine requires manual recreation
- **Cluster changes**: Adding/removing ON CLUSTER requires manual intervention

## Examples

See the `examples/` directory for sample schema files showing database definitions.

## Current Limitations

- **Database operations only**: Table and view support removed to focus on one object type at a time
- **No column or table operations**: These will be re-added incrementally in future versions
- **Engine/cluster changes not supported**: These operations require manual database recreation

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
- **Database Schema Comparison**: Detects differences between desired and current database state  
- **Migration Generator**: Creates executable up/down SQL migrations for database operations
- **ClickHouse Client**: Connects and reads current database schema
- **Error Handling**: Uses wrapped errors with sentinel values for unsupported operations

### Parser Features

The participle-based parser provides robust support for:

#### Database Operations (Complete)
- `CREATE DATABASE [IF NOT EXISTS] name [ON CLUSTER cluster] [ENGINE = engine(...)] [COMMENT 'comment']`
- `ALTER DATABASE name [ON CLUSTER cluster] MODIFY COMMENT 'comment'`
- `ATTACH DATABASE [IF NOT EXISTS] name [ENGINE = engine(...)] [ON CLUSTER cluster]`
- `DETACH DATABASE [IF EXISTS] name [ON CLUSTER cluster] [PERMANENTLY] [SYNC]`
- `DROP DATABASE [IF EXISTS] name [ON CLUSTER cluster] [SYNC]`

#### Future Work
- Table operations (CREATE, ALTER, DROP TABLE)
- View operations (CREATE, DROP VIEW)
- Full migration generation with executable DDL

See [pkg/parser/README.md](pkg/parser/README.md) for detailed parser documentation.

## Migration Strategy

This tool currently focuses exclusively on database operations to ensure robust, well-tested functionality. Table and view operations will be incrementally added back as the parser and migration system mature.

The database-only approach allows for:
- Proper database lifecycle management
- Cluster-aware database operations
- Foundation for future table/view operations
- Simplified testing and validation