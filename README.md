# Housekeeper

[![CI](https://github.com/noibu/housekeeper/workflows/CI/badge.svg)](https://github.com/noibu/housekeeper/actions?query=workflow%3ACI)
[![GoDoc](https://godoc.org/github.com/noibu/housekeeper?status.svg)](https://godoc.org/github.com/noibu/housekeeper)
[![Go Report Card](https://goreportcard.com/badge/github.com/noibu/housekeeper)](https://goreportcard.com/report/github.com/noibu/housekeeper)

A ClickHouse schema management tool heavily inspired by [Atlas](https://atlasgo.io/), built specifically to address the gaps in ClickHouse support that Atlas couldn't fill.

> **NOTE**: This is very much still a WIP and heavily focused on my own needs. I intend to continue development as
available. And of course, PRs are welcome and encouraged.

## Why This Exists

While Atlas is an excellent database schema management tool, its ClickHouse support falls short of what's needed for production ClickHouse deployments. Critical ClickHouse features like `ON CLUSTER` operations, `PARTITION BY` clauses, materialized view management, and dictionary operations either weren't supported or had significant limitations.

Rather than wait for Atlas to catch up with ClickHouse's unique requirements, Housekeeper was created as a purpose-built solution that understands ClickHouse's distributed nature, specialized data types, and advanced features from the ground up.

## Key Features

- **Complete ClickHouse DDL Support** - Full support for databases, tables (including `CREATE TABLE AS`), dictionaries, views, materialized views, functions, and roles
- **Cluster-Aware Operations** - Native `ON CLUSTER` support for distributed ClickHouse deployments
- **Intelligent Migration Generation** - Smart schema comparison with proper operation ordering and dependency management
- **Modern Parser Architecture** - Built with participle for robust, maintainable SQL parsing
- **Professional SQL Formatting** - Clean, consistent output optimized for ClickHouse
- **Comprehensive Testing** - Extensive test suite with 100% DDL operation coverage

## Supported Migration Operations

| Object Type | CREATE | ALTER | ATTACH | DETACH | DROP | RENAME | GRANT/REVOKE | Notes |
|------------|--------|-------|---------|---------|------|--------|--------------|-------|
| **Database** | ✅ | ✅¹ | ✅ | ✅ | ✅ | ✅ | N/A | ¹Comment changes only |
| **Function** | ✅ | ❌² | ❌ | ❌ | ✅ | ✅³ | N/A | ²Uses DROP+CREATE strategy |
| **Table** | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | N/A | Full ALTER support, CREATE AS syntax |
| **Dictionary** | ✅ | ❌⁵ | ✅ | ✅ | ✅ | ✅ | N/A | ⁵Uses CREATE OR REPLACE |
| **View** | ✅ | ❌⁶ | ✅ | ✅ | ✅ | ✅⁷ | N/A | ⁶Uses CREATE OR REPLACE |
| **Materialized View** | ✅ | ❌⁸ | ✅⁹ | ✅⁹ | ✅⁹ | ✅⁹ | N/A | ⁸Query changes use DROP+CREATE |
| **Role** | ✅ | ✅¹⁰ | ❌ | ❌ | ✅ | ✅¹¹ | ✅ | ¹⁰Settings and rename only |

**Legend:**
- ✅ Fully supported
- ❌ Not supported (alternative strategy used)
- N/A Not applicable  
- ¹ ALTER DATABASE only supports comment modifications
- ² Functions use DROP+CREATE for all modifications (no ALTER FUNCTION in ClickHouse)
- ³ Functions use DROP+CREATE for renames (no RENAME FUNCTION in ClickHouse)
- ⁴ Dictionaries use CREATE OR REPLACE for all modifications (ClickHouse limitation)
- ⁵ Views use CREATE OR REPLACE for modifications
- ⁶ Views use RENAME TABLE for renames
- ⁷ Materialized view query changes use DROP+CREATE strategy for reliability
- ⁸ Materialized views use table operations (ATTACH/DETACH/DROP/RENAME TABLE)
- ⁹ ALTER ROLE supports RENAME TO and SETTINGS modifications
- ¹⁰ Roles use ALTER ROLE...RENAME TO for rename operations

### Migration Strategy Notes

- **Dependencies**: Proper ordering ensures roles → functions → databases → collections → tables → dictionaries → views
- **Function Support**: CREATE/DROP FUNCTION with lambda expressions (→) and ON CLUSTER support
- **Integration Engines**: Tables using Kafka, RabbitMQ, etc. automatically use DROP+CREATE strategy
- **Cluster Operations**: Full `ON CLUSTER` support, but cluster association cannot be changed after creation
- **Engine Changes**: Not supported for any object type (requires manual migration)
- **Role Management**: Full support for CREATE/ALTER/DROP ROLE plus GRANT/REVOKE operations
- **Smart Rename Detection**: Avoids unnecessary DROP+CREATE when only names change
- **CREATE TABLE AS**: Supports schema copying with automatic column propagation to dependent tables

## Documentation

📚 **[Complete Documentation](https://noibu.github.io/housekeeper/)**

- [Getting Started](https://noibu.github.io/housekeeper/getting-started/installation/) - Installation and setup
- [User Guide](https://noibu.github.io/housekeeper/user-guide/schema-management/) - Schema management and migrations
- [How It Works](https://noibu.github.io/housekeeper/how-it-works/overview/) - Architecture and technical details
- [Examples](https://noibu.github.io/housekeeper/examples/basic-schema/) - Real-world usage patterns

## Quick Start

```bash
# Install
go install github.com/pseudomuto/housekeeper@latest

# Initialize a new project
mkdir my-clickhouse-project && cd my-clickhouse-project
housekeeper init

# Configure database connection (recommended)
export HOUSEKEEPER_DATABASE_URL="localhost:9000"

# Define your schema in db/main.sql, then generate migrations
housekeeper diff

# Apply migrations to your database
housekeeper migrate

# Check migration status
housekeeper status
```

### Connection Configuration

Housekeeper uses a unified connection approach for all database commands:

```bash
# Set once via environment variable (recommended)
export HOUSEKEEPER_DATABASE_URL="localhost:9000"

# Or use --url flag with each command
housekeeper migrate --url localhost:9000
housekeeper status --url localhost:9000
housekeeper schema dump --url localhost:9000
```

Supported connection formats:
- Simple: `localhost:9000`
- Full DSN: `clickhouse://user:password@host:9000/database`
- TCP: `tcp://host:9000?username=user&password=pass`

## Requirements

- **ClickHouse**: 24.0+ 
- **Go**: 1.21+ (for development)

## Installation

### Go Install

```bash
go install github.com/pseudomuto/housekeeper@latest
```

### Docker

```bash
docker pull ghcr.io/pseudomuto/housekeeper:latest
```

### Binary Releases

Download pre-built binaries from the [releases page](https://github.com/pseudomuto/housekeeper/releases).

## Contributing

Contributions are welcome! Please see our [contributing guidelines](.github/CONTRIBUTING.md) for details.

## License

This project is licensed under the GNU General Public License v3.0 - see the [LICENSE.txt](LICENSE.txt) file for details.

