# Housekeeper

[![CI](https://github.com/pseudomuto/housekeeper/workflows/CI/badge.svg)](https://github.com/pseudomuto/housekeeper/actions?query=workflow%3ACI)
[![GoDoc](https://godoc.org/github.com/pseudomuto/housekeeper?status.svg)](https://godoc.org/github.com/pseudomuto/housekeeper)
[![Go Report Card](https://goreportcard.com/badge/github.com/pseudomuto/housekeeper)](https://goreportcard.com/report/github.com/pseudomuto/housekeeper)

A ClickHouse schema management tool heavily inspired by [Atlas](https://atlasgo.io/), built specifically to address the gaps in ClickHouse support that Atlas couldn't fill.

> **NOTE**: This is very much still a WIP and heavily focused on my own needs. I intend to continue development as
available. And of course, PRs are welcome and encouraged.

## Why This Exists

While Atlas is an excellent database schema management tool, its ClickHouse support falls short of what's needed for production ClickHouse deployments. Critical ClickHouse features like `ON CLUSTER` operations, `PARTITION BY` clauses, materialized view management, and dictionary operations either weren't supported or had significant limitations.

Rather than wait for Atlas to catch up with ClickHouse's unique requirements, Housekeeper was created as a purpose-built solution that understands ClickHouse's distributed nature, specialized data types, and advanced features from the ground up.

## Key Features

- **Complete ClickHouse DDL Support** - Full support for databases, tables, dictionaries, views, materialized views, named collections, and roles
- **Cluster-Aware Operations** - Native `ON CLUSTER` support for distributed ClickHouse deployments
- **Intelligent Migration Generation** - Smart schema comparison with proper operation ordering and dependency management
- **Modern Parser Architecture** - Built with participle for robust, maintainable SQL parsing
- **Professional SQL Formatting** - Clean, consistent output optimized for ClickHouse
- **Comprehensive Testing** - Extensive test suite with 100% DDL operation coverage

## Supported Migration Operations

| Object Type | CREATE | ALTER | ATTACH | DETACH | DROP | RENAME | GRANT/REVOKE | Notes |
|------------|--------|-------|---------|---------|------|--------|--------------|-------|
| **Database** | ✅ | ✅¹ | ✅ | ✅ | ✅ | ✅ | N/A | ¹Comment changes only |
| **Named Collection** | ✅ | ❌² | ❌ | ❌ | ✅ | ✅ | N/A | ²Uses CREATE OR REPLACE |
| **Table** | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | N/A | Full ALTER support |
| **Dictionary** | ✅ | ❌³ | ✅ | ✅ | ✅ | ✅ | N/A | ³Uses CREATE OR REPLACE |
| **View** | ✅ | ❌⁴ | ✅ | ✅ | ✅ | ✅⁵ | N/A | ⁴Uses CREATE OR REPLACE |
| **Materialized View** | ✅ | ❌⁶ | ✅⁷ | ✅⁷ | ✅⁷ | ✅⁷ | N/A | ⁶Query changes use DROP+CREATE |
| **Role** | ✅ | ✅⁸ | ❌ | ❌ | ✅ | ✅⁹ | ✅ | ⁸Settings and rename only |

**Legend:**
- ✅ Fully supported
- ❌ Not supported (alternative strategy used)
- N/A Not applicable  
- ¹ ALTER DATABASE only supports comment modifications
- ² Named collections use CREATE OR REPLACE for all modifications
- ³ Dictionaries use CREATE OR REPLACE for all modifications (ClickHouse limitation)
- ⁴ Views use CREATE OR REPLACE for modifications
- ⁵ Views use RENAME TABLE for renames
- ⁶ Materialized view query changes use DROP+CREATE strategy for reliability
- ⁷ Materialized views use table operations (ATTACH/DETACH/DROP/RENAME TABLE)
- ⁸ ALTER ROLE supports RENAME TO and SETTINGS modifications
- ⁹ Roles use ALTER ROLE...RENAME TO for rename operations

### Migration Strategy Notes

- **Dependencies**: Proper ordering ensures roles → databases → collections → tables → dictionaries → views
- **Integration Engines**: Tables using Kafka, RabbitMQ, etc. automatically use DROP+CREATE strategy
- **Cluster Operations**: Full `ON CLUSTER` support, but cluster association cannot be changed after creation
- **Engine Changes**: Not supported for any object type (requires manual migration)
- **Role Management**: Full support for CREATE/ALTER/DROP ROLE plus GRANT/REVOKE operations
- **Smart Rename Detection**: Avoids unnecessary DROP+CREATE when only names change

## Documentation

📚 **[Complete Documentation](https://pseudomuto.github.io/housekeeper/)**

- [Getting Started](https://pseudomuto.github.io/housekeeper/getting-started/installation/) - Installation and setup
- [User Guide](https://pseudomuto.github.io/housekeeper/user-guide/schema-management/) - Schema management and migrations
- [How It Works](https://pseudomuto.github.io/housekeeper/how-it-works/overview/) - Architecture and technical details
- [Examples](https://pseudomuto.github.io/housekeeper/examples/basic-schema/) - Real-world usage patterns

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

