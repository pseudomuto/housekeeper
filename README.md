# Housekeeper

[![CI](https://github.com/pseudomuto/housekeeper/workflows/CI/badge.svg)](https://github.com/pseudomuto/housekeeper/actions?query=workflow%3ACI)
[![GoDoc](https://godoc.org/github.com/pseudomuto/housekeeper?status.svg)](https://godoc.org/github.com/pseudomuto/housekeeper)
[![Go Report Card](https://goreportcard.com/badge/github.com/pseudomuto/housekeeper)](https://goreportcard.com/report/github.com/pseudomuto/housekeeper)

A ClickHouse schema management tool heavily inspired by [Atlas](https://atlasgo.io/), built specifically to address the gaps in ClickHouse support that Atlas couldn't fill.

## Why This Exists

While Atlas is an excellent database schema management tool, its ClickHouse support falls short of what's needed for production ClickHouse deployments. Critical ClickHouse features like `ON CLUSTER` operations, `PARTITION BY` clauses, materialized view management, and dictionary operations either weren't supported or had significant limitations.

Rather than wait for Atlas to catch up with ClickHouse's unique requirements, Housekeeper was created as a purpose-built solution that understands ClickHouse's distributed nature, specialized data types, and advanced features from the ground up.

## Key Features

- **Complete ClickHouse DDL Support** - Full support for databases, tables, dictionaries, views, and materialized views
- **Cluster-Aware Operations** - Native `ON CLUSTER` support for distributed ClickHouse deployments
- **Intelligent Migration Generation** - Smart schema comparison with proper operation ordering
- **Modern Parser Architecture** - Built with participle for robust, maintainable SQL parsing
- **Professional SQL Formatting** - Clean, consistent output optimized for ClickHouse
- **Comprehensive Testing** - Extensive test suite with 100% DDL operation coverage

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

# Define your schema in db/main.sql, then generate migrations
housekeeper diff
```

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

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.