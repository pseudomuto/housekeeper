# Housekeeper Documentation

Welcome to Housekeeper, a modern command-line tool for managing ClickHouse schema migrations with comprehensive support for databases, dictionaries, views, and tables.

## What is Housekeeper?

Housekeeper is designed to make ClickHouse schema management simple, reliable, and maintainable. It provides:

- **Complete ClickHouse DDL Support**: Full support for databases, tables, dictionaries, and views
- **Intelligent Migration Generation**: Smart comparison and migration creation with proper operation ordering
- **Project Management**: Complete project initialization and schema compilation with import directives
- **Professional SQL Formatting**: Clean, readable SQL output with ClickHouse-optimized formatting
- **Cluster-Aware Operations**: Full support for `ON CLUSTER` distributed DDL operations
- **Docker Integration**: Built-in ClickHouse container management for testing and validation

## Key Features

### 🏗️ Complete DDL Support
Support for all major ClickHouse operations including complex data types, advanced table engines, dictionary sources, and materialized views.

### 🧠 Intelligent Migrations
Automatically generates optimal migration strategies, including rename detection to avoid unnecessary DROP+CREATE operations.

### 📁 Project Organization
Modular schema organization with import directives, allowing you to split complex schemas into manageable files.

### 🔍 Robust Parsing
Built with a modern participle-based parser for reliable, maintainable SQL parsing instead of fragile regex patterns.

### 🐳 Docker Integration
Test your migrations against real ClickHouse instances using built-in Docker container management.

## Quick Start

Get started with Housekeeper in just a few commands:

```bash
# Install Housekeeper
go install github.com/pseudomuto/housekeeper@latest

# Initialize a new project
mkdir my-clickhouse-project && cd my-clickhouse-project
housekeeper init

# Start development server
housekeeper dev up

# Generate migrations from schema changes
housekeeper diff
```

## Documentation Structure

- **[Getting Started](getting-started/installation.md)** - Installation, setup, and your first migration
- **[User Guide](user-guide/schema-management.md)** - Day-to-day usage patterns and best practices
- **[How It Works](how-it-works/overview.md)** - Deep dive into Housekeeper's architecture and algorithms
- **[Advanced Topics](advanced/cluster-management.md)** - Cluster management, performance tuning, and troubleshooting
- **[Examples](examples/basic-schema.md)** - Real-world examples and common patterns

## Community and Support

- **GitHub**: [pseudomuto/housekeeper](https://github.com/pseudomuto/housekeeper)
- **Issues**: [Report bugs or request features](https://github.com/pseudomuto/housekeeper/issues)
- **Discussions**: [Community discussions](https://github.com/pseudomuto/housekeeper/discussions)

Ready to get started? Head over to the [Installation Guide](getting-started/installation.md) to begin your journey with Housekeeper!