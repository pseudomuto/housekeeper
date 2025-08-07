# ClickHouse Schema Extraction Tests

This directory contains test data and configuration for testing the ClickHouse schema extraction functionality.

## Files

- `clickhouse-config.xml` - ClickHouse server configuration with Keeper and cluster setup
- `sample_schema.sql` - Sample schema with ON CLUSTER statements and Replicated tables

## Configuration Details

### ClickHouse Configuration
The `clickhouse-config.xml` includes:
- **Keeper configuration**: Embedded ZooKeeper replacement for replication coordination
- **Single-node cluster**: `test_cluster` with one shard and one replica
- **Macros**: Predefined macros for replica naming (`{cluster}`, `{shard}`, `{replica}`)
- **Network settings**: HTTP port 8123, TCP port 9000

### Sample Schema
The `sample_schema.sql` demonstrates:
- **Databases**: Created with `ON CLUSTER test_cluster`
- **Replicated Tables**: Using `ReplicatedMergeTree`, `ReplicatedReplacingMergeTree`, etc.
- **Dictionaries**: With various source types and layouts
- **Views**: Both regular and materialized views with `ON CLUSTER`
- **Advanced Features**: TTL, partitioning, compression, complex data types

## Running Tests

The tests use the testcontainers ClickHouse module to spin up a real ClickHouse instance:

```bash
# Run all tests (requires Docker for testcontainer tests)
go test ./pkg/clickhouse

# Run with short flag to skip testcontainer tests
go test ./pkg/clickhouse -short

# Update golden files (run after making changes to schema extraction)
go test ./pkg/clickhouse -update
```

## Requirements

- **Docker**: Required for testcontainers
- **Docker permissions**: User must be able to run Docker containers
- **Network access**: Container needs to download ClickHouse image

## Test Behavior

The schema extraction test (`TestDumpSchema`) validates the complete functionality and:

1. Starts a ClickHouse container using the specialized testcontainers ClickHouse module
2. Automatically loads the sample schema via initialization scripts
3. Tests the complete `DumpSchema()` functionality via `client.GetSchema()`
4. Formats the extracted schema using the `format` package
5. **Compares output against golden file** (`expected_schema.sql`) for exact validation
6. Tests all individual extraction methods (`GetDatabases`, `GetTables`, `GetViews`, `GetDictionaries`)
7. Uses built-in ClickHouse module configuration for reliable setup

### Golden File Testing

The test uses golden file comparison to validate the exact output of schema extraction:

- **Golden file**: `testdata/expected_schema.sql` contains the expected formatted output
- **Comparison**: Test formats extracted schema and compares against golden file
- **Updates**: Use `-update` flag to update golden file when extraction logic changes
- **Benefits**: Catches any changes in output format, ordering, or content automatically

## Notes

- **Single-node limitations**: Some cluster features won't work in single-node setup
- **ON CLUSTER preservation**: Single-node ClickHouse may not preserve ON CLUSTER in system tables
- **Error tolerance**: Test is designed to handle expected failures in simplified environment
- **Replicated engines**: May fall back to non-replicated engines without proper cluster setup

The integration test focuses on verifying the extraction logic works correctly rather than the full cluster functionality.