// Package clickhouse provides a comprehensive client for interacting with ClickHouse databases.
//
// This package offers functionality to connect to ClickHouse instances, retrieve
// complete database schemas (databases, tables, views, dictionaries), and execute DDL migrations.
// It serves as the bridge between the Housekeeper tool and actual ClickHouse deployments.
//
// Key features:
//   - Simple connection management with DSN-based configuration
//   - Comprehensive schema extraction for all ClickHouse object types
//   - Dedicated SchemaExtractor for granular control over schema operations
//   - Retrieval of current schemas as parser.SQL structures for migration generation
//   - Execution of migration SQL statements with validation
//   - Automatic filtering of system objects (system, information_schema databases)
//   - Support for clustered ClickHouse deployments (ON CLUSTER detection)
//
// Schema Extraction:
//   - Databases: Complete CREATE DATABASE statements with engine and comments
//   - Tables: Full table definitions with columns, engines, and all table properties
//   - Views: Both regular and materialized views with their definitions
//   - Dictionaries: Dictionary definitions with source, layout, and lifetime configurations
//
// The client is designed to work seamlessly with the parser and migrator packages,
// retrieving current database state in a format that can be directly compared
// with target schemas defined in SQL files for migration generation.
//
// Example usage:
//
//	// Connect to ClickHouse
//	client, err := clickhouse.NewClient(ctx, "localhost:9000")
//	if err != nil {
//	    log.Fatal(err)
//	}
//	defer client.Close()
//
//	// Get complete schema (databases, tables, views, dictionaries)
//	currentSchema, err := client.GetSchema(ctx)
//	if err != nil {
//	    log.Fatal(err)
//	}
//
//	// Or get specific object types
//	tables, err := client.GetTables(ctx)
//	if err != nil {
//	    log.Fatal(err)
//	}
//
//	// Use schema extractor for more control
//	extractor := client.NewSchemaExtractor()
//	databases, err := extractor.ExtractDatabases(ctx)
//	if err != nil {
//	    log.Fatal(err)
//	}
//
//	// Execute a migration
//	err = client.ExecuteMigration(ctx, migrationSQL)
//	if err != nil {
//	    log.Fatal(err)
//	}
//
// The client automatically handles:
//   - Connection pooling and management
//   - Proper reconstruction of DDL statements from system tables
//   - Validation of generated DDL using the built-in parser
//   - Cluster configuration detection and preservation
package clickhouse
