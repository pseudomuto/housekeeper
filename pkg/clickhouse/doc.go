// Package clickhouse provides a client for interacting with ClickHouse databases.
//
// This package offers functionality to connect to ClickHouse instances, retrieve
// current database schemas, and execute DDL migrations. It serves as the bridge
// between the Housekeeper tool and actual ClickHouse deployments.
//
// Key features:
//   - Simple connection management with DSN-based configuration
//   - Retrieval of current database schemas as parser.Grammar structures
//   - Execution of migration SQL statements
//   - Generation of CREATE DATABASE statements from current state
//   - Filtering of system databases (INFORMATION_SCHEMA, information_schema, system)
//
// The client is designed to work seamlessly with the parser and migrator packages,
// retrieving current database state in a format that can be directly compared
// with target schemas defined in SQL files.
//
// Example usage:
//
//	// Connect to ClickHouse
//	client, err := clickhouse.NewClient("localhost:9000")
//	if err != nil {
//	    log.Fatal(err)
//	}
//	defer client.Close()
//	
//	// Get current database schema
//	currentSchema, err := client.GetDatabasesOnly(ctx)
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
//   - Proper escaping of database names and values
//   - Reconstruction of CREATE DATABASE statements with all parameters
package clickhouse