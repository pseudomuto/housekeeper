// Package migrator provides comprehensive schema migration generation for ClickHouse.
//
// This package compares current and target schemas to generate executable
// migration files with both UP (apply) and DOWN (rollback) SQL statements.
// It supports all major ClickHouse schema objects including databases, tables,
// dictionaries, and views, ensuring safe and predictable migrations for
// ClickHouse deployments.
//
// Key features:
//   - Intelligent diff detection between current and target schemas
//   - Generation of executable DDL statements (not just comments)
//   - Automatic rollback SQL generation with proper operation ordering
//   - Complete support for all schema objects: databases, tables, dictionaries, views
//   - Smart rename detection to avoid unnecessary DROP+CREATE operations
//   - Different migration strategies for different object types
//   - Error handling for unsupported operations (engine/cluster changes)
//   - Comprehensive testing with YAML fixtures and table-driven tests
//
// Supported Operations:
//   - Database operations: CREATE, ALTER, ATTACH, DETACH, DROP, RENAME DATABASE
//   - Table operations: CREATE, ALTER, ATTACH, DETACH, DROP, RENAME TABLE
//   - Dictionary operations: CREATE OR REPLACE, ATTACH, DETACH, DROP, RENAME DICTIONARY
//   - View operations: CREATE, ALTER, ATTACH, DETACH, DROP, RENAME for both regular and materialized views
//
// Migration Strategies:
//   - Databases: Standard CREATE, ALTER, DROP operations
//   - Tables: Full DDL support including column modifications
//   - Dictionaries: CREATE OR REPLACE for modifications (since they can't be altered)
//   - Regular Views: CREATE OR REPLACE for modifications
//   - Materialized Views: ALTER TABLE MODIFY QUERY for query changes
//
// The migration generation process:
//  1. Parse current schema state (from ClickHouse or SQL files)
//  2. Parse target schema state (from SQL files)
//  3. Compare the two states using intelligent algorithms
//  4. Generate appropriate DDL for each difference with correct strategies
//  5. Order operations correctly (databases → tables → dictionaries → views; CREATE → ALTER → RENAME → DROP)
//  6. Generate corresponding rollback statements in reverse order
//
// Example usage:
//
//	// Parse current schema (from ClickHouse or existing SQL files)
//	currentGrammar, _ := parser.ParseSQL("CREATE DATABASE analytics;")
//
//	// Parse target schema (from SQL files)
//	targetGrammar, _ := parser.ParseSQLFromDirectory("schema/")
//
//	// Generate comprehensive migration
//	migration, err := migrator.GenerateMigration(
//	    currentGrammar,
//	    targetGrammar,
//	    "setup_analytics_schema"
//	)
//	if err != nil {
//	    // Handle error (e.g., unsupported operation like engine changes)
//	    log.Fatalf("Migration generation failed: %v", err)
//	}
//
//	// Write timestamped migration files
//	timestamp := time.Now().Format("20060102150405")
//	upFile := fmt.Sprintf("%s_setup_analytics_schema.up.sql", timestamp)
//	downFile := fmt.Sprintf("%s_setup_analytics_schema.down.sql", timestamp)
//
//	os.WriteFile(upFile, []byte(migration.Up), 0644)
//	os.WriteFile(downFile, []byte(migration.Down), 0644)
//
// The package will return errors for operations that cannot be safely
// automated, such as database engine changes, cluster modifications, or
// certain complex table structure changes that require manual intervention.
package migrator
