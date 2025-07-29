// Package migrator provides database schema migration generation for ClickHouse.
//
// This package compares current and target database schemas to generate executable
// migration files with both UP (apply) and DOWN (rollback) SQL statements.
// It focuses exclusively on database-level operations, ensuring safe and predictable
// migrations for ClickHouse deployments.
//
// Key features:
//   - Intelligent diff detection between current and target schemas
//   - Generation of executable DDL statements (not just comments)
//   - Automatic rollback SQL generation
//   - Support for CREATE, ALTER, and DROP DATABASE operations
//   - Error handling for unsupported operations (engine/cluster changes)
//   - Table-driven testing with YAML fixtures
//
// The migration generation process:
//   1. Parse current database state (from ClickHouse)
//   2. Parse target database state (from SQL files)
//   3. Compare the two states to identify differences
//   4. Generate appropriate DDL for each difference
//   5. Order operations correctly (CREATE -> ALTER -> DROP)
//   6. Generate corresponding rollback statements
//
// Example usage:
//
//	currentGrammar, _ := client.GetDatabasesOnly(ctx)
//	targetGrammar, _ := parser.ParseSQLFromDirectory("schema/")
//	
//	migration, err := migrator.GenerateDatabaseMigration(
//	    currentGrammar,
//	    targetGrammar,
//	    "add_analytics_db"
//	)
//	if err != nil {
//	    // Handle error (e.g., unsupported operation)
//	}
//	
//	// Write migration files
//	os.WriteFile("001_add_analytics_db.up.sql", []byte(migration.Up), 0644)
//	os.WriteFile("001_add_analytics_db.down.sql", []byte(migration.Down), 0644)
//
// The package will return ErrUnsupported for operations that cannot be safely
// automated, such as database engine changes or cluster modifications.
package migrator