// Package executor provides migration execution functionality for ClickHouse databases.
//
// The executor package handles the safe, atomic execution of database migrations
// with comprehensive error handling, progress tracking, and integrity verification.
// It integrates with the existing revision tracking system to maintain a complete
// audit trail of migration execution.
//
// # Core Components
//
// The package provides the Executor type for migration execution and supporting
// types for configuration and result handling:
//
//   - Executor: Main execution engine for applying migrations
//   - Config: Configuration options for executor creation
//   - ExecutionResult: Detailed results of migration execution
//   - ExecutionStatus: Status enumeration for migration outcomes
//
// # Key Features
//
//   - Statement-by-statement execution with transaction safety
//   - Automatic bootstrap of housekeeper.revisions infrastructure
//   - Progress tracking and comprehensive error recovery
//   - Hash-based integrity verification
//   - Integration with existing revision and migration systems
//   - Cluster-aware execution with proper ON CLUSTER handling
//
// # Usage Example
//
//	// Create executor with ClickHouse client
//	executor := executor.New(executor.Config{
//		ClickHouse:         clickhouseClient,
//		Formatter:          format.New(format.Defaults),
//		HousekeeperVersion: "1.0.0",
//	})
//
//	// Load migrations from directory
//	migrationDir, err := migrator.LoadMigrationDir(os.DirFS("./migrations"))
//	if err != nil {
//		log.Fatal(err)
//	}
//
//	// Execute pending migrations
//	results, err := executor.Execute(ctx, migrationDir.Migrations)
//	if err != nil {
//		log.Fatal(err)
//	}
//
//	// Process results
//	for _, result := range results {
//		switch result.Status {
//		case executor.StatusSuccess:
//			fmt.Printf("✓ %s completed in %v\n", result.Version, result.ExecutionTime)
//		case executor.StatusFailed:
//			fmt.Printf("✗ %s failed: %v\n", result.Version, result.Error)
//		case executor.StatusSkipped:
//			fmt.Printf("- %s already applied\n", result.Version)
//		}
//	}
//
// # Bootstrap Handling
//
// The executor automatically handles the bootstrap process for new ClickHouse
// instances that don't have the housekeeper migration tracking infrastructure:
//
//   - Detects if housekeeper database and revisions table exist
//   - Creates missing infrastructure automatically before migration execution
//   - Uses IF NOT EXISTS clauses for safe, idempotent bootstrap operations
//   - Handles the special case where revisions table doesn't exist on initial setup
//
// # Error Handling and Recovery
//
// The executor provides robust error handling with detailed context:
//
//   - Statement-level error reporting with exact SQL and position
//   - Partial execution tracking for migration recovery scenarios
//   - Comprehensive revision records for failed migrations
//   - Safe execution termination on first failure to prevent cascade issues
//
// # Integration with Revision System
//
// The executor seamlessly integrates with the existing pkg/migrator revision
// tracking system:
//
//   - Loads existing revisions to determine pending migrations
//   - Creates comprehensive revision records for all executions
//   - Records timing, error information, and integrity hashes
//   - Supports both StandardRevision and SnapshotRevision types
//   - Maintains compatibility with existing revision query methods
//
// # Testing and Development
//
// The package is designed with testing in mind and includes:
//
//   - Interface-based ClickHouse client for easy mocking
//   - Comprehensive test coverage with testcontainers integration
//   - Support for dry-run execution modes
//   - Integration with existing Housekeeper test infrastructure
package executor
