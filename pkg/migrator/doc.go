// Package migrator provides functionality for loading, parsing, and managing
// ClickHouse database migration files and directories.
//
// This package handles the core migration lifecycle including:
//   - Loading migration files from filesystem or embedded sources
//   - Parsing SQL migration content using the ClickHouse DDL parser
//   - Managing migration directories with integrity verification
//   - Tracking migration state and execution history
//
// The migrator package integrates with the parser package to provide structured
// access to ClickHouse DDL statements within migration files, enabling tools
// to analyze, validate, and execute schema changes safely.
//
// # Core Components
//
// The package provides three main types:
//
//   - Migration: Represents a single migration with parsed DDL statements
//   - MigrationDir: Represents a collection of migrations from a directory
//   - SumFile: Provides cryptographic integrity verification using chained hashing
//   - Revision: Records migration execution history and audit information
//
// # Basic Usage
//
// Loading a single migration:
//
//	migration, err := migrator.LoadMigration("001_create_users", file)
//	if err != nil {
//		log.Fatal(err)
//	}
//
//	for _, stmt := range migration.Statements {
//		if stmt.CreateTable != nil {
//			fmt.Printf("CREATE TABLE: %s\n", stmt.CreateTable.Name)
//		}
//	}
//
// Loading a migration directory:
//
//	migDir, err := migrator.LoadMigrationDir(os.DirFS("./migrations"))
//	if err != nil {
//		log.Fatal(err)
//	}
//
//	fmt.Printf("Found %d migrations\n", len(migDir.Migrations))
//
// Rehashing a migration directory to update integrity verification:
//
//	// After potential modifications to migration files...
//	err = migDir.Rehash()
//	if err != nil {
//		log.Fatal(err)
//	}
//
//	// Write updated sum file
//	file, err := os.Create("migrations.sum")
//	if err != nil {
//		log.Fatal(err)
//	}
//	defer file.Close()
//
//	_, err = migDir.SumFile.WriteTo(file)
//	if err != nil {
//		log.Fatal(err)
//	}
//
// Creating a sum file for integrity verification:
//
//	sumFile := migrator.NewSumFile()
//	err := sumFile.Add("001_init.sql", strings.NewReader(sqlContent))
//	if err != nil {
//		log.Fatal(err)
//	}
//
//	file, err := os.Create("migrations.sum")
//	if err != nil {
//		log.Fatal(err)
//	}
//	defer file.Close()
//
//	_, err = sumFile.WriteTo(file)
//	if err != nil {
//		log.Fatal(err)
//	}
//
// # Integrity Verification
//
// The SumFile type implements a reverse one-branch Merkle tree using chained
// SHA256 hashing. This provides tamper evidence for migration files:
//
//   - Each migration hash includes the content of the previous migration
//   - Any modification to any file invalidates all subsequent hashes
//   - File ordering changes are detectable through hash chain breaks
//
// This ensures migration file integrity and prevents unauthorized modifications
// in production environments.
//
// # Revision Tracking
//
// The Revision type captures detailed migration execution information:
//
//	revision := &migrator.Revision{
//		Version:            "20240101120000_create_users",
//		ExecutedAt:         time.Now(),
//		ExecutionTime:      2 * time.Second,
//		Kind:               migrator.StandardRevision,
//		Applied:            5,
//		Total:              5,
//		Hash:               "migration_content_hash",
//		PartialHashes:      []string{"stmt1_hash", "stmt2_hash", ...},
//		HousekeeperVersion: "1.0.0",
//	}
//
// This provides complete audit trail information for compliance, debugging,
// and rollback scenarios.
package migrator
