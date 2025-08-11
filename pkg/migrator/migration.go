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
package migrator

import (
	"crypto/sha256"
	"io"
	"io/fs"
	"path/filepath"
	"slices"
	"strings"

	"github.com/pkg/errors"
	"github.com/pseudomuto/housekeeper/pkg/parser"
)

type (
	// Migration represents a single ClickHouse database migration containing
	// a version identifier and the parsed DDL statements to be executed.
	//
	// Migrations are typically loaded from .sql files in a migration directory
	// and contain CREATE, ALTER, DROP, and other ClickHouse DDL operations
	// that modify database schema or structure.
	//
	// Example migration content:
	//   CREATE TABLE users (id UInt64, name String) ENGINE = MergeTree() ORDER BY id;
	//   ALTER TABLE users ADD COLUMN email String DEFAULT '';
	Migration struct {
		// Version is the migration identifier, typically derived from the filename
		// or a timestamp. Used for ordering and tracking migration application.
		Version string

		// Statements contains the parsed ClickHouse DDL statements from the
		// migration file. Each statement represents a single DDL operation
		// such as CREATE TABLE, ALTER DATABASE, etc.
		Statements []*parser.Statement
	}

	// MigrationDir represents a collection of migrations loaded from a directory
	// along with integrity verification data.
	//
	// This structure provides a complete view of a migration directory including
	// all migration files and their associated sum file for integrity checking.
	// The migrations are automatically sorted in lexical order to ensure
	// consistent application ordering.
	MigrationDir struct {
		// Migrations contains all migration files found in the directory,
		// sorted in lexical order by filename to ensure deterministic
		// execution order.
		Migrations []*Migration

		// SumFile contains integrity verification data for the migration
		// directory, allowing detection of modified or corrupted migration
		// files. This field is always present and provides cryptographic
		// verification of migration file contents.
		SumFile *SumFile

		// checkpoint stores the loaded checkpoint if one exists in the directory.
		// This is kept private and accessed through HasCheckpoint() and GetCheckpoint().
		checkpoint *Checkpoint

		// fs stores the filesystem reference for reloading operations.
		// This is kept private to ensure controlled access through methods.
		fs fs.FS
	}
)

// LoadMigrationDir loads all migration files from the specified filesystem and returns
// a MigrationDir containing parsed migrations and integrity verification data.
//
// This function walks the provided filesystem in lexical order, loading all .sql files
// as migrations and any .sum files for integrity verification. The filesystem can be
// a regular directory, embedded filesystem, or any implementation of fs.FS.
//
// Checkpoint files (marked with -- housekeeper:checkpoint) are automatically detected
// and stored separately from regular migrations.
//
// Supported file extensions:
//   - .sql: Migration files containing ClickHouse DDL statements or checkpoint data
//   - .sum: Sum files containing integrity hashes (currently loaded but not processed)
//
// Example usage:
//
//	// Load from regular filesystem directory
//	migDir, err := migrator.LoadMigrationDir(os.DirFS("./migrations"))
//	if err != nil {
//		log.Fatal(err)
//	}
//
//	// Load from embedded filesystem
//	//go:embed migrations/*.sql
//	var migrationsFS embed.FS
//	migDir, err := migrator.LoadMigrationDir(migrationsFS)
//	if err != nil {
//		log.Fatal(err)
//	}
//
//	// Process loaded migrations
//	for _, mig := range migDir.Migrations {
//		fmt.Printf("Migration %s has %d statements\n", mig.Version, len(mig.Statements))
//		for _, stmt := range mig.Statements {
//			if stmt.CreateTable != nil {
//				fmt.Printf("  CREATE TABLE: %s\n", stmt.CreateTable.Name)
//			}
//		}
//	}
//
//	// Check for checkpoint
//	if migDir.HasCheckpoint() {
//		checkpoint, _ := migDir.GetCheckpoint()
//		fmt.Printf("Found checkpoint: %s\n", checkpoint.Version)
//	}
//
// Returns an error if the directory cannot be read or any migration file
// contains invalid ClickHouse DDL syntax.
func LoadMigrationDir(dir fs.FS) (*MigrationDir, error) {
	exts := []string{".sql", ".sum"}
	mig := &MigrationDir{
		fs:      dir,
		SumFile: NewSumFile(),
	}

	// NB: WalkDir always walks in lexical order.
	if err := fs.WalkDir(dir, ".", func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}

		ext := filepath.Ext(path)
		if !slices.Contains(exts, ext) {
			return nil
		}

		f, err := dir.Open(path)
		if err != nil {
			return errors.Wrapf(err, "failed to open: %s", path)
		}
		defer func() { _ = f.Close() }()

		if ext == ".sql" {
			// Read content for both migration loading and sum file
			content, err := io.ReadAll(f)
			if err != nil {
				return errors.Wrapf(err, "failed to read migration: %s", path)
			}

			// Check if this is a checkpoint file
			reader := strings.NewReader(string(content))
			isCheckpoint, err := IsCheckpoint(reader)
			if err != nil {
				return errors.Wrapf(err, "failed to check if file is checkpoint: %s", path)
			}

			if isCheckpoint {
				// Load as checkpoint
				reader := strings.NewReader(string(content))
				checkpoint, err := LoadCheckpoint(reader)
				if err != nil {
					return errors.Wrapf(err, "failed to load checkpoint: %s", path)
				}
				mig.checkpoint = checkpoint
			} else {
				// Load as regular migration - extract filename without extension as version
				filename := filepath.Base(path)
				version := filename[:strings.Index(filename, ".")]
				m, err := LoadMigration(version, strings.NewReader(string(content)))
				if err != nil {
					return errors.Wrapf(err, "failed to load migration: %s", path)
				}
				mig.Migrations = append(mig.Migrations, m)
			}

			// Add to sum file
			err = mig.SumFile.Add(path, strings.NewReader(string(content)))
			if err != nil {
				return errors.Wrapf(err, "failed to add migration to sum file: %s", path)
			}
		}

		return nil
	}); err != nil {
		return nil, err
	}

	return mig, nil
}

// LoadMigration creates a Migration from the provided io.Reader containing ClickHouse DDL statements.
//
// This function parses the SQL content using the ClickHouse DDL parser and creates a Migration
// structure with the specified version and parsed statements. The version is typically a
// timestamp or sequential identifier used for migration ordering.
//
// The reader content should contain valid ClickHouse DDL statements such as:
//   - CREATE DATABASE, CREATE TABLE, CREATE VIEW, CREATE DICTIONARY
//   - ALTER TABLE, ALTER DATABASE operations
//   - DROP statements for cleanup
//   - Any other supported ClickHouse DDL operations
//
// Example usage:
//
//	// Load from string
//	sql := `
//		CREATE TABLE users (
//			id UInt64,
//			name String,
//			email String DEFAULT ''
//		) ENGINE = MergeTree() ORDER BY id;
//
//		ALTER TABLE users ADD COLUMN created_at DateTime DEFAULT now();
//	`
//	migration, err := migrator.LoadMigration("001_create_users", strings.NewReader(sql))
//	if err != nil {
//		log.Fatal(err)
//	}
//
//	// Load from file
//	file, err := os.Open("001_create_users.sql")
//	if err != nil {
//		log.Fatal(err)
//	}
//	defer file.Close()
//
//	migration, err = migrator.LoadMigration("001_create_users", file)
//	if err != nil {
//		log.Fatal(err)
//	}
//
//	// Access parsed statements
//	fmt.Printf("Migration %s contains %d statements\n", migration.Version, len(migration.Statements))
//	for i, stmt := range migration.Statements {
//		if stmt.CreateTable != nil {
//			table := stmt.CreateTable
//			name := table.Name
//			if table.Database != nil {
//				name = *table.Database + "." + name
//			}
//			fmt.Printf("  Statement %d: CREATE TABLE %s\n", i+1, name)
//		}
//		if stmt.AlterTable != nil {
//			alter := stmt.AlterTable
//			name := alter.Name
//			if alter.Database != nil {
//				name = *alter.Database + "." + name
//			}
//			fmt.Printf("  Statement %d: ALTER TABLE %s (%d operations)\n",
//				i+1, name, len(alter.Operations))
//		}
//	}
//
// Returns an error if the reader content contains invalid ClickHouse DDL syntax
// or if the reader cannot be read.
func LoadMigration(v string, r io.Reader) (*Migration, error) {
	sql, err := parser.Parse(r)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to parse: %s.sql", v)
	}

	return &Migration{
		Version:    v,
		Statements: sql.Statements,
	}, nil
}

// Rehash reloads all migration files from the filesystem and recalculates the SumFile.
//
// This method is useful for:
//   - Verifying migration file integrity after potential modifications
//   - Regenerating the sum file after adding or modifying migrations
//   - Detecting unauthorized changes to migration files
//
// The method performs the following operations:
//  1. Clears existing migrations and sum file
//  2. Reloads all .sql files from the filesystem in lexical order
//  3. Recalculates the chained SHA256 hashes for each migration
//  4. Updates the SumFile with new integrity verification data
//
// Example usage:
//
//	migDir, err := migrator.LoadMigrationDir(os.DirFS("./migrations"))
//	if err != nil {
//		log.Fatal(err)
//	}
//
//	// After some time or potential changes...
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
// Returns an error if the filesystem cannot be read, any migration file
// contains invalid SQL, or if the filesystem reference is nil.
func (m *MigrationDir) Rehash() error {
	if m.fs == nil {
		return errors.New("cannot rehash: filesystem reference is nil")
	}

	// Clear existing data
	m.Migrations = nil
	m.SumFile = NewSumFile()

	// Track .sql files for sum file generation
	var sqlFiles []string

	// Walk directory in lexical order
	if err := fs.WalkDir(m.fs, ".", func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}

		// Only process .sql files
		if filepath.Ext(path) != ".sql" {
			return nil
		}

		sqlFiles = append(sqlFiles, path)

		// Load and parse migration
		f, err := m.fs.Open(path)
		if err != nil {
			return errors.Wrapf(err, "failed to open: %s", path)
		}
		defer func() { _ = f.Close() }()

		filename := filepath.Base(path)
		version := filename[:strings.Index(filename, ".")]
		migration, err := LoadMigration(version, f)
		if err != nil {
			return errors.Wrapf(err, "failed to load migration: %s", path)
		}

		m.Migrations = append(m.Migrations, migration)
		return nil
	}); err != nil {
		return errors.Wrap(err, "failed to walk migration directory")
	}

	// Recalculate sum file with all migrations in order
	for _, path := range sqlFiles {
		f, err := m.fs.Open(path)
		if err != nil {
			return errors.Wrapf(err, "failed to open for hashing: %s", path)
		}

		err = m.SumFile.Add(path, f)
		_ = f.Close()
		if err != nil {
			return errors.Wrapf(err, "failed to hash migration: %s", path)
		}
	}

	return nil
}

// Validate verifies the integrity of the MigrationDir by ensuring that all migrations
// are present in the sum file and that the sum file validates correctly against
// the current migration content.
//
// This method provides comprehensive validation of the migration directory by:
//  1. Checking that all loaded migrations have corresponding entries in the sum file
//  2. Validating the sum file's chained hashes against the actual migration content
//  3. Detecting any missing or modified migration files
//
// The validation process:
//  1. Verifies that every migration in the directory has an entry in the sum file
//  2. Reads each migration file content from the filesystem
//  3. Uses the sum file's Validate method to verify cryptographic integrity
//  4. Returns false if any migration is missing from sum file or content doesn't match
//
// Example usage:
//
//	// Load migration directory
//	migDir, err := migrator.LoadMigrationDir(os.DirFS("./migrations"))
//	if err != nil {
//		log.Fatal(err)
//	}
//
//	// Validate integrity
//	isValid, err := migDir.Validate()
//	if err != nil {
//		log.Fatal(err)
//	}
//
//	if isValid {
//		fmt.Println("Migration directory is valid and unmodified")
//	} else {
//		fmt.Println("Migration directory has integrity issues!")
//	}
//
//	// Handle validation in migration pipeline
//	if !isValid {
//		log.Fatal("Migration integrity check failed - cannot proceed")
//	}
//	fmt.Println("All migrations validated successfully")
//
// Returns false if:
//   - Any migration is missing from the sum file
//   - Any migration content doesn't match its stored hash
//   - The sum file's chained hash validation fails
//
// Returns an error if:
//   - The filesystem cannot be accessed
//   - Any migration file cannot be read
//   - The filesystem reference is nil
//
// Note: This method requires a filesystem reference (fs field) to read migration
// content. If the MigrationDir was loaded without a filesystem or the reference
// is nil, an error will be returned.
func (m *MigrationDir) Validate() (bool, error) {
	if m.fs == nil {
		return false, errors.New("cannot validate: filesystem reference is nil")
	}

	// Create a temporary sum file from current migration files to compare
	tempSumFile := NewSumFile()
	for _, migration := range m.Migrations {
		filePath := migration.Version + ".sql"

		file, err := m.fs.Open(filePath)
		if err != nil {
			return false, errors.Wrapf(err, "failed to open migration file: %s", filePath)
		}

		err = tempSumFile.Add(filePath, file)
		_ = file.Close()
		if err != nil {
			return false, errors.Wrapf(err, "failed to hash migration: %s", filePath)
		}
	}

	// Compute a hash of the temp sum file content for comparison
	tempHash, err := computeSumFileHash(tempSumFile)
	if err != nil {
		return false, errors.Wrap(err, "failed to compute temp sum file hash")
	}

	// Compute a hash of the stored sum file content for comparison
	storedHash, err := computeSumFileHash(m.SumFile)
	if err != nil {
		return false, errors.Wrap(err, "failed to compute stored sum file hash")
	}

	// Compare the computed hashes
	return equalHashes(tempHash, storedHash), nil
}

// HasCheckpoint returns true if a checkpoint was loaded from the migration directory.
//
// Example usage:
//
//	migDir, err := migrator.LoadMigrationDir(os.DirFS("./migrations"))
//	if err != nil {
//		log.Fatal(err)
//	}
//
//	if migDir.HasCheckpoint() {
//		fmt.Println("Directory contains a checkpoint")
//	}
func (m *MigrationDir) HasCheckpoint() bool {
	return m.checkpoint != nil
}

// GetCheckpoint returns the loaded checkpoint, if one exists.
//
// Returns nil if no checkpoint was found in the migration directory.
//
// Example usage:
//
//	migDir, err := migrator.LoadMigrationDir(os.DirFS("./migrations"))
//	if err != nil {
//		log.Fatal(err)
//	}
//
//	if checkpoint := migDir.GetCheckpoint(); checkpoint != nil {
//		fmt.Printf("Found checkpoint: %s (%s)\n",
//			checkpoint.Version, checkpoint.Description)
//		fmt.Printf("Includes %d migrations\n", len(checkpoint.IncludedMigrations))
//	}
func (m *MigrationDir) GetCheckpoint() *Checkpoint {
	return m.checkpoint
}

// CreateCheckpoint generates a new checkpoint from all current migrations.
//
// This method creates a checkpoint that consolidates all migrations currently
// in the directory. The checkpoint can then be written to a file and the old
// migration files can be safely removed.
//
// Example usage:
//
//	migDir, err := migrator.LoadMigrationDir(os.DirFS("./migrations"))
//	if err != nil {
//		log.Fatal(err)
//	}
//
//	checkpoint, err := migDir.CreateCheckpoint(
//		"20240810120000_checkpoint",
//		"Q3 2024 Release",
//	)
//	if err != nil {
//		log.Fatal(err)
//	}
//
//	// Write checkpoint to file
//	file, err := os.Create("migrations/20240810120000_checkpoint.sql")
//	if err != nil {
//		log.Fatal(err)
//	}
//	defer file.Close()
//
//	_, err = checkpoint.WriteTo(file)
//	if err != nil {
//		log.Fatal(err)
//	}
func (m *MigrationDir) CreateCheckpoint(version, description string) (*Checkpoint, error) {
	if len(m.Migrations) == 0 {
		return nil, errors.New("no migrations to checkpoint")
	}

	// If there's an existing checkpoint, only include migrations after it
	var migrationsToInclude []*Migration
	if m.checkpoint != nil {
		// Find migrations not already in the checkpoint
		checkpointVersions := make(map[string]bool)
		for _, v := range m.checkpoint.IncludedMigrations {
			checkpointVersions[v] = true
		}

		for _, mig := range m.Migrations {
			if !checkpointVersions[mig.Version] {
				migrationsToInclude = append(migrationsToInclude, mig)
			}
		}

		if len(migrationsToInclude) == 0 {
			return nil, errors.New("no new migrations to checkpoint")
		}
	} else {
		migrationsToInclude = m.Migrations
	}

	return GenerateCheckpoint(version, description, migrationsToInclude)
}

// GetMigrationsAfterCheckpoint returns all migrations that are not included in the checkpoint.
//
// If no checkpoint exists, returns all migrations.
//
// Example usage:
//
//	migDir, err := migrator.LoadMigrationDir(os.DirFS("./migrations"))
//	if err != nil {
//		log.Fatal(err)
//	}
//
//	newMigrations := migDir.GetMigrationsAfterCheckpoint()
//	fmt.Printf("Found %d migrations after checkpoint\n", len(newMigrations))
func (m *MigrationDir) GetMigrationsAfterCheckpoint() []*Migration {
	if m.checkpoint == nil {
		return m.Migrations
	}

	// Create a set of checkpoint migration versions for quick lookup
	checkpointVersions := make(map[string]bool)
	for _, v := range m.checkpoint.IncludedMigrations {
		checkpointVersions[v] = true
	}

	// Filter out migrations that are in the checkpoint
	var afterCheckpoint []*Migration
	for _, mig := range m.Migrations {
		if !checkpointVersions[mig.Version] {
			afterCheckpoint = append(afterCheckpoint, mig)
		}
	}

	return afterCheckpoint
}

// computeSumFileHash computes a SHA256 hash of all the entry hashes in a SumFile.
// This provides a simple way to compare two sum files for equality.
func computeSumFileHash(sumFile *SumFile) ([]byte, error) {
	var buf strings.Builder
	_, err := sumFile.WriteTo(&buf)
	if err != nil {
		return nil, err
	}

	// Hash the entire serialized content (including the total hash line)
	h := sha256.New()
	_, err = h.Write([]byte(buf.String()))
	if err != nil {
		return nil, err
	}

	return h.Sum(nil), nil
}
