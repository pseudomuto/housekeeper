package migrator

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/pkg/errors"
	"github.com/pseudomuto/housekeeper/pkg/parser"
)

// ErrNoDiff is returned when no differences are found between current and target schemas
var ErrNoDiff = errors.New("no differences found")

type (
	// Migration represents a database migration with up and down SQL statements
	Migration struct {
		Version   string    // Version is the timestamp-based version (e.g., "20240101120000")
		Name      string    // Name is the human-readable name of the migration
		Up        string    // Up contains SQL statements to apply the migration
		Down      string    // Down contains SQL statements to rollback the migration
		Timestamp time.Time // Timestamp is when the migration was generated
	}
)

// GenerateMigration creates a comprehensive migration by comparing current and target schema states.
// It analyzes the differences between the current schema and the desired target schema,
// then generates appropriate DDL statements for both applying (UP) and rolling back (DOWN) the changes.
//
// The migration includes all schema objects (databases, tables, dictionaries, views), processing them in the correct order:
// - UP migration: Databases → Tables → Dictionaries → Views (CREATE → ALTER → RENAME → DROP)
// - DOWN migration: Views → Dictionaries → Tables → Databases (reverse order, reverse operations)
//
// Migration strategies for different object types:
//   - Databases: Standard DDL operations (CREATE, ALTER, DROP, RENAME)
//   - Tables: Full DDL support including column modifications (CREATE, ALTER, DROP, RENAME)
//   - Dictionaries: CREATE OR REPLACE for modifications (since they can't be altered)
//   - Regular Views: CREATE OR REPLACE for modifications
//   - Materialized Views: ALTER TABLE MODIFY QUERY for query changes
//
// The function returns an error if:
//   - No differences are found between current and target schemas (returns ErrNoDiff)
//   - An unsupported operation is detected (e.g., engine or cluster changes)
//   - Schema comparison fails for any object type
//
// Example:
//
//	currentSQL := `
//		CREATE DATABASE analytics ENGINE = Atomic COMMENT 'Old comment';
//		CREATE TABLE analytics.events (id UInt64, name String) ENGINE = MergeTree() ORDER BY id;
//	`
//	targetSQL := `
//		CREATE DATABASE analytics ENGINE = Atomic COMMENT 'New comment';
//		CREATE TABLE analytics.events (id UInt64, name String, timestamp DateTime) ENGINE = MergeTree() ORDER BY id;
//		CREATE DICTIONARY analytics.users_dict (id UInt64) PRIMARY KEY id SOURCE(HTTP(url 'test')) LAYOUT(FLAT()) LIFETIME(600);
//		CREATE VIEW analytics.daily_stats AS SELECT date, count() FROM events GROUP BY date;
//	`
//
//	current, _ := parser.ParseSQL(currentSQL)
//	target, _ := parser.ParseSQL(targetSQL)
//
//	migration, err := GenerateMigration(current, target, "update_analytics_schema")
//
//nolint:gocyclo,funlen,maintidx // Complex function handles multiple DDL statement types and migration ordering
func GenerateMigration(current, target *parser.SQL, name string) (*Migration, error) {
	// Compare databases and dictionaries to find differences
	dbDiffs, err := CompareDatabaseGrammars(current, target)
	if err != nil {
		return nil, errors.Wrap(err, "failed to compare database grammars")
	}

	dictDiffs, err := CompareDictionaryGrammars(current, target)
	if err != nil {
		return nil, errors.Wrap(err, "failed to compare dictionary grammars")
	}

	viewDiffs, err := CompareViewGrammars(current, target)
	if err != nil {
		return nil, errors.Wrap(err, "failed to compare view grammars")
	}

	tableDiffs, err := CompareTableGrammars(current, target)
	if err != nil {
		return nil, errors.Wrap(err, "failed to compare table grammars")
	}

	if len(dbDiffs) == 0 && len(dictDiffs) == 0 && len(viewDiffs) == 0 && len(tableDiffs) == 0 {
		return nil, ErrNoDiff
	}

	timestamp := time.Now()
	version := timestamp.Format("20060102150405")

	// Generate migration header
	up := fmt.Sprintf("-- Schema migration: %s\n", name)
	up += fmt.Sprintf("-- Generated at: %s\n\n", timestamp.Format("2006-01-02 15:04:05"))

	down := fmt.Sprintf("-- Schema rollback: %s\n", name)
	down += fmt.Sprintf("-- Generated at: %s\n\n", timestamp.Format("2006-01-02 15:04:05"))

	// Process diffs in proper order: databases first, then tables, then views, then dictionaries
	// Within each type: CREATE first, then ALTER/REPLACE, then DROP
	upStatements := make([]string, 0, 50)   // Pre-allocate with estimated capacity
	downStatements := make([]string, 0, 50) // Pre-allocate with estimated capacity

	// Group database diffs by type for proper ordering
	var dbCreateDiffs, dbAlterDiffs, dbRenameDiffs, dbDropDiffs []*DatabaseDiff
	for _, diff := range dbDiffs {
		switch diff.Type {
		case DatabaseDiffCreate:
			dbCreateDiffs = append(dbCreateDiffs, diff)
		case DatabaseDiffAlter:
			dbAlterDiffs = append(dbAlterDiffs, diff)
		case DatabaseDiffRename:
			dbRenameDiffs = append(dbRenameDiffs, diff)
		case DatabaseDiffDrop:
			dbDropDiffs = append(dbDropDiffs, diff)
		}
	}

	// Group dictionary diffs by type for proper ordering
	var dictCreateDiffs, dictReplaceDiffs, dictRenameDiffs, dictDropDiffs []*DictionaryDiff
	for _, diff := range dictDiffs {
		switch diff.Type {
		case DictionaryDiffCreate:
			dictCreateDiffs = append(dictCreateDiffs, diff)
		case DictionaryDiffReplace:
			dictReplaceDiffs = append(dictReplaceDiffs, diff)
		case DictionaryDiffRename:
			dictRenameDiffs = append(dictRenameDiffs, diff)
		case DictionaryDiffDrop:
			dictDropDiffs = append(dictDropDiffs, diff)
		}
	}

	// Group view diffs by type for proper ordering
	var viewCreateDiffs, viewAlterDiffs, viewRenameDiffs, viewDropDiffs []*ViewDiff
	for _, diff := range viewDiffs {
		switch diff.Type {
		case ViewDiffCreate:
			viewCreateDiffs = append(viewCreateDiffs, diff)
		case ViewDiffAlter:
			viewAlterDiffs = append(viewAlterDiffs, diff)
		case ViewDiffRename:
			viewRenameDiffs = append(viewRenameDiffs, diff)
		case ViewDiffDrop:
			viewDropDiffs = append(viewDropDiffs, diff)
		}
	}

	// Group table diffs by type for proper ordering
	var tableCreateDiffs, tableAlterDiffs, tableRenameDiffs, tableDropDiffs []*TableDiff
	for _, diff := range tableDiffs {
		switch diff.Type {
		case TableDiffCreate:
			tableCreateDiffs = append(tableCreateDiffs, diff)
		case TableDiffAlter:
			tableAlterDiffs = append(tableAlterDiffs, diff)
		case TableDiffRename:
			tableRenameDiffs = append(tableRenameDiffs, diff)
		case TableDiffDrop:
			tableDropDiffs = append(tableDropDiffs, diff)
		}
	}

	// UP migration: Databases first, then tables, then views, then dictionaries
	// Database order: CREATE -> ALTER -> RENAME -> DROP
	for _, diff := range dbCreateDiffs {
		upStatements = append(upStatements, "-- "+diff.Description)
		upStatements = append(upStatements, diff.UpSQL)
	}
	for _, diff := range dbAlterDiffs {
		upStatements = append(upStatements, "-- "+diff.Description)
		upStatements = append(upStatements, diff.UpSQL)
	}
	for _, diff := range dbRenameDiffs {
		upStatements = append(upStatements, "-- "+diff.Description)
		upStatements = append(upStatements, diff.UpSQL)
	}
	for _, diff := range dbDropDiffs {
		upStatements = append(upStatements, "-- "+diff.Description)
		upStatements = append(upStatements, diff.UpSQL)
	}

	// Dictionary order: CREATE -> REPLACE -> RENAME -> DROP
	for _, diff := range dictCreateDiffs {
		upStatements = append(upStatements, "-- "+diff.Description)
		upStatements = append(upStatements, diff.UpSQL)
	}
	for _, diff := range dictReplaceDiffs {
		upStatements = append(upStatements, "-- "+diff.Description)
		upStatements = append(upStatements, diff.UpSQL)
	}
	for _, diff := range dictRenameDiffs {
		upStatements = append(upStatements, "-- "+diff.Description)
		upStatements = append(upStatements, diff.UpSQL)
	}
	for _, diff := range dictDropDiffs {
		upStatements = append(upStatements, "-- "+diff.Description)
		upStatements = append(upStatements, diff.UpSQL)
	}

	// Table order: CREATE -> ALTER -> RENAME -> DROP
	for _, diff := range tableCreateDiffs {
		upStatements = append(upStatements, "-- "+diff.Description)
		upStatements = append(upStatements, diff.UpSQL)
	}
	for _, diff := range tableAlterDiffs {
		upStatements = append(upStatements, "-- "+diff.Description)
		upStatements = append(upStatements, diff.UpSQL)
	}
	for _, diff := range tableRenameDiffs {
		upStatements = append(upStatements, "-- "+diff.Description)
		upStatements = append(upStatements, diff.UpSQL)
	}
	for _, diff := range tableDropDiffs {
		upStatements = append(upStatements, "-- "+diff.Description)
		upStatements = append(upStatements, diff.UpSQL)
	}

	// View order: CREATE -> ALTER -> RENAME -> DROP
	for _, diff := range viewCreateDiffs {
		upStatements = append(upStatements, "-- "+diff.Description)
		upStatements = append(upStatements, diff.UpSQL)
	}
	for _, diff := range viewAlterDiffs {
		upStatements = append(upStatements, "-- "+diff.Description)
		upStatements = append(upStatements, diff.UpSQL)
	}
	for _, diff := range viewRenameDiffs {
		upStatements = append(upStatements, "-- "+diff.Description)
		upStatements = append(upStatements, diff.UpSQL)
	}
	for _, diff := range viewDropDiffs {
		upStatements = append(upStatements, "-- "+diff.Description)
		upStatements = append(upStatements, diff.UpSQL)
	}

	// DOWN migration: reverse order (views first, then tables, then dictionaries, then databases)
	// View order: DROP <- RENAME <- ALTER <- CREATE
	for i := len(viewDropDiffs) - 1; i >= 0; i-- {
		diff := viewDropDiffs[i]
		downStatements = append(downStatements, "-- Rollback: "+diff.Description)
		downStatements = append(downStatements, diff.DownSQL)
	}
	for i := len(viewRenameDiffs) - 1; i >= 0; i-- {
		diff := viewRenameDiffs[i]
		downStatements = append(downStatements, "-- Rollback: "+diff.Description)
		downStatements = append(downStatements, diff.DownSQL)
	}
	for i := len(viewAlterDiffs) - 1; i >= 0; i-- {
		diff := viewAlterDiffs[i]
		downStatements = append(downStatements, "-- Rollback: "+diff.Description)
		downStatements = append(downStatements, diff.DownSQL)
	}
	for i := len(viewCreateDiffs) - 1; i >= 0; i-- {
		diff := viewCreateDiffs[i]
		downStatements = append(downStatements, "-- Rollback: "+diff.Description)
		downStatements = append(downStatements, diff.DownSQL)
	}

	// Table order: DROP <- RENAME <- ALTER <- CREATE
	for i := len(tableDropDiffs) - 1; i >= 0; i-- {
		diff := tableDropDiffs[i]
		downStatements = append(downStatements, "-- Rollback: "+diff.Description)
		downStatements = append(downStatements, diff.DownSQL)
	}
	for i := len(tableRenameDiffs) - 1; i >= 0; i-- {
		diff := tableRenameDiffs[i]
		downStatements = append(downStatements, "-- Rollback: "+diff.Description)
		downStatements = append(downStatements, diff.DownSQL)
	}
	for i := len(tableAlterDiffs) - 1; i >= 0; i-- {
		diff := tableAlterDiffs[i]
		downStatements = append(downStatements, "-- Rollback: "+diff.Description)
		downStatements = append(downStatements, diff.DownSQL)
	}
	for i := len(tableCreateDiffs) - 1; i >= 0; i-- {
		diff := tableCreateDiffs[i]
		downStatements = append(downStatements, "-- Rollback: "+diff.Description)
		downStatements = append(downStatements, diff.DownSQL)
	}

	// Dictionary order: DROP <- RENAME <- REPLACE <- CREATE
	for i := len(dictDropDiffs) - 1; i >= 0; i-- {
		diff := dictDropDiffs[i]
		downStatements = append(downStatements, "-- Rollback: "+diff.Description)
		downStatements = append(downStatements, diff.DownSQL)
	}
	for i := len(dictRenameDiffs) - 1; i >= 0; i-- {
		diff := dictRenameDiffs[i]
		downStatements = append(downStatements, "-- Rollback: "+diff.Description)
		downStatements = append(downStatements, diff.DownSQL)
	}
	for i := len(dictReplaceDiffs) - 1; i >= 0; i-- {
		diff := dictReplaceDiffs[i]
		downStatements = append(downStatements, "-- Rollback: "+diff.Description)
		downStatements = append(downStatements, diff.DownSQL)
	}
	for i := len(dictCreateDiffs) - 1; i >= 0; i-- {
		diff := dictCreateDiffs[i]
		downStatements = append(downStatements, "-- Rollback: "+diff.Description)
		downStatements = append(downStatements, diff.DownSQL)
	}

	// Database order: DROP <- RENAME <- ALTER <- CREATE
	for i := len(dbDropDiffs) - 1; i >= 0; i-- {
		diff := dbDropDiffs[i]
		downStatements = append(downStatements, "-- Rollback: "+diff.Description)
		downStatements = append(downStatements, diff.DownSQL)
	}
	for i := len(dbRenameDiffs) - 1; i >= 0; i-- {
		diff := dbRenameDiffs[i]
		downStatements = append(downStatements, "-- Rollback: "+diff.Description)
		downStatements = append(downStatements, diff.DownSQL)
	}
	for i := len(dbAlterDiffs) - 1; i >= 0; i-- {
		diff := dbAlterDiffs[i]
		downStatements = append(downStatements, "-- Rollback: "+diff.Description)
		downStatements = append(downStatements, diff.DownSQL)
	}
	for i := len(dbCreateDiffs) - 1; i >= 0; i-- {
		diff := dbCreateDiffs[i]
		downStatements = append(downStatements, "-- Rollback: "+diff.Description)
		downStatements = append(downStatements, diff.DownSQL)
	}

	up += strings.Join(upStatements, "\n\n")
	down += strings.Join(downStatements, "\n\n")

	return &Migration{
		Version:   version,
		Name:      name,
		Up:        up,
		Down:      down,
		Timestamp: timestamp,
	}, nil
}

// GenerateMigrationFile creates a timestamped migration file by comparing current and target schemas.
// The migration file is named using UTC timestamp in yyyyMMddhhmmss format and written to the specified directory.
// Returns the generated filename and any error encountered.
//
// Down migrations can be generated by swapping current and target parameters.
//
// Parameters:
//   - migrationDir: Directory where the migration file should be written
//   - current: Current schema state
//   - target: Target schema state
//
// Returns:
//   - filename: The generated migration filename (e.g., "20240806143022_schema_update.sql")
//   - error: Any error encountered during generation or file writing (returns ErrNoDiff if no differences found)
//
// Example:
//
//	filename, err := GenerateMigrationFile("/path/to/migrations", currentSchema, targetSchema)
//	// Creates: /path/to/migrations/20240806143022_schema_update.sql
//
//	// For down migration:
//	downFilename, err := GenerateMigrationFile("/path/to/migrations", targetSchema, currentSchema)
func GenerateMigrationFile(migrationDir string, current, target *parser.SQL) (string, error) {
	// Generate migration using existing function
	migration, err := GenerateMigration(current, target, "schema_update")
	if err != nil {
		return "", errors.Wrap(err, "failed to generate migration")
	}

	// Create timestamped filename using UTC
	now := time.Now().UTC()
	timestamp := now.Format("20060102150405")
	filename := timestamp + "_schema_update.sql"

	// Ensure migration directory exists
	if err := os.MkdirAll(migrationDir, 0o755); err != nil {
		return "", errors.Wrapf(err, "failed to create migration directory: %s", migrationDir)
	}

	// Write migration SQL to file (only the UP migration for simplicity)
	content := fmt.Sprintf("-- Schema migration generated at %s UTC\n", now.Format("2006-01-02 15:04:05"))
	content += "-- Down migration: swap current and target schemas and regenerate\n\n"
	content += migration.Up

	migrationPath := filepath.Join(migrationDir, filename)
	err = os.WriteFile(migrationPath, []byte(content), os.FileMode(0o644))
	if err != nil {
		return "", errors.Wrapf(err, "failed to write migration file: %s", migrationPath)
	}

	return filename, nil
}
