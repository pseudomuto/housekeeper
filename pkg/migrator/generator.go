package migrator

import (
	"fmt"
	"strings"
	"time"

	"github.com/pseudomuto/housekeeper/pkg/parser"
)

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

// GenerateMigration creates a migration by comparing current and target schema states.
// It analyzes the differences between the current schema and the desired target schema,
// then generates appropriate DDL statements for both applying (UP) and rolling back (DOWN) the changes.
//
// The migration includes both database and dictionary changes, processing them in the correct order:
// - Databases are processed first (since dictionaries may depend on them)
// - Within each type: CREATE -> ALTER/REPLACE -> DROP
//
// The function returns an error if:
//   - No differences are found between current and target schemas
//   - An unsupported operation is detected (e.g., database engine or cluster changes)
//   - Schema comparison fails
//
// Example:
//
//	currentSQL := `CREATE DATABASE db1 ENGINE = Atomic COMMENT 'Old comment';`
//	targetSQL := `CREATE DATABASE db1 ENGINE = Atomic COMMENT 'New comment';
//	              CREATE DICTIONARY dict1 (id UInt64) PRIMARY KEY id SOURCE(HTTP(url 'test')) LAYOUT(FLAT()) LIFETIME(600);`
//	
//	current, _ := parser.ParseSQL(currentSQL)
//	target, _ := parser.ParseSQL(targetSQL)
//	
//	migration, err := GenerateMigration(current, target, "update_schema")
func GenerateMigration(current, target *parser.Grammar, name string) (*Migration, error) {
	// Compare databases and dictionaries to find differences
	dbDiffs, err := CompareDatabaseGrammars(current, target)
	if err != nil {
		return nil, fmt.Errorf("failed to compare database grammars: %w", err)
	}

	dictDiffs, err := CompareDictionaryGrammars(current, target)
	if err != nil {
		return nil, fmt.Errorf("failed to compare dictionary grammars: %w", err)
	}

	viewDiffs, err := CompareViewGrammars(current, target)
	if err != nil {
		return nil, fmt.Errorf("failed to compare view grammars: %w", err)
	}

	if len(dbDiffs) == 0 && len(dictDiffs) == 0 && len(viewDiffs) == 0 {
		return nil, fmt.Errorf("no differences found")
	}

	timestamp := time.Now()
	version := timestamp.Format("20060102150405")

	// Generate migration header
	up := fmt.Sprintf("-- Schema migration: %s\n", name)
	up += fmt.Sprintf("-- Generated at: %s\n\n", timestamp.Format("2006-01-02 15:04:05"))

	down := fmt.Sprintf("-- Schema rollback: %s\n", name)
	down += fmt.Sprintf("-- Generated at: %s\n\n", timestamp.Format("2006-01-02 15:04:05"))

	// Process diffs in proper order: databases first, then dictionaries, then views
	// Within each type: CREATE first, then ALTER/REPLACE, then DROP
	var upStatements []string
	var downStatements []string

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

	// UP migration: Databases first, then dictionaries, then views
	// Database order: CREATE -> ALTER -> RENAME -> DROP
	for _, diff := range dbCreateDiffs {
		upStatements = append(upStatements, fmt.Sprintf("-- %s", diff.Description))
		upStatements = append(upStatements, diff.UpSQL)
	}
	for _, diff := range dbAlterDiffs {
		upStatements = append(upStatements, fmt.Sprintf("-- %s", diff.Description))
		upStatements = append(upStatements, diff.UpSQL)
	}
	for _, diff := range dbRenameDiffs {
		upStatements = append(upStatements, fmt.Sprintf("-- %s", diff.Description))
		upStatements = append(upStatements, diff.UpSQL)
	}
	for _, diff := range dbDropDiffs {
		upStatements = append(upStatements, fmt.Sprintf("-- %s", diff.Description))
		upStatements = append(upStatements, diff.UpSQL)
	}

	// Dictionary order: CREATE -> REPLACE -> RENAME -> DROP
	for _, diff := range dictCreateDiffs {
		upStatements = append(upStatements, fmt.Sprintf("-- %s", diff.Description))
		upStatements = append(upStatements, diff.UpSQL)
	}
	for _, diff := range dictReplaceDiffs {
		upStatements = append(upStatements, fmt.Sprintf("-- %s", diff.Description))
		upStatements = append(upStatements, diff.UpSQL)
	}
	for _, diff := range dictRenameDiffs {
		upStatements = append(upStatements, fmt.Sprintf("-- %s", diff.Description))
		upStatements = append(upStatements, diff.UpSQL)
	}
	for _, diff := range dictDropDiffs {
		upStatements = append(upStatements, fmt.Sprintf("-- %s", diff.Description))
		upStatements = append(upStatements, diff.UpSQL)
	}

	// View order: CREATE -> ALTER -> RENAME -> DROP
	for _, diff := range viewCreateDiffs {
		upStatements = append(upStatements, fmt.Sprintf("-- %s", diff.Description))
		upStatements = append(upStatements, diff.UpSQL)
	}
	for _, diff := range viewAlterDiffs {
		upStatements = append(upStatements, fmt.Sprintf("-- %s", diff.Description))
		upStatements = append(upStatements, diff.UpSQL)
	}
	for _, diff := range viewRenameDiffs {
		upStatements = append(upStatements, fmt.Sprintf("-- %s", diff.Description))
		upStatements = append(upStatements, diff.UpSQL)
	}
	for _, diff := range viewDropDiffs {
		upStatements = append(upStatements, fmt.Sprintf("-- %s", diff.Description))
		upStatements = append(upStatements, diff.UpSQL)
	}

	// DOWN migration: reverse order (views first, then dictionaries, then databases)
	// View order: DROP <- RENAME <- ALTER <- CREATE
	for i := len(viewDropDiffs) - 1; i >= 0; i-- {
		diff := viewDropDiffs[i]
		downStatements = append(downStatements, fmt.Sprintf("-- Rollback: %s", diff.Description))
		downStatements = append(downStatements, diff.DownSQL)
	}
	for i := len(viewRenameDiffs) - 1; i >= 0; i-- {
		diff := viewRenameDiffs[i]
		downStatements = append(downStatements, fmt.Sprintf("-- Rollback: %s", diff.Description))
		downStatements = append(downStatements, diff.DownSQL)
	}
	for i := len(viewAlterDiffs) - 1; i >= 0; i-- {
		diff := viewAlterDiffs[i]
		downStatements = append(downStatements, fmt.Sprintf("-- Rollback: %s", diff.Description))
		downStatements = append(downStatements, diff.DownSQL)
	}
	for i := len(viewCreateDiffs) - 1; i >= 0; i-- {
		diff := viewCreateDiffs[i]
		downStatements = append(downStatements, fmt.Sprintf("-- Rollback: %s", diff.Description))
		downStatements = append(downStatements, diff.DownSQL)
	}

	// Dictionary order: DROP <- RENAME <- REPLACE <- CREATE
	for i := len(dictDropDiffs) - 1; i >= 0; i-- {
		diff := dictDropDiffs[i]
		downStatements = append(downStatements, fmt.Sprintf("-- Rollback: %s", diff.Description))
		downStatements = append(downStatements, diff.DownSQL)
	}
	for i := len(dictRenameDiffs) - 1; i >= 0; i-- {
		diff := dictRenameDiffs[i]
		downStatements = append(downStatements, fmt.Sprintf("-- Rollback: %s", diff.Description))
		downStatements = append(downStatements, diff.DownSQL)
	}
	for i := len(dictReplaceDiffs) - 1; i >= 0; i-- {
		diff := dictReplaceDiffs[i]
		downStatements = append(downStatements, fmt.Sprintf("-- Rollback: %s", diff.Description))
		downStatements = append(downStatements, diff.DownSQL)
	}
	for i := len(dictCreateDiffs) - 1; i >= 0; i-- {
		diff := dictCreateDiffs[i]
		downStatements = append(downStatements, fmt.Sprintf("-- Rollback: %s", diff.Description))
		downStatements = append(downStatements, diff.DownSQL)
	}

	// Database order: DROP <- RENAME <- ALTER <- CREATE
	for i := len(dbDropDiffs) - 1; i >= 0; i-- {
		diff := dbDropDiffs[i]
		downStatements = append(downStatements, fmt.Sprintf("-- Rollback: %s", diff.Description))
		downStatements = append(downStatements, diff.DownSQL)
	}
	for i := len(dbRenameDiffs) - 1; i >= 0; i-- {
		diff := dbRenameDiffs[i]
		downStatements = append(downStatements, fmt.Sprintf("-- Rollback: %s", diff.Description))
		downStatements = append(downStatements, diff.DownSQL)
	}
	for i := len(dbAlterDiffs) - 1; i >= 0; i-- {
		diff := dbAlterDiffs[i]
		downStatements = append(downStatements, fmt.Sprintf("-- Rollback: %s", diff.Description))
		downStatements = append(downStatements, diff.DownSQL)
	}
	for i := len(dbCreateDiffs) - 1; i >= 0; i-- {
		diff := dbCreateDiffs[i]
		downStatements = append(downStatements, fmt.Sprintf("-- Rollback: %s", diff.Description))
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


