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

// GenerateDatabaseMigration creates a database migration by comparing current and target database states.
// It analyzes the differences between the current database schema and the desired target schema,
// then generates appropriate DDL statements for both applying (UP) and rolling back (DOWN) the changes.
//
// The function returns an error if:
//   - No differences are found between current and target schemas
//   - An unsupported operation is detected (e.g., engine or cluster changes)
//   - Database comparison fails
//
// Example:
//
//	currentSQL := `CREATE DATABASE db1 ENGINE = Atomic COMMENT 'Old comment';`
//	targetSQL := `CREATE DATABASE db1 ENGINE = Atomic COMMENT 'New comment';`
//	
//	current, _ := parser.ParseSQL(currentSQL)
//	target, _ := parser.ParseSQL(targetSQL)
//	
//	migration, err := GenerateDatabaseMigration(current, target, "update_comment")
//	if err != nil {
//	    log.Fatal(err)
//	}
//	
//	fmt.Println(migration.Up)   // ALTER DATABASE db1 MODIFY COMMENT 'New comment';
//	fmt.Println(migration.Down) // ALTER DATABASE db1 MODIFY COMMENT 'Old comment';
func GenerateDatabaseMigration(current, target *parser.Grammar, name string) (*Migration, error) {
	// Compare the grammars to find differences
	diffs, err := CompareDatabaseGrammars(current, target)
	if err != nil {
		return nil, fmt.Errorf("failed to compare database grammars: %w", err)
	}

	if len(diffs) == 0 {
		return nil, fmt.Errorf("no differences found")
	}

	timestamp := time.Now()
	version := timestamp.Format("20060102150405")

	// Generate migration header
	up := fmt.Sprintf("-- Database migration: %s\n", name)
	up += fmt.Sprintf("-- Generated at: %s\n\n", timestamp.Format("2006-01-02 15:04:05"))

	down := fmt.Sprintf("-- Database rollback: %s\n", name)
	down += fmt.Sprintf("-- Generated at: %s\n\n", timestamp.Format("2006-01-02 15:04:05"))

	// Process diffs in order: CREATE first, then ALTER, then DROP
	var upStatements []string
	var downStatements []string

	// Group diffs by type for proper ordering
	var createDiffs, alterDiffs, dropDiffs []*DatabaseDiff
	for _, diff := range diffs {
		switch diff.Type {
		case DatabaseDiffCreate:
			createDiffs = append(createDiffs, diff)
		case DatabaseDiffAlter:
			alterDiffs = append(alterDiffs, diff)
		case DatabaseDiffDrop:
			dropDiffs = append(dropDiffs, diff)
		}
	}

	// UP migration: CREATE -> ALTER -> DROP
	for _, diff := range createDiffs {
		upStatements = append(upStatements, fmt.Sprintf("-- %s", diff.Description))
		upStatements = append(upStatements, diff.UpSQL)
	}
	for _, diff := range alterDiffs {
		upStatements = append(upStatements, fmt.Sprintf("-- %s", diff.Description))
		upStatements = append(upStatements, diff.UpSQL)
	}
	for _, diff := range dropDiffs {
		upStatements = append(upStatements, fmt.Sprintf("-- %s", diff.Description))
		upStatements = append(upStatements, diff.UpSQL)
	}

	// DOWN migration: reverse order (CREATE <- ALTER <- DROP)
	for i := len(dropDiffs) - 1; i >= 0; i-- {
		diff := dropDiffs[i]
		downStatements = append(downStatements, fmt.Sprintf("-- Rollback: %s", diff.Description))
		downStatements = append(downStatements, diff.DownSQL)
	}
	for i := len(alterDiffs) - 1; i >= 0; i-- {
		diff := alterDiffs[i]
		downStatements = append(downStatements, fmt.Sprintf("-- Rollback: %s", diff.Description))
		downStatements = append(downStatements, diff.DownSQL)
	}
	for i := len(createDiffs) - 1; i >= 0; i-- {
		diff := createDiffs[i]
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

