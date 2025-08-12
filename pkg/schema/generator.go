package schema

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/pkg/errors"
	"github.com/pseudomuto/housekeeper/pkg/consts"
	"github.com/pseudomuto/housekeeper/pkg/format"
	"github.com/pseudomuto/housekeeper/pkg/parser"
)

// ErrNoDiff is returned when no differences are found between current and target schemas
var ErrNoDiff = errors.New("no differences found")

// Invalid migration operation errors
var (
	// ErrUnsupported is returned for operations that are fundamentally unsupported by ClickHouse
	ErrUnsupported = errors.New("unsupported operation")
	// ErrDictionaryAlter is returned when attempting to use ALTER operations on dictionaries
	ErrDictionaryAlter = errors.New("dictionary ALTER operations not supported")
	// ErrClusterChange is returned when attempting to change cluster configuration
	ErrClusterChange = errors.New("cluster configuration changes not supported")
	// ErrEngineChange is returned when attempting to change engine types
	ErrEngineChange = errors.New("engine type changes not supported")
	// ErrSystemObject is returned when attempting to modify system objects
	ErrSystemObject = errors.New("system object modifications not supported")
	// ErrInvalidType is returned for invalid type combinations
	ErrInvalidType = errors.New("invalid type combination")
)

// GenerateDiff creates a diff by comparing current and target schema states.
// It analyzes the differences between the current schema and the desired target schema,
// then generates appropriate DDL statements.
//
// The migration includes all schema objects (databases, tables, dictionaries, views), processing them in the correct order:
// Databases → Tables → Dictionaries → Views (CREATE → ALTER → RENAME → DROP)
//
// Migration strategies for different object types:
//   - Databases: Standard DDL operations (CREATE, ALTER, DROP, RENAME)
//   - Tables: Full DDL support including column modifications (CREATE, ALTER, DROP, RENAME)
//   - Dictionaries: CREATE OR REPLACE for modifications (since they can't be altered)
//   - Regular Views: CREATE OR REPLACE for modifications
//   - Materialized Views: ALTER TABLE MODIFY QUERY for query changes
//
// The function returns a *parser.SQL containing the migration statements, or an error if:
//   - No differences are found between current and target schemas (returns ErrNoDiff)
//   - An unsupported operation is detected (e.g., engine or cluster changes)
//   - Schema comparison fails for any object type
//   - Generated SQL cannot be parsed back into statements
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
//	current, _ := parser.ParseString(currentSQL)
//	target, _ := parser.ParseString(targetSQL)
//
//	diff, err := GenerateDiff(current, target)
//	if err != nil {
//		log.Fatal(err)
//	}
//
//	// Format the migration SQL for output
//	var buf bytes.Buffer
//	format.FormatSQL(&buf, format.Defaults, diff)
//	fmt.Println(buf.String())
//
//nolint:gocyclo,funlen,maintidx // Complex function handles multiple DDL statement types and migration ordering
func GenerateDiff(current, target *parser.SQL) (*parser.SQL, error) {
	// Compare databases and dictionaries to find differences
	dbDiffs, err := compareDatabases(current, target)
	if err != nil {
		return nil, errors.Wrap(err, "failed to compare databases")
	}

	dictDiffs, err := compareDictionaries(current, target)
	if err != nil {
		return nil, errors.Wrap(err, "failed to compare dictionaries")
	}

	viewDiffs, err := compareViews(current, target)
	if err != nil {
		return nil, errors.Wrap(err, "failed to compare views")
	}

	tableDiffs, err := compareTables(current, target)
	if err != nil {
		return nil, errors.Wrap(err, "failed to compare tables")
	}

	if len(dbDiffs) == 0 && len(dictDiffs) == 0 && len(viewDiffs) == 0 && len(tableDiffs) == 0 {
		return nil, ErrNoDiff
	}

	// Process diffs in proper order: databases first, then tables, then dictionaries, then views
	// Within each type: CREATE first, then ALTER/REPLACE, then RENAME, then DROP
	statements := make([]string, 0, 50) // Pre-allocate with estimated capacity

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

	// Migration order: Databases first, then tables, then dictionaries, then views
	// Database order: CREATE -> ALTER -> RENAME -> DROP
	for _, diff := range dbCreateDiffs {
		statements = append(statements, diff.UpSQL)
	}
	for _, diff := range dbAlterDiffs {
		statements = append(statements, diff.UpSQL)
	}
	for _, diff := range dbRenameDiffs {
		statements = append(statements, diff.UpSQL)
	}
	for _, diff := range dbDropDiffs {
		statements = append(statements, diff.UpSQL)
	}

	// Table order: CREATE -> ALTER -> RENAME -> DROP
	for _, diff := range tableCreateDiffs {
		statements = append(statements, diff.UpSQL)
	}
	for _, diff := range tableAlterDiffs {
		statements = append(statements, diff.UpSQL)
	}
	for _, diff := range tableRenameDiffs {
		statements = append(statements, diff.UpSQL)
	}
	for _, diff := range tableDropDiffs {
		statements = append(statements, diff.UpSQL)
	}

	// Dictionary order: CREATE -> REPLACE -> RENAME -> DROP
	for _, diff := range dictCreateDiffs {
		statements = append(statements, diff.UpSQL)
	}
	for _, diff := range dictReplaceDiffs {
		statements = append(statements, diff.UpSQL)
	}
	for _, diff := range dictRenameDiffs {
		statements = append(statements, diff.UpSQL)
	}
	for _, diff := range dictDropDiffs {
		statements = append(statements, diff.UpSQL)
	}

	// View order: CREATE -> ALTER -> RENAME -> DROP
	for _, diff := range viewCreateDiffs {
		statements = append(statements, diff.UpSQL)
	}
	for _, diff := range viewAlterDiffs {
		statements = append(statements, diff.UpSQL)
	}
	for _, diff := range viewRenameDiffs {
		statements = append(statements, diff.UpSQL)
	}
	for _, diff := range viewDropDiffs {
		statements = append(statements, diff.UpSQL)
	}

	// Split any statements that contain multiple SQL statements (separated by \n\n)
	// and ensure each individual SQL statement ends with a semicolon
	var processedStatements []string
	for _, stmt := range statements {
		// Split on \n\n in case a single statement contains multiple SQL statements
		subStatements := strings.Split(stmt, "\n\n")
		for _, subStmt := range subStatements {
			subStmt = strings.TrimSpace(subStmt)
			if subStmt != "" {
				if !strings.HasSuffix(subStmt, ";") {
					subStmt = subStmt + ";"
				}
				processedStatements = append(processedStatements, subStmt)
			}
		}
	}

	sql := strings.Join(processedStatements, "\n\n")

	// Parse the generated SQL back into *parser.SQL
	if strings.TrimSpace(sql) == "" {
		return &parser.SQL{Statements: []*parser.Statement{}}, nil
	}

	parsedSQL, err := parser.ParseString(sql)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to parse generated migration SQL")
	}

	return parsedSQL, nil
}

// GenerateMigrationFile creates a timestamped migration file by comparing current and target schemas.
// The migration file is named using UTC timestamp in yyyyMMddhhmmss format and written to the specified directory.
// Returns the generated filename and any error encountered.
//
// Parameters:
//   - migrationDir: Directory where the migration file should be written
//   - current: Current schema state
//   - target: Target schema state
//
// Returns:
//   - filename: The generated migration filename (e.g., "20240806143022.sql")
//   - error: Any error encountered during generation or file writing (returns ErrNoDiff if no differences found)
//
// Example:
//
//	filename, err := GenerateMigrationFile("/path/to/migrations", currentSchema, targetSchema)
//	// Creates: /path/to/migrations/20240806143022.sql
func GenerateMigrationFile(migrationDir string, current, target *parser.SQL) (string, error) {
	// Generate diff using existing function
	diff, err := GenerateDiff(current, target)
	if err != nil {
		return "", errors.Wrap(err, "failed to generate migration")
	}

	// Create timestamped filename using UTC
	filename := time.Now().UTC().Format("20060102150405") + ".sql"

	// Ensure migration directory exists
	if err := os.MkdirAll(migrationDir, consts.ModeDir); err != nil {
		return "", errors.Wrapf(err, "failed to create migration directory: %s", migrationDir)
	}

	// Format the generated SQL using the formatter
	var buf bytes.Buffer
	err = format.FormatSQL(&buf, format.Defaults, diff)
	if err != nil {
		return "", errors.Wrap(err, "failed to format migration SQL")
	}

	content := buf.String()

	migrationPath := filepath.Join(migrationDir, filename)
	err = os.WriteFile(migrationPath, []byte(content), consts.ModeFile)
	if err != nil {
		return "", errors.Wrapf(err, "failed to write migration file: %s", migrationPath)
	}

	return filename, nil
}
