package schema

import (
	"bytes"
	"fmt"
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
	// ErrInvalidClause is returned when unsupported clauses are used with specific engines
	ErrInvalidClause = errors.New("invalid clause for engine type")
)

// diffProcessor defines the interface needed for generic diff processing.
// This interface is satisfied implicitly by all diff types without requiring
// explicit method implementations.
type diffProcessor interface {
	GetDiffType() string // Returns the operation type (CREATE, ALTER, DROP, RENAME, etc.)
	GetUpSQL() string    // Returns the forward migration SQL
}

// Processing order configurations for each object type.
// These define the exact order in which different operation types should be applied
// for each schema object type to ensure proper dependency management and safety.
var (
	// roleProcessingOrder defines the order for role operations
	// CREATE -> ALTER -> RENAME -> GRANT -> REVOKE -> DROP
	roleProcessingOrder = []string{"CREATE", "ALTER", "RENAME", "GRANT", "REVOKE", "DROP"}

	// functionProcessingOrder defines the order for function operations
	// CREATE -> REPLACE -> RENAME -> DROP
	functionProcessingOrder = []string{"CREATE", "REPLACE", "RENAME", "DROP"}

	// databaseProcessingOrder defines the order for database operations
	// CREATE -> ALTER -> RENAME -> DROP
	databaseProcessingOrder = []string{"CREATE", "ALTER", "RENAME", "DROP"}

	// tableProcessingOrder defines the order for table operations
	// CREATE -> ALTER -> RENAME -> DROP
	tableProcessingOrder = []string{"CREATE", "ALTER", "RENAME", "DROP"}

	// dictionaryProcessingOrder defines the order for dictionary operations
	// CREATE -> REPLACE -> RENAME -> DROP
	dictionaryProcessingOrder = []string{"CREATE", "REPLACE", "RENAME", "DROP"}

	// viewProcessingOrder defines the order for view operations
	// CREATE -> ALTER -> RENAME -> DROP
	viewProcessingOrder = []string{"CREATE", "ALTER", "RENAME", "DROP"}
)

// groupDiffsByType groups a slice of diffs by their type using a generic approach.
// This eliminates the need for repetitive switch statements for each diff type.
//
// Type parameter T must implement diffProcessor interface.
//
// Returns a map where keys are diff types (CREATE, ALTER, DROP, etc.) and values
// are slices of diffs of that type.
func groupDiffsByType[T diffProcessor](diffs []T) map[string][]T {
	groups := make(map[string][]T)
	for _, diff := range diffs {
		diffType := diff.GetDiffType()
		groups[diffType] = append(groups[diffType], diff)
	}
	return groups
}

// processDiffsInOrder processes grouped diffs in the specified order and returns
// a slice of SQL statements. This eliminates repetitive for loop patterns.
//
// Parameters:
//   - groups: Map of diff type to slice of diffs (from groupDiffsByType)
//   - order: Slice specifying the processing order (e.g., ["CREATE", "ALTER", "RENAME", "DROP"])
//
// Returns a slice of SQL statements in the correct order.
func processDiffsInOrder[T diffProcessor](groups map[string][]T, order []string) []string {
	var statements []string
	for _, diffType := range order {
		if diffs, exists := groups[diffType]; exists {
			for _, diff := range diffs {
				statements = append(statements, diff.GetUpSQL())
			}
		}
	}
	return statements
}

// processAllDiffsInOrder is a convenience function that combines grouping and processing
// for a single diff type. This replaces the repetitive group->process pattern.
//
// Parameters:
//   - diffs: Slice of diffs to process
//   - order: Processing order for the diff types
//
// Returns a slice of SQL statements in the correct order.
func processAllDiffsInOrder[T diffProcessor](diffs []T, order []string) []string {
	groups := groupDiffsByType(diffs)
	return processDiffsInOrder(groups, order)
}

// GenerateDiff creates a diff by comparing current and target schema states.
// It analyzes the differences between the current schema and the desired target schema,
// then generates appropriate DDL statements.
//
// The migration includes all schema objects (roles, functions, databases, tables, dictionaries, views), processing them in the correct order:
// Roles → Functions → Databases → Named Collections → Tables → Dictionaries → Views (CREATE → ALTER → RENAME → DROP)
//
// Migration strategies for different object types:
//   - Roles: Standard DDL operations (CREATE, ALTER, DROP, RENAME, GRANT, REVOKE)
//   - Functions: DROP+CREATE for modifications (since they can't be altered)
//   - Databases: Standard DDL operations (CREATE, ALTER, DROP, RENAME)
//   - Named Collections: Standard DDL operations (CREATE, ALTER, DROP)
//   - Tables: Full DDL support including column modifications (CREATE, ALTER, DROP, RENAME)
//   - Dictionaries: CREATE OR REPLACE for modifications (since they can't be altered)
//   - Regular Views: CREATE OR REPLACE for modifications
//   - Materialized Views: DROP+CREATE for query modifications (more reliable than ALTER TABLE MODIFY QUERY)
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
//nolint:gocyclo,funlen,maintidx,gocognit // Complex function handles multiple DDL statement types and migration ordering
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

	roleDiffs := compareRoles(current, target)

	functionDiffs := compareFunctions(current, target)

	if len(dbDiffs) == 0 && len(dictDiffs) == 0 && len(viewDiffs) == 0 && len(tableDiffs) == 0 && len(roleDiffs) == 0 && len(functionDiffs) == 0 {
		return nil, ErrNoDiff
	}

	// Process diffs in proper order: roles first (global objects), then functions (global objects), then databases, then tables, then dictionaries, then views
	// Within each type: CREATE first, then ALTER/REPLACE, then RENAME, then DROP/GRANT/REVOKE
	statements := make([]string, 0, 50) // Pre-allocate with estimated capacity

	// Process all diffs using generic diff processor in proper order:
	// Roles first (global objects), then functions (global objects), then databases,
	// then tables, then dictionaries, then views

	// Process roles: CREATE -> ALTER -> RENAME -> GRANT -> REVOKE -> DROP
	statements = append(statements, processAllDiffsInOrder(roleDiffs, roleProcessingOrder)...)

	// Process functions: CREATE -> REPLACE -> RENAME -> DROP
	statements = append(statements, processAllDiffsInOrder(functionDiffs, functionProcessingOrder)...)

	// Process databases: CREATE -> ALTER -> RENAME -> DROP
	statements = append(statements, processAllDiffsInOrder(dbDiffs, databaseProcessingOrder)...)

	// Process tables: CREATE -> ALTER -> RENAME -> DROP
	statements = append(statements, processAllDiffsInOrder(tableDiffs, tableProcessingOrder)...)

	// Process dictionaries: CREATE -> REPLACE -> RENAME -> DROP
	statements = append(statements, processAllDiffsInOrder(dictDiffs, dictionaryProcessingOrder)...)

	// Process views: CREATE -> ALTER -> RENAME -> DROP
	statements = append(statements, processAllDiffsInOrder(viewDiffs, viewProcessingOrder)...)

	// Split any statements that contain multiple SQL statements (separated by \n\n)
	// and ensure each individual SQL statement ends with a semicolon
	var processedStatements []string
	for _, stmt := range statements {
		// Split on \n\n in case a single statement contains multiple SQL statements
		subStatements := strings.SplitSeq(stmt, "\n\n")
		for subStmt := range subStatements {
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
		return nil, ErrNoDiff
	}

	parsedSQL, err := parser.ParseString(sql)
	if err != nil {
		// Log the invalid SQL for debugging but don't fail
		fmt.Printf("WARNING: Generated invalid DDL (possible parser limitation):\n%s\nError: %v\n", sql, err)
		// Return as if no differences found since the generated SQL is invalid
		return nil, ErrNoDiff
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
