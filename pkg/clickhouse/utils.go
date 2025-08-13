package clickhouse

import (
	"strings"

	"github.com/pseudomuto/housekeeper/pkg/parser"
)

// systemDatabases contains the list of ClickHouse system databases that should be excluded
// from schema extraction operations. These databases are managed by ClickHouse internally
// and should not be included in user schema operations.
var systemDatabases = []string{
	"default",
	"system",
	"information_schema",
	"INFORMATION_SCHEMA",
}

// buildSystemDatabaseExclusion creates a SQL "NOT IN" clause for excluding system databases
// using parameterized queries to prevent SQL injection. Returns the SQL condition and the
// parameters to use with the query.
// The columnName parameter specifies which column to check (e.g., "database", "name").
func buildSystemDatabaseExclusion(columnName string) (string, []any) {
	// Create placeholders for each system database
	placeholders := make([]string, len(systemDatabases))
	params := make([]any, len(systemDatabases))

	for i, db := range systemDatabases {
		placeholders[i] = "?"
		params[i] = db
	}

	condition := columnName + " NOT IN (" + strings.Join(placeholders, ", ") + ")"
	return condition, params
}

// cleanCreateStatement normalizes a CREATE statement by removing extra whitespace and ensuring semicolon
func cleanCreateStatement(createQuery string) string {
	cleaned := strings.TrimSpace(createQuery)
	if !strings.HasSuffix(cleaned, ";") {
		cleaned += ";"
	}

	return cleaned
}

// validateDDLStatement ensures the generated DDL statement is valid by parsing it
func validateDDLStatement(ddl string) error {
	_, err := parser.ParseString(ddl)
	return err
}
