package clickhouse

import (
	"regexp"
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

// cleanCreateStatement normalizes a CREATE statement by removing extra whitespace, ensuring semicolon,
// and normalizing data types for consistent comparison with parsed DDL
func cleanCreateStatement(createQuery string) string {
	cleaned := strings.TrimSpace(createQuery)
	if !strings.HasSuffix(cleaned, ";") {
		cleaned += ";"
	}

	// Normalize data types to match what the parser produces
	cleaned = normalizeDataTypesInDDL(cleaned)

	return cleaned
}

// normalizeDataTypesInDDL normalizes data types in DDL statements to match parser output
func normalizeDataTypesInDDL(ddl string) string {
	// Normalize Decimal(18, X) -> Decimal64(X)
	decimalPattern := regexp.MustCompile(`Decimal\(18,\s*(\d+)\)`)
	ddl = decimalPattern.ReplaceAllString(ddl, "Decimal64($1)")
	
	// Normalize DateTime64(X, 'TZ') -> DateTime(X, 'TZ')
	datetimePattern := regexp.MustCompile(`DateTime64\((\d+),\s*'([^']+)'\)`)
	ddl = datetimePattern.ReplaceAllString(ddl, "DateTime($1, '$2')")
	
	return ddl
}

// validateDDLStatement ensures the generated DDL statement is valid by parsing it
func validateDDLStatement(ddl string) error {
	_, err := parser.ParseString(ddl)
	return err
}
