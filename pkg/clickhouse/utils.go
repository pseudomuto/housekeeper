package clickhouse

import (
	"regexp"
	"strings"

	"github.com/pseudomuto/housekeeper/pkg/format"
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
	"housekeeper",
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

// buildDatabaseExclusion creates a SQL "NOT IN" clause for excluding both system databases
// and user-specified ignored databases. Returns the SQL condition and the parameters to use
// with the query. The columnName parameter specifies which column to check (e.g., "database", "name").
func buildDatabaseExclusion(columnName string, ignoreDatabases []string) (string, []any) {
	// Combine system databases with user-specified ignored databases
	allExcluded := append([]string{}, systemDatabases...)
	allExcluded = append(allExcluded, ignoreDatabases...)

	// If no databases to exclude, return a trivial condition
	if len(allExcluded) == 0 {
		return "1=1", []any{}
	}

	// Create placeholders for all excluded databases
	placeholders := make([]string, len(allExcluded))
	params := make([]any, len(allExcluded))

	for i, db := range allExcluded {
		placeholders[i] = "?"
		params[i] = db
	}

	condition := columnName + " NOT IN (" + strings.Join(placeholders, ", ") + ")"
	return condition, params
}

// cleanCreateStatement normalizes a CREATE statement using AST-based approach
// This parses the DDL and reformats it to ensure consistency, avoiding fragile string manipulation
func cleanCreateStatement(createQuery string) string {
	cleaned := strings.TrimSpace(createQuery)
	if !strings.HasSuffix(cleaned, ";") {
		cleaned += ";"
	}

	// Only do essential security normalization (password hiding)
	cleaned = normalizeDataTypesInDDL(cleaned)

	// Try to parse and reformat using AST for consistency
	// If parsing fails, return the minimally cleaned version
	if parsed, err := parser.ParseString(cleaned); err == nil {
		var buf strings.Builder
		formatter := format.New(format.Defaults)
		if err := formatter.Format(&buf, parsed.Statements...); err == nil {
			return buf.String()
		}
	}

	return cleaned
}

// normalizeDataTypesInDDL performs minimal normalization of DDL statements
// This function is kept minimal to avoid corrupting complex type definitions
func normalizeDataTypesInDDL(ddl string) string {
	// Normalize hidden passwords (essential for security)
	ddl = regexp.MustCompile(`(?i)\bpassword\s+'?\[HIDDEN\]'?`).ReplaceAllString(ddl, "password ''")

	// Normalize Float defaults for test consistency (ClickHouse sometimes drops trailing zeros)
	floatDefaultPattern := regexp.MustCompile(`(Float32|Float64)\s+DEFAULT\s+(\d+)\.(\s?)`)
	ddl = floatDefaultPattern.ReplaceAllString(ddl, "$1 DEFAULT $2.0$3")

	// Normalize LIFETIME(MIN 0 MAX N) -> LIFETIME(N) for test consistency
	lifetimePattern := regexp.MustCompile(`LIFETIME\s*\(\s*MIN\s+0\s+MAX\s+(\d+)\s*\)`)
	ddl = lifetimePattern.ReplaceAllString(ddl, "LIFETIME($1)")

	return ddl
}

// validateDDLStatement ensures the generated DDL statement is valid by parsing it
func validateDDLStatement(ddl string) error {
	_, err := parser.ParseString(ddl)
	return err
}
