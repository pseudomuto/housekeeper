package clickhouse

import (
	"regexp"
	"strings"

	"github.com/pseudomuto/housekeeper/pkg/format"
	"github.com/pseudomuto/housekeeper/pkg/parser"
)

// systemObjectDatabases lists ClickHouse-internal databases that should be excluded from
// object extraction (tables, views, dictionaries). "default" is intentionally absent
// because users can and do place tables there.
var systemObjectDatabases = []string{
	"system",
	"information_schema",
	"INFORMATION_SCHEMA",
	"housekeeper",
}

// systemDatabaseNames extends systemObjectDatabases with "default" for use when generating
// CREATE DATABASE statements. "default" always exists in ClickHouse and must not appear as
// a migration target, but the tables/views/dicts it contains are valid user objects.
var systemDatabaseNames = append(append([]string{}, systemObjectDatabases...), "default")

// buildSystemDatabaseExclusion creates a SQL "NOT IN" clause for excluding system databases
// (for object extraction) using parameterized queries. The columnName parameter specifies
// which column to check (e.g., "database", "name").
func buildSystemDatabaseExclusion(columnName string) (string, []any) {
	placeholders := make([]string, len(systemObjectDatabases))
	params := make([]any, len(systemObjectDatabases))

	for i, db := range systemObjectDatabases {
		placeholders[i] = "?"
		params[i] = db
	}

	condition := columnName + " NOT IN (" + strings.Join(placeholders, ", ") + ")"
	return condition, params
}

// buildDatabaseExclusion creates a SQL "NOT IN" clause for excluding system object databases
// and user-specified ignored databases from object extraction (tables, views, dictionaries).
// Use buildDatabaseNameExclusion when generating CREATE DATABASE statements instead.
func buildDatabaseExclusion(columnName string, ignoreDatabases []string) (string, []any) {
	allExcluded := append([]string{}, systemObjectDatabases...)
	allExcluded = append(allExcluded, ignoreDatabases...)

	if len(allExcluded) == 0 {
		return "1=1", []any{}
	}

	placeholders := make([]string, len(allExcluded))
	params := make([]any, len(allExcluded))

	for i, db := range allExcluded {
		placeholders[i] = "?"
		params[i] = db
	}

	condition := columnName + " NOT IN (" + strings.Join(placeholders, ", ") + ")"
	return condition, params
}

// buildDatabaseNameExclusion creates a SQL "NOT IN" clause for use in CREATE DATABASE
// extraction. It excludes systemDatabaseNames (which includes "default") plus any
// user-specified ignored databases.
func buildDatabaseNameExclusion(columnName string, ignoreDatabases []string) (string, []any) {
	allExcluded := append([]string{}, systemDatabaseNames...)
	allExcluded = append(allExcluded, ignoreDatabases...)

	if len(allExcluded) == 0 {
		return "1=1", []any{}
	}

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
