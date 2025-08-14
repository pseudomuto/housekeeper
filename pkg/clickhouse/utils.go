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

	// Normalize LIFETIME(MIN 0 MAX N) -> LIFETIME(N) when MIN is 0
	// This handles ClickHouse's normalization of single lifetime values
	lifetimePattern := regexp.MustCompile(`LIFETIME\s*\(\s*MIN\s+0\s+MAX\s+(\d+)\s*\)`)
	ddl = lifetimePattern.ReplaceAllString(ddl, "LIFETIME($1)")

	// Normalize Float32 DEFAULT 0. -> Float32 DEFAULT 0.0
	// ClickHouse sometimes truncates trailing zeros
	floatDefaultPattern := regexp.MustCompile(`(Float32|Float64)\s+DEFAULT\s+(\d+)\.(?:\s|,|\n|$)`)
	ddl = floatDefaultPattern.ReplaceAllString(ddl, "$1 DEFAULT $2.0")

	// Normalize hidden passwords back to empty strings to match our schema
	// ClickHouse may use uppercase or lowercase for password keyword
	ddl = regexp.MustCompile(`(?i)\bpassword\s+'?\[HIDDEN\]'?`).ReplaceAllString(ddl, "password ''")

	// Normalize CREATE statement keywords - ClickHouse sometimes returns lowercase
	ddl = regexp.MustCompile(`^CREATE\s+table\s+`).ReplaceAllString(ddl, "CREATE TABLE ")
	ddl = regexp.MustCompile(`^CREATE\s+view\s+`).ReplaceAllString(ddl, "CREATE VIEW ")
	ddl = regexp.MustCompile(`^CREATE\s+materialized\s+view\s+`).ReplaceAllString(ddl, "CREATE MATERIALIZED VIEW ")
	ddl = regexp.MustCompile(`^CREATE\s+database\s+`).ReplaceAllString(ddl, "CREATE DATABASE ")
	ddl = regexp.MustCompile(`^CREATE\s+dictionary\s+`).ReplaceAllString(ddl, "CREATE DICTIONARY ")

	// Remove backticks from identifiers to match our schema format
	// ClickHouse returns `column_name` but we write column_name
	ddl = regexp.MustCompile("`([^`]+)`").ReplaceAllString(ddl, "$1")

	return ddl
}

// validateDDLStatement ensures the generated DDL statement is valid by parsing it
func validateDDLStatement(ddl string) error {
	_, err := parser.ParseString(ddl)
	return err
}
