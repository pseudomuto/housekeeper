package clickhouse

import (
	"strings"

	"github.com/pseudomuto/housekeeper/pkg/parser"
)

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
	_, err := parser.ParseSQL(ddl)
	return err
}
