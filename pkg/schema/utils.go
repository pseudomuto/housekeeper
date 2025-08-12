package schema

import (
	"strings"

	"github.com/pseudomuto/housekeeper/pkg/parser"
)

// removeQuotes removes surrounding single quotes from a string
func removeQuotes(s string) string {
	if len(s) >= 2 && s[0] == '\'' && s[len(s)-1] == '\'' {
		return s[1 : len(s)-1]
	}

	return s
}

// escapeSQL escapes single quotes in SQL strings
func escapeSQL(s string) string {
	return strings.ReplaceAll(s, "'", "\\'")
}

// formatEngine formats a database engine with its parameters
func formatEngine(engine *parser.DatabaseEngine) string {
	if len(engine.Parameters) == 0 {
		return engine.Name
	}

	params := make([]string, len(engine.Parameters))
	for i, param := range engine.Parameters {
		params[i] = param.Value
	}

	return engine.Name + "(" + strings.Join(params, ", ") + ")"
}

// getStringValue safely gets a string value from a string pointer
func getStringValue(s *string) string {
	if s == nil {
		return ""
	}
	return *s
}
