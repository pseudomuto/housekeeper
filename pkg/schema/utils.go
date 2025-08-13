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

// normalizeIdentifier removes surrounding backticks from ClickHouse identifiers
// for consistent comparison between parsed DDL and ClickHouse system table output
func normalizeIdentifier(s string) string {
	if len(s) >= 2 && s[0] == '`' && s[len(s)-1] == '`' {
		return s[1 : len(s)-1]
	}
	return s
}

// normalizeDataType normalizes ClickHouse data type representations for consistent comparison
// ClickHouse system tables return expanded forms that differ from the parsed DDL
func normalizeDataType(dataType string) string {
	// Normalize Decimal types: Decimal(18, 2) -> Decimal64(2)
	if strings.HasPrefix(dataType, "Decimal(18, ") {
		// Extract scale from "Decimal(18, X)"
		if strings.HasSuffix(dataType, ")") {
			parts := strings.Split(dataType, ", ")
			if len(parts) == 2 {
				scale := strings.TrimSuffix(parts[1], ")")
				return "Decimal64(" + scale + ")"
			}
		}
	}
	
	// Normalize DateTime with timezone: DateTime64(3, 'UTC') -> DateTime(3, 'UTC')
	if strings.HasPrefix(dataType, "DateTime64(") && strings.Contains(dataType, ", '") {
		// Extract precision and timezone from "DateTime64(X, 'TZ')"
		start := strings.Index(dataType, "(") + 1
		end := strings.Index(dataType, ")")
		if start > 0 && end > start {
			params := dataType[start:end]
			return "DateTime(" + params + ")"
		}
	}
	
	return dataType
}
