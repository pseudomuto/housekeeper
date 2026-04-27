package utils

import (
	"regexp"
	"strings"
)

// chMacroRe matches ClickHouse server-side macro references as stored by the
// participle lexer. The lexer emits String tokens with their surrounding single
// quotes intact, so '{cluster}' is stored as the Go string "'{cluster}'".
//
// Pattern: opening quote, opening brace, a valid ClickHouse identifier
// ([A-Za-z_][A-Za-z0-9_]*), closing brace, closing quote.
var chMacroRe = regexp.MustCompile(`^'\{[A-Za-z_][A-Za-z0-9_]*\}'$`)

// BacktickIdentifier adds backticks around an identifier, handling nested identifiers.
// It properly handles database.table.column style identifiers by backticking each part.
//
// Examples:
//   - "table" -> "`table`"
//   - "database.table" -> "`database`.`table`"
//   - "db.schema.table" -> "`db`.`schema`.`table`"
//   - "`table`" -> "`table`" (already backticked, not double-backticked)
//   - "" -> ""
//
// This function is used throughout the codebase for consistent identifier formatting
// in generated DDL statements.
func BacktickIdentifier(name string) string {
	if name == "" {
		return ""
	}

	// If the entire string is already backticked and doesn't contain dots outside backticks,
	// return as-is (it's a single identifier that happens to contain dots)
	if len(name) >= 2 && name[0] == '`' && name[len(name)-1] == '`' {
		// Check if there are any backticks in the middle
		inner := name[1 : len(name)-1]
		if !strings.Contains(inner, "`") {
			// This is a single backticked identifier, possibly containing dots
			return name
		}
	}

	// Handle database.table.column format by backticking each part
	parts := strings.Split(name, ".")
	for i, part := range parts {
		// Skip if this part is already backticked
		if len(part) >= 2 && part[0] == '`' && part[len(part)-1] == '`' {
			continue
		}
		parts[i] = "`" + part + "`"
	}
	return strings.Join(parts, ".")
}

// BacktickColumnName adds backticks around a column name without splitting on dots.
// This is specifically for column names that may contain dots (like flattened nested columns).
//
// Examples:
//   - "column" -> "`column`"
//   - "metadata.source" -> "`metadata.source`"  (NOT split)
//   - "`column`" -> "`column`" (already backticked)
//   - "" -> ""
//
// This function treats the entire input as a single identifier, unlike BacktickIdentifier
// which splits on dots for qualified names.
func BacktickColumnName(name string) string {
	if name == "" {
		return ""
	}

	// If already backticked, return as-is
	if len(name) >= 2 && name[0] == '`' && name[len(name)-1] == '`' {
		return name
	}

	// Just backtick the whole thing without splitting
	return "`" + name + "`"
}

// BacktickQualifiedName formats a qualified name (database.name) with proper backticks.
// If database is nil or empty, only the name is backticked.
//
// Examples:
//   - ("analytics", "events") -> "`analytics`.`events`"
//   - (nil, "events") -> "`events`"
//   - ("", "events") -> "`events`"
//
// This is commonly used for formatting table, view, and dictionary names that may
// include a database prefix.
func BacktickQualifiedName(database *string, name string) string {
	if database != nil && *database != "" {
		return BacktickIdentifier(*database) + "." + BacktickIdentifier(name)
	}
	return BacktickIdentifier(name)
}

// IsBackticked checks if a string is already wrapped in backticks.
//
// Examples:
//   - "`table`" -> true
//   - "table" -> false
//   - "`db`.`table`" -> false (qualified name, not a single backticked identifier)
//   - "" -> false
func IsBackticked(s string) bool {
	return len(s) >= 2 && s[0] == '`' && s[len(s)-1] == '`' && !strings.Contains(s[1:len(s)-1], "`")
}

// IsClusterMacro reports whether a cluster name is a ClickHouse server macro reference.
//
// ClickHouse macros use single-quoted braces in ON CLUSTER clauses, e.g. '{cluster}',
// '{shard}'. These are expanded at query execution time from the server's macros config
// (<macros><cluster>…</cluster></macros>). They must be preserved verbatim in DDL so that
// migrations are portable across clusters with different names.
//
// The parser stores the raw lexer token value for String tokens, which includes the
// surrounding single quotes, so '{cluster}' is stored as the Go string "'{cluster}'".
//
// Examples:
//
//	IsClusterMacro("'{cluster}'") → true
//	IsClusterMacro("'{shard}'")   → true
//	IsClusterMacro("default")     → false
//	IsClusterMacro("")            → false
func IsClusterMacro(s string) bool {
	return len(s) >= 2 && s[0] == '\'' && s[len(s)-1] == '\''
}

// IsClickHouseMacro reports whether s is a ClickHouse server macro reference
// of the form '{identifier}' (single-quoted curly-brace expression).
//
// This is stricter than IsClusterMacro: it requires the inner content to be a
// valid identifier wrapped in curly braces, matching the ClickHouse <macros>
// config substitution syntax. Use this when you need to distinguish a genuine
// server macro (suitable for cross-cluster portability) from an arbitrary
// single-quoted cluster name.
//
// Examples:
//
//	IsClickHouseMacro("'{cluster}'")      → true
//	IsClickHouseMacro("'{shard}'")        → true
//	IsClickHouseMacro("'{replica}'")      → true
//	IsClickHouseMacro("'plain-string'")   → false  (no braces)
//	IsClickHouseMacro("default")          → false  (not quoted)
//	IsClickHouseMacro("")                 → false
func IsClickHouseMacro(s string) bool {
	return chMacroRe.MatchString(s)
}

// FormatClusterName formats a cluster name for use in SQL DDL output.
//
// This is the single, canonical place where the macro-vs-identifier distinction is
// handled for ON CLUSTER clauses. It is used by the formatter, SQLBuilder, and the
// schema DDL generators so that macro syntax is preserved end-to-end.
//
// - Macro references (e.g. "'{cluster}'") are returned as-is — the single quotes are
//   part of the ClickHouse syntax and must not be wrapped in backticks.
// - Plain identifiers (e.g. "default", "my-cluster") are backtick-quoted for safety.
//
// Examples:
//
//	FormatClusterName("'{cluster}'") → "'{cluster}'"
//	FormatClusterName("default")     → "`default`"
//	FormatClusterName("")            → ""
func FormatClusterName(name string) string {
	switch {
	case name == "":
		return ""
	case IsClusterMacro(name):
		return name
	default:
		return BacktickIdentifier(name)
	}
}

// StripBackticks removes backticks from an identifier if present.
//
// Examples:
//   - "`table`" -> "table"
//   - "table" -> "table"
//   - "`db`.`table`" -> "db.table"
//   - "" -> ""
func StripBackticks(s string) string {
	// Remove all backticks
	return strings.ReplaceAll(s, "`", "")
}
