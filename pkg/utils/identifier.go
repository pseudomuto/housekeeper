package utils

import "strings"

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
