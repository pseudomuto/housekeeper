// Package utils provides common utility functions used throughout the Housekeeper codebase.
//
// This package contains shared utilities that are used by multiple packages to avoid
// code duplication and ensure consistent behavior across the application.
//
// # Identifier Utilities (identifier.go)
//
// The identifier utilities provide consistent handling of ClickHouse SQL identifiers,
// including proper backtick quoting for names that may contain special characters or
// reserved keywords.
//
// # Value Type Utilities (validation.go)
//
// The value type utilities provide proper validation for different data types commonly
// used in ClickHouse SQL generation, particularly for named collection parameters and
// other configuration values.
//
// ## BacktickIdentifier
//
// The primary function for adding backticks to identifiers, handling both simple
// and qualified names:
//
//	// Simple identifier
//	name := utils.BacktickIdentifier("users")
//	// Result: `users`
//
//	// Qualified identifier
//	qualified := utils.BacktickIdentifier("analytics.events")
//	// Result: `analytics`.`events`
//
//	// Already backticked (not double-backticked)
//	existing := utils.BacktickIdentifier("`users`")
//	// Result: `users`
//
// ## BacktickQualifiedName
//
// Specialized function for database-qualified names, commonly used for tables,
// views, and dictionaries:
//
//	db := "analytics"
//	table := "events"
//	qualified := utils.BacktickQualifiedName(&db, table)
//	// Result: `analytics`.`events`
//
//	// Without database prefix
//	simple := utils.BacktickQualifiedName(nil, "users")
//	// Result: `users`
//
// ## Helper Functions
//
// Additional utilities for working with backticked identifiers:
//
//	// Check if already backticked
//	if utils.IsBackticked(name) {
//		// Already has backticks
//	}
//
//	// Remove backticks
//	clean := utils.StripBackticks("`database`.`table`")
//	// Result: database.table
//
// ## IsNumericValue and IsBooleanValue
//
// Value type validation functions for properly formatting SQL values:
//
//	// Validate numeric values (uses strconv.ParseFloat)
//	if utils.IsNumericValue("123.45") {
//		// Can be used without quotes in SQL
//	}
//
//	// Validate boolean values (case-insensitive)
//	if utils.IsBooleanValue("TRUE") {
//		// Can be used without quotes in SQL
//	}
//
//	// Example usage for SQL value formatting
//	value := "123.45"
//	if utils.IsNumericValue(value) || utils.IsBooleanValue(value) {
//		sql += value // No quotes needed
//	} else {
//		sql += "'" + value + "'" // Add quotes for string values
//	}
//
// # Usage Guidelines
//
// These utilities should be used whenever generating or manipulating SQL identifiers
// to ensure consistent formatting across all generated DDL statements. They handle
// edge cases like:
//
//   - Identifiers that are already backticked
//   - Qualified names with multiple parts (database.schema.table)
//   - Empty strings and nil pointers
//   - Special characters in identifiers
//
// The utilities are designed to be safe and idempotent - calling BacktickIdentifier
// on an already backticked identifier will not double-backtick it.
package utils
