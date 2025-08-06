// Package format provides well-formatted SQL output for ClickHouse DDL statements.
//
// This package takes parsed parser statements and generates clean, readable SQL
// with consistent formatting, proper indentation, and standardized styling.
// It separates formatting concerns from parsing and migration logic.
//
// Key features:
//   - Consistent indentation and spacing
//   - Proper line breaks for readability
//   - Standardized keyword casing
//   - Column alignment in table definitions
//   - Comprehensive support for all DDL statement types
//   - ClickHouse-optimized backtick formatting for identifiers
//
// Usage:
//
//	// Object-oriented API with default options
//	formatter := format.New(format.Defaults)
//
//	// Object-oriented API with custom options
//	formatter := format.New(format.FormatterOptions{
//		IndentSize:        2,
//		UppercaseKeywords: false,
//		AlignColumns:      true,
//	})
//
//	var buf bytes.Buffer
//	err := formatter.Format(&buf, statements...)
//
//	// Functional API
//	var buf bytes.Buffer
//	err := format.Format(&buf, format.Defaults, statements...)
//
//	// Convenient grammar formatting
//	grammar, _ := parser.ParseSQL("CREATE DATABASE test; CREATE TABLE test.users (id UInt64) ENGINE = MergeTree();")
//	var buf bytes.Buffer
//	err := format.FormatGrammar(&buf, format.Defaults, grammar)
//
// The formatter supports all ClickHouse DDL operations including databases,
// tables, dictionaries, views, and SELECT statements with proper formatting
// for complex features like CTEs, window functions, and nested structures.
package format
