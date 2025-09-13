package format_test

import (
	"bytes"
	"testing"

	"github.com/pseudomuto/housekeeper/pkg/format"
	"github.com/pseudomuto/housekeeper/pkg/parser"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestFormatter_CreateFunction(t *testing.T) {
	tests := []struct {
		name     string
		sql      string
		expected string
	}{
		{
			name:     "simple_function",
			sql:      "CREATE FUNCTION test_func AS (x) -> multiply(x, 2);",
			expected: "CREATE FUNCTION `test_func` AS (`x`) -> multiply(`x`, 2);",
		},
		{
			name:     "function_with_multiple_parameters",
			sql:      "CREATE FUNCTION calc AS (a, b, c) -> plus(multiply(a, b), c);",
			expected: "CREATE FUNCTION `calc` AS (`a`, `b`, `c`) -> plus(multiply(`a`, `b`), `c`);",
		},
		{
			name:     "function_with_no_parameters",
			sql:      "CREATE FUNCTION current_time AS () -> now();",
			expected: "CREATE FUNCTION `current_time` AS () -> now();",
		},
		{
			name:     "function_with_on_cluster",
			sql:      "CREATE FUNCTION test_func ON CLUSTER production AS (x) -> multiply(x, 2);",
			expected: "CREATE FUNCTION `test_func` ON CLUSTER `production` AS (`x`) -> multiply(`x`, 2);",
		},
		{
			name:     "function_with_backticked_name",
			sql:      "CREATE FUNCTION `my-special-function` AS (value) -> multiply(value, 2);",
			expected: "CREATE FUNCTION `my-special-function` AS (`value`) -> multiply(`value`, 2);",
		},
		{
			name:     "function_with_backticked_cluster",
			sql:      "CREATE FUNCTION calc ON CLUSTER `prod-cluster` AS (x, y) -> plus(x, y);",
			expected: "CREATE FUNCTION `calc` ON CLUSTER `prod-cluster` AS (`x`, `y`) -> plus(`x`, `y`);",
		},
		{
			name:     "complex_function_expression",
			sql:      "CREATE FUNCTION safe_divide AS (a, b) -> if(equals(b, 0), 0, divide(a, b));",
			expected: "CREATE FUNCTION `safe_divide` AS (`a`, `b`) -> if(equals(`b`, 0), 0, divide(`a`, `b`));",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Parse the SQL
			sql, err := parser.ParseString(tt.sql)
			require.NoError(t, err)
			require.Len(t, sql.Statements, 1)
			require.NotNil(t, sql.Statements[0].CreateFunction)

			// Format the statement
			formatter := format.New(format.Defaults)
			var buf bytes.Buffer
			err = formatter.Format(&buf, sql.Statements[0])
			require.NoError(t, err)

			assert.Equal(t, tt.expected, buf.String())
		})
	}
}

func TestFormatter_DropFunction(t *testing.T) {
	tests := []struct {
		name     string
		sql      string
		expected string
	}{
		{
			name:     "simple_drop",
			sql:      "DROP FUNCTION test_func;",
			expected: "DROP FUNCTION `test_func`;",
		},
		{
			name:     "drop_with_if_exists",
			sql:      "DROP FUNCTION IF EXISTS test_func;",
			expected: "DROP FUNCTION IF EXISTS `test_func`;",
		},
		{
			name:     "drop_with_on_cluster",
			sql:      "DROP FUNCTION test_func ON CLUSTER production;",
			expected: "DROP FUNCTION `test_func` ON CLUSTER `production`;",
		},
		{
			name:     "drop_with_if_exists_and_cluster",
			sql:      "DROP FUNCTION IF EXISTS test_func ON CLUSTER production;",
			expected: "DROP FUNCTION IF EXISTS `test_func` ON CLUSTER `production`;",
		},
		{
			name:     "drop_with_backticked_name",
			sql:      "DROP FUNCTION IF EXISTS `my-special-function`;",
			expected: "DROP FUNCTION IF EXISTS `my-special-function`;",
		},
		{
			name:     "drop_with_backticked_cluster",
			sql:      "DROP FUNCTION calc ON CLUSTER `prod-cluster`;",
			expected: "DROP FUNCTION `calc` ON CLUSTER `prod-cluster`;",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Parse the SQL
			sql, err := parser.ParseString(tt.sql)
			require.NoError(t, err)
			require.Len(t, sql.Statements, 1)
			require.NotNil(t, sql.Statements[0].DropFunction)

			// Format the statement
			formatter := format.New(format.Defaults)
			var buf bytes.Buffer
			err = formatter.Format(&buf, sql.Statements[0])
			require.NoError(t, err)

			assert.Equal(t, tt.expected, buf.String())
		})
	}
}

func TestFormatter_FunctionStatements(t *testing.T) {
	tests := []struct {
		name     string
		sql      string
		expected string
	}{
		{
			name: "multiple_function_statements",
			sql: `CREATE FUNCTION linear AS (x, k, b) -> plus(multiply(k, x), b);
			      CREATE FUNCTION square AS (x) -> multiply(x, x);
			      DROP FUNCTION IF EXISTS old_func;`,
			expected: "CREATE FUNCTION `linear` AS (`x`, `k`, `b`) -> plus(multiply(`k`, `x`), `b`);\n\nCREATE FUNCTION `square` AS (`x`) -> multiply(`x`, `x`);\n\nDROP FUNCTION IF EXISTS `old_func`;",
		},
		{
			name: "mixed_with_other_ddl",
			sql: `CREATE DATABASE test;
			      CREATE FUNCTION calc AS (x) -> multiply(x, 2);
			      CREATE TABLE test.users (id UInt64) ENGINE = MergeTree() ORDER BY id;`,
			expected: "CREATE DATABASE `test`;\n\nCREATE FUNCTION `calc` AS (`x`) -> multiply(`x`, 2);\n\nCREATE TABLE `test`.`users` (\n    `id` UInt64\n)\nENGINE = MergeTree()\nORDER BY `id`;",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Parse the SQL
			sql, err := parser.ParseString(tt.sql)
			require.NoError(t, err)

			// Format all statements
			formatter := format.New(format.Defaults)
			var buf bytes.Buffer
			err = formatter.Format(&buf, sql.Statements...)
			require.NoError(t, err)

			assert.Equal(t, tt.expected, buf.String())
		})
	}
}

func TestFormatter_FunctionOptions(t *testing.T) {
	tests := []struct {
		name     string
		sql      string
		options  format.FormatterOptions
		expected string
	}{
		{
			name: "uppercase_keywords",
			sql:  "CREATE FUNCTION test AS (x) -> multiply(x, 2);",
			options: format.FormatterOptions{
				UppercaseKeywords: true,
				IndentSize:        2,
				AlignColumns:      true,
			},
			expected: "CREATE FUNCTION `test` AS (`x`) -> multiply(`x`, 2);",
		},
		{
			name: "drop_function_uppercase",
			sql:  "DROP FUNCTION IF EXISTS test ON CLUSTER prod;",
			options: format.FormatterOptions{
				UppercaseKeywords: true,
				IndentSize:        2,
				AlignColumns:      true,
			},
			expected: "DROP FUNCTION IF EXISTS `test` ON CLUSTER `prod`;",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Parse the SQL
			sql, err := parser.ParseString(tt.sql)
			require.NoError(t, err)
			require.Len(t, sql.Statements, 1)

			// Format the statement with custom options
			formatter := format.New(tt.options)
			var buf bytes.Buffer
			err = formatter.Format(&buf, sql.Statements[0])
			require.NoError(t, err)

			assert.Equal(t, tt.expected, buf.String())
		})
	}
}

func TestFormatSQL_Functions(t *testing.T) {
	tests := []struct {
		name     string
		sql      string
		expected string
	}{
		{
			name:     "format_sql_with_functions",
			sql:      `CREATE FUNCTION test AS (x) -> multiply(x, 2); DROP FUNCTION old_func;`,
			expected: "CREATE FUNCTION `test` AS (`x`) -> multiply(`x`, 2);\n\nDROP FUNCTION `old_func`;",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Parse the SQL
			sql, err := parser.ParseString(tt.sql)
			require.NoError(t, err)

			// Format using the convenience function
			var buf bytes.Buffer
			err = format.FormatSQL(&buf, format.Defaults, sql)
			require.NoError(t, err)

			assert.Equal(t, tt.expected, buf.String())
		})
	}
}

func TestFormatter_FunctionEdgeCases(t *testing.T) {
	tests := []struct {
		name        string
		description string
		sql         string
		expected    string
	}{
		{
			name:        "function_with_complex_nested_expression",
			description: "Function with deeply nested expressions",
			sql:         "CREATE FUNCTION complex AS (a, b, c) -> if(greater(a, b), multiply(a, c), if(equals(b, 0), 1, divide(c, b)));",
			expected:    "CREATE FUNCTION `complex` AS (`a`, `b`, `c`) -> if(greater(`a`, `b`), multiply(`a`, `c`), if(equals(`b`, 0), 1, divide(`c`, `b`)));",
		},
		{
			name:        "function_with_single_parameter",
			description: "Function with exactly one parameter",
			sql:         "CREATE FUNCTION identity AS (x) -> x;",
			expected:    "CREATE FUNCTION `identity` AS (`x`) -> `x`;",
		},
		{
			name:        "function_with_empty_parameter_list",
			description: "Function with no parameters",
			sql:         "CREATE FUNCTION constant AS () -> 42;",
			expected:    "CREATE FUNCTION `constant` AS () -> 42;",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Parse the SQL
			sql, err := parser.ParseString(tt.sql)
			require.NoError(t, err)
			require.Len(t, sql.Statements, 1)
			require.NotNil(t, sql.Statements[0].CreateFunction)

			// Format the statement
			formatter := format.New(format.Defaults)
			var buf bytes.Buffer
			err = formatter.Format(&buf, sql.Statements[0])
			require.NoError(t, err)

			assert.Equal(t, tt.expected, buf.String())
		})
	}
}
