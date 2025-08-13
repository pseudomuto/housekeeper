package format_test

import (
	"bytes"
	"testing"

	. "github.com/pseudomuto/housekeeper/pkg/format"
	"github.com/pseudomuto/housekeeper/pkg/parser"
	"github.com/stretchr/testify/require"
)

func TestFormatter_Options(t *testing.T) {
	t.Run("lowercase keywords", func(t *testing.T) {
		sql := "CREATE DATABASE test;"

		options := FormatterOptions{
			IndentSize:        4,
			UppercaseKeywords: false,
			AlignColumns:      true,
		}

		sqlResult, err := parser.ParseString(sql)
		require.NoError(t, err)

		var buf bytes.Buffer
		require.NoError(t, Format(&buf, options, sqlResult.Statements[0]))
		formatted := buf.String()
		require.Equal(t, "create database `test`;", formatted)
	})

	t.Run("custom indent", func(t *testing.T) {
		sql := "CREATE TABLE users (id UInt64, name String) ENGINE = MergeTree();"

		options := FormatterOptions{
			IndentSize:        2,
			UppercaseKeywords: true,
			AlignColumns:      false,
		}

		sqlResult, err := parser.ParseString(sql)
		require.NoError(t, err)

		var buf bytes.Buffer
		require.NoError(t, Format(&buf, options, sqlResult.Statements[0]))
		formatted := buf.String()
		lines := []string{
			"CREATE TABLE `users` (",
			"  `id` UInt64,",
			"  `name` String",
			")",
			"ENGINE = MergeTree();",
		}
		expected := lines[0] + "\n" + lines[1] + "\n" + lines[2] + "\n" + lines[3] + "\n" + lines[4]
		require.Equal(t, expected, formatted)
	})

	t.Run("no column alignment", func(t *testing.T) {
		sql := "CREATE TABLE test (id UInt64, very_long_column_name String) ENGINE = MergeTree();"

		options := FormatterOptions{
			IndentSize:        4,
			UppercaseKeywords: true,
			AlignColumns:      false,
		}

		sqlResult, err := parser.ParseString(sql)
		require.NoError(t, err)

		var buf bytes.Buffer
		require.NoError(t, Format(&buf, options, sqlResult.Statements[0]))
		formatted := buf.String()
		// Should not have extra spaces for alignment
		require.Contains(t, formatted, "`id` UInt64,")
		require.Contains(t, formatted, "`very_long_column_name` String")
	})
}

func TestFormatter_SQL(t *testing.T) {
	sql := `CREATE DATABASE test;
			CREATE TABLE test.users (id UInt64) ENGINE = MergeTree();`

	sqlResult, err := parser.ParseString(sql)
	require.NoError(t, err)
	require.Len(t, sqlResult.Statements, 2)

	var buf bytes.Buffer
	require.NoError(t, Format(&buf, Defaults, sqlResult.Statements...))
	formatted := buf.String()

	expected := "CREATE DATABASE `test`;\n\nCREATE TABLE `test`.`users` (\n    `id` UInt64\n)\nENGINE = MergeTree();"
	require.Equal(t, expected, formatted)
}

func TestFormatter_FormatFunction(t *testing.T) {
	sql := "CREATE DATABASE test;"
	sqlResult, err := parser.ParseString(sql)
	require.NoError(t, err)

	// Test Format function with single statement
	var buf1 bytes.Buffer
	require.NoError(t, Format(&buf1, Defaults, sqlResult.Statements[0]))
	formatted1 := buf1.String()
	require.Equal(t, "CREATE DATABASE `test`;", formatted1)

	// Test Format function with multiple statements
	var buf2 bytes.Buffer
	require.NoError(t, Format(&buf2, Defaults, sqlResult.Statements...))
	formatted2 := buf2.String()
	require.Equal(t, "CREATE DATABASE `test`;", formatted2)
}

func TestFormatter_EmptyInput(t *testing.T) {
	// Test no statements
	var buf1 bytes.Buffer
	require.NoError(t, Format(&buf1, Defaults))
	require.Empty(t, buf1.String())

	// Test nil statement
	var buf2 bytes.Buffer
	require.NoError(t, Format(&buf2, Defaults, nil))
	require.Empty(t, buf2.String())

	// Test empty statements
	var buf3 bytes.Buffer
	require.NoError(t, Format(&buf3, Defaults, []*parser.Statement{}...))
	require.Empty(t, buf3.String())
}

func TestFormatSQL_Function(t *testing.T) {
	tests := []struct {
		name     string
		sql      string
		expected string
	}{
		{
			name:     "single_statement",
			sql:      "CREATE DATABASE test;",
			expected: "CREATE DATABASE `test`;",
		},
		{
			name:     "multiple_statements",
			sql:      "CREATE DATABASE test; CREATE TABLE test.users (id UInt64) ENGINE = MergeTree();",
			expected: "CREATE DATABASE `test`;\n\nCREATE TABLE `test`.`users` (\n    `id` UInt64\n)\nENGINE = MergeTree();",
		},
		{
			name:     "complex_statements",
			sql:      "CREATE DATABASE analytics; CREATE DICTIONARY analytics.users_dict (id UInt64) PRIMARY KEY id SOURCE(HTTP(url 'http://example.com')) LAYOUT(HASHED()) LIFETIME(3600);",
			expected: "CREATE DATABASE `analytics`;\n\nCREATE DICTIONARY `analytics`.`users_dict` (\n    `id` UInt64\n)\nPRIMARY KEY `id`\nSOURCE(HTTP(url 'http://example.com'))\nLAYOUT(HASHED())\nLIFETIME(3600);",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			sqlResult, err := parser.ParseString(tt.sql)
			require.NoError(t, err)

			var buf bytes.Buffer
			require.NoError(t, FormatSQL(&buf, Defaults, sqlResult))

			require.Equal(t, tt.expected, buf.String())
		})
	}
}

func TestFormatSQL_Method(t *testing.T) {
	tests := []struct {
		name     string
		sql      string
		expected string
	}{
		{
			name:     "with_custom_options",
			sql:      "CREATE DATABASE test; CREATE TABLE test.users (id UInt64, name String) ENGINE = MergeTree();",
			expected: "create database `test`;\n\ncreate table `test`.`users` (\n  `id`   UInt64,\n  `name` String\n)\nengine = MergeTree();",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			sqlResult, err := parser.ParseString(tt.sql)
			require.NoError(t, err)

			formatter := New(FormatterOptions{
				IndentSize:        2,
				UppercaseKeywords: false,
				AlignColumns:      true,
			})

			var buf bytes.Buffer
			require.NoError(t, formatter.FormatSQL(&buf, sqlResult))

			require.Equal(t, tt.expected, buf.String())
		})
	}
}

func TestFormatSQL_NilSQL(t *testing.T) {
	var buf bytes.Buffer

	// Test function with nil sql
	require.NoError(t, FormatSQL(&buf, Defaults, nil))
	require.Empty(t, buf.String())

	// Test method with nil sql
	formatter := New(Defaults)
	require.NoError(t, formatter.FormatSQL(&buf, nil))
	require.Empty(t, buf.String())
}

func TestFormatSQL_EmptySQL(t *testing.T) {
	var buf bytes.Buffer

	// Test function with empty sqlResult
	sqlResult := &parser.SQL{Statements: []*parser.Statement{}}
	require.NoError(t, FormatSQL(&buf, Defaults, sqlResult))
	require.Empty(t, buf.String())

	// Test method with empty sqlResult
	formatter := New(Defaults)
	require.NoError(t, formatter.FormatSQL(&buf, sqlResult))
	require.Empty(t, buf.String())
}

func TestFormatter_BooleanDefaults(t *testing.T) {
	tests := []struct {
		name        string
		sql         string
		contains    []string
		notContains []string
	}{
		{
			name:        "boolean_defaults_in_table",
			sql:         "CREATE TABLE test_table (id UInt64, is_premium Bool DEFAULT false, is_active Bool DEFAULT true) ENGINE = MergeTree();",
			contains:    []string{"DEFAULT false", "DEFAULT true"},
			notContains: []string{"`false`", "`true`"},
		},
		{
			name:        "boolean_defaults_in_alter",
			sql:         "ALTER TABLE test_table ADD COLUMN new_col Bool DEFAULT false;",
			contains:    []string{"DEFAULT false"},
			notContains: []string{"`false`"},
		},
		{
			name:        "regular_identifiers_still_backticked",
			sql:         "CREATE TABLE test_table (id UInt64, user_defined String DEFAULT some_identifier) ENGINE = MergeTree();",
			contains:    []string{"DEFAULT `some_identifier`"},
			notContains: []string{"DEFAULT some_identifier"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			sqlResult, err := parser.ParseString(tt.sql)
			require.NoError(t, err)

			var buf bytes.Buffer
			require.NoError(t, Format(&buf, Defaults, sqlResult.Statements[0]))
			formatted := buf.String()

			for _, contains := range tt.contains {
				require.Contains(t, formatted, contains, "formatted output should contain: %s", contains)
			}

			for _, notContains := range tt.notContains {
				require.NotContains(t, formatted, notContains, "formatted output should not contain: %s", notContains)
			}
		})
	}
}
