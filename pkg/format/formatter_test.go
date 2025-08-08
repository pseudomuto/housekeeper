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

		sqlResult, err := parser.ParseSQL(sql)
		require.NoError(t, err)

		var buf bytes.Buffer
		err = Format(&buf, options, sqlResult.Statements[0])
		require.NoError(t, err)
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

		sqlResult, err := parser.ParseSQL(sql)
		require.NoError(t, err)

		var buf bytes.Buffer
		err = Format(&buf, options, sqlResult.Statements[0])
		require.NoError(t, err)
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

		sqlResult, err := parser.ParseSQL(sql)
		require.NoError(t, err)

		var buf bytes.Buffer
		err = Format(&buf, options, sqlResult.Statements[0])
		require.NoError(t, err)
		formatted := buf.String()
		// Should not have extra spaces for alignment
		require.Contains(t, formatted, "`id` UInt64,")
		require.Contains(t, formatted, "`very_long_column_name` String")
	})
}

func TestFormatter_SQL(t *testing.T) {
	sql := `CREATE DATABASE test;
			CREATE TABLE test.users (id UInt64) ENGINE = MergeTree();`

	sqlResult, err := parser.ParseSQL(sql)
	require.NoError(t, err)
	require.Len(t, sqlResult.Statements, 2)

	var buf bytes.Buffer
	err = Format(&buf, Defaults, sqlResult.Statements...)
	require.NoError(t, err)
	formatted := buf.String()

	expected := "CREATE DATABASE `test`;\n\nCREATE TABLE `test`.`users` (\n    `id` UInt64\n)\nENGINE = MergeTree();"
	require.Equal(t, expected, formatted)
}

func TestFormatter_FormatFunction(t *testing.T) {
	sql := "CREATE DATABASE test;"
	sqlResult, err := parser.ParseSQL(sql)
	require.NoError(t, err)

	// Test Format function with single statement
	var buf1 bytes.Buffer
	err = Format(&buf1, Defaults, sqlResult.Statements[0])
	require.NoError(t, err)
	formatted1 := buf1.String()
	require.Equal(t, "CREATE DATABASE `test`;", formatted1)

	// Test Format function with multiple statements
	var buf2 bytes.Buffer
	err = Format(&buf2, Defaults, sqlResult.Statements...)
	require.NoError(t, err)
	formatted2 := buf2.String()
	require.Equal(t, "CREATE DATABASE `test`;", formatted2)
}

func TestFormatter_EmptyInput(t *testing.T) {
	// Test no statements
	var buf1 bytes.Buffer
	err := Format(&buf1, Defaults)
	require.NoError(t, err)
	require.Empty(t, buf1.String())

	// Test nil statement
	var buf2 bytes.Buffer
	err = Format(&buf2, Defaults, nil)
	require.NoError(t, err)
	require.Empty(t, buf2.String())

	// Test empty statements
	var buf3 bytes.Buffer
	err = Format(&buf3, Defaults, []*parser.Statement{}...)
	require.NoError(t, err)
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
			sqlResult, err := parser.ParseSQL(tt.sql)
			require.NoError(t, err)

			var buf bytes.Buffer
			err = FormatSQL(&buf, Defaults, sqlResult)
			require.NoError(t, err)

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
			sqlResult, err := parser.ParseSQL(tt.sql)
			require.NoError(t, err)

			formatter := New(FormatterOptions{
				IndentSize:        2,
				UppercaseKeywords: false,
				AlignColumns:      true,
			})

			var buf bytes.Buffer
			err = formatter.FormatSQL(&buf, sqlResult)
			require.NoError(t, err)

			require.Equal(t, tt.expected, buf.String())
		})
	}
}

func TestFormatSQL_NilSQL(t *testing.T) {
	var buf bytes.Buffer

	// Test function with nil grammar
	err := FormatSQL(&buf, Defaults, nil)
	require.NoError(t, err)
	require.Empty(t, buf.String())

	// Test method with nil grammar
	formatter := New(Defaults)
	err = formatter.FormatSQL(&buf, nil)
	require.NoError(t, err)
	require.Empty(t, buf.String())
}

func TestFormatSQL_EmptySQL(t *testing.T) {
	var buf bytes.Buffer

	// Test function with empty sqlResult
	sqlResult := &parser.SQL{Statements: []*parser.Statement{}}
	err := FormatSQL(&buf, Defaults, sqlResult)
	require.NoError(t, err)
	require.Empty(t, buf.String())

	// Test method with empty sqlResult
	formatter := New(Defaults)
	err = formatter.FormatSQL(&buf, sqlResult)
	require.NoError(t, err)
	require.Empty(t, buf.String())
}
