package format_test

import (
	"testing"

	"github.com/pseudomuto/housekeeper/pkg/format"
	"github.com/pseudomuto/housekeeper/pkg/parser"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestFormatter_Options(t *testing.T) {
	t.Run("lowercase keywords", func(t *testing.T) {
		sql := "CREATE DATABASE test;"

		options := &format.FormatterOptions{
			IndentSize:        4,
			UppercaseKeywords: false,
			AlignColumns:      true,
		}
		formatter := format.New(options)

		grammar, err := parser.ParseSQL(sql)
		require.NoError(t, err)

		formatted := formatter.Statement(grammar.Statements[0])
		assert.Equal(t, "create database `test`;", formatted)
	})

	t.Run("custom indent", func(t *testing.T) {
		sql := "CREATE TABLE users (id UInt64, name String) ENGINE = MergeTree();"

		options := &format.FormatterOptions{
			IndentSize:        2,
			UppercaseKeywords: true,
			AlignColumns:      false,
		}
		formatter := format.New(options)

		grammar, err := parser.ParseSQL(sql)
		require.NoError(t, err)

		formatted := formatter.Statement(grammar.Statements[0])
		lines := []string{
			"CREATE TABLE `users` (",
			"  `id` UInt64,",
			"  `name` String",
			")",
			"ENGINE = MergeTree();",
		}
		expected := lines[0] + "\n" + lines[1] + "\n" + lines[2] + "\n" + lines[3] + "\n" + lines[4]
		assert.Equal(t, expected, formatted)
	})

	t.Run("no column alignment", func(t *testing.T) {
		sql := "CREATE TABLE test (id UInt64, very_long_column_name String) ENGINE = MergeTree();"

		options := &format.FormatterOptions{
			IndentSize:        4,
			UppercaseKeywords: true,
			AlignColumns:      false,
		}
		formatter := format.New(options)

		grammar, err := parser.ParseSQL(sql)
		require.NoError(t, err)

		formatted := formatter.Statement(grammar.Statements[0])
		// Should not have extra spaces for alignment
		assert.Contains(t, formatted, "`id` UInt64,")
		assert.Contains(t, formatted, "`very_long_column_name` String")
	})
}

func TestFormatter_Grammar(t *testing.T) {
	sql := `CREATE DATABASE test;
			CREATE TABLE test.users (id UInt64) ENGINE = MergeTree();`

	grammar, err := parser.ParseSQL(sql)
	require.NoError(t, err)
	require.Len(t, grammar.Statements, 2)

	formatter := format.NewDefault()
	formatted := formatter.Grammar(grammar)

	expected := "CREATE DATABASE `test`;\n\nCREATE TABLE `test`.`users` (\n    `id` UInt64\n)\nENGINE = MergeTree();"
	assert.Equal(t, expected, formatted)
}

func TestFormatter_ConvenienceFunctions(t *testing.T) {
	sql := "CREATE DATABASE test;"
	grammar, err := parser.ParseSQL(sql)
	require.NoError(t, err)

	// Test Statement convenience function
	formatted1 := format.Statement(grammar.Statements[0])
	assert.Equal(t, "CREATE DATABASE `test`;", formatted1)

	// Test Grammar convenience function
	formatted2 := format.Grammar(grammar)
	assert.Equal(t, "CREATE DATABASE `test`;", formatted2)
}

func TestFormatter_EmptyInput(t *testing.T) {
	formatter := format.NewDefault()

	// Test nil statement
	assert.Empty(t, formatter.Statement(nil))

	// Test nil grammar
	assert.Empty(t, formatter.Grammar(nil))

	// Test empty grammar
	emptyGrammar := &parser.Grammar{Statements: []*parser.Statement{}}
	assert.Empty(t, formatter.Grammar(emptyGrammar))
}
