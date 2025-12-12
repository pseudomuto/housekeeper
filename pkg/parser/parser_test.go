package parser_test

import (
	"strings"
	"testing"

	. "github.com/pseudomuto/housekeeper/pkg/parser"
	"github.com/stretchr/testify/require"
)

func TestParse(t *testing.T) {
	sql := `CREATE DATABASE test ENGINE = Atomic COMMENT 'Test database';
CREATE TABLE test.events (
    id UInt64,
    name String
) ENGINE = MergeTree() ORDER BY id;`

	reader := strings.NewReader(sql)
	result, err := Parse(reader)

	require.NoError(t, err)
	require.Len(t, result.Statements, 2)

	// Verify database creation
	require.NotNil(t, result.Statements[0].CreateDatabase)
	require.Equal(t, "test", result.Statements[0].CreateDatabase.Name)
	require.NotNil(t, result.Statements[0].CreateDatabase.Comment)
	require.Equal(t, "'Test database'", *result.Statements[0].CreateDatabase.Comment)

	// Verify table creation
	require.NotNil(t, result.Statements[1].CreateTable)
	require.Equal(t, "events", result.Statements[1].CreateTable.Name)
	require.NotNil(t, result.Statements[1].CreateTable.Database)
	require.Equal(t, "test", *result.Statements[1].CreateTable.Database)
	require.Len(t, result.Statements[1].CreateTable.Elements, 2)
}
