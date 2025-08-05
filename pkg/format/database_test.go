package format_test

import (
	"testing"

	"github.com/pseudomuto/housekeeper/pkg/format"
	"github.com/pseudomuto/housekeeper/pkg/parser"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestFormatter_Database(t *testing.T) {
	tests := []struct {
		name     string
		sql      string
		expected string
	}{
		{
			name:     "simple create database",
			sql:      "CREATE DATABASE test;",
			expected: "CREATE DATABASE `test`;",
		},
		{
			name:     "create database with options",
			sql:      "CREATE DATABASE IF NOT EXISTS analytics ON CLUSTER production ENGINE = Atomic COMMENT 'Analytics database';",
			expected: "CREATE DATABASE IF NOT EXISTS `analytics` ON CLUSTER `production` ENGINE = Atomic COMMENT 'Analytics database';",
		},
		{
			name:     "alter database",
			sql:      "ALTER DATABASE analytics ON CLUSTER production MODIFY COMMENT 'Updated comment';",
			expected: "ALTER DATABASE `analytics` ON CLUSTER `production` MODIFY COMMENT 'Updated comment';",
		},
		{
			name:     "drop database",
			sql:      "DROP DATABASE IF EXISTS old_db ON CLUSTER production SYNC;",
			expected: "DROP DATABASE IF EXISTS `old_db` ON CLUSTER `production` SYNC;",
		},
	}

	formatter := format.NewDefault()

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			grammar, err := parser.ParseSQL(tt.sql)
			require.NoError(t, err)
			require.Len(t, grammar.Statements, 1)

			formatted := formatter.Statement(grammar.Statements[0])
			assert.Equal(t, tt.expected, formatted)
		})
	}
}
