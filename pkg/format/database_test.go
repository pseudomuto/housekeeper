package format_test

import (
	"bytes"
	"testing"

	. "github.com/pseudomuto/housekeeper/pkg/format"
	"github.com/pseudomuto/housekeeper/pkg/parser"
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

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			sqlResult, err := parser.ParseString(tt.sql)
			require.NoError(t, err)
			require.Len(t, sqlResult.Statements, 1)

			var buf bytes.Buffer
			err = Format(&buf, Defaults, sqlResult.Statements[0])
			require.NoError(t, err)
			formatted := buf.String()
			require.Equal(t, tt.expected, formatted)
		})
	}
}
