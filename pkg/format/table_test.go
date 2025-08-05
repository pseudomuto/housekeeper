package format_test

import (
	"bytes"
	"strings"
	"testing"

	. "github.com/pseudomuto/housekeeper/pkg/format"
	"github.com/pseudomuto/housekeeper/pkg/parser"
	"github.com/stretchr/testify/require"
)

func TestFormatter_Table(t *testing.T) {
	tests := []struct {
		name     string
		sql      string
		expected []string // Lines expected in output
	}{
		{
			name: "simple create table",
			sql:  "CREATE TABLE users (id UInt64, name String) ENGINE = MergeTree() ORDER BY id;",
			expected: []string{
				"CREATE TABLE `users` (",
				"    `id`   UInt64,",
				"    `name` String",
				")",
				"ENGINE = MergeTree()",
				"ORDER BY id;",
			},
		},
		{
			name: "qualified table name",
			sql:  "CREATE TABLE analytics.events (id UInt64, timestamp DateTime) ENGINE = MergeTree() ORDER BY timestamp;",
			expected: []string{
				"CREATE TABLE `analytics`.`events` (",
				"    `id`        UInt64,",
				"    `timestamp` DateTime",
				")",
				"ENGINE = MergeTree()",
				"ORDER BY timestamp;",
			},
		},
		{
			name: "table with complex columns",
			sql:  "CREATE TABLE test (id UInt64 DEFAULT 0, data Nullable(String) CODEC(ZSTD), tags Array(String)) ENGINE = MergeTree() ORDER BY id;",
			expected: []string{
				"CREATE TABLE `test` (",
				"    `id`   UInt64 DEFAULT 0,",
				"    `data` Nullable(String) CODEC(ZSTD()),",
				"    `tags` Array(String)",
				")",
				"ENGINE = MergeTree()",
				"ORDER BY id;",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			grammar, err := parser.ParseSQL(tt.sql)
			require.NoError(t, err)
			require.Len(t, grammar.Statements, 1)

			var buf bytes.Buffer
			err = Format(&buf, Defaults, grammar.Statements[0])
			require.NoError(t, err)
			formatted := buf.String()
			lines := strings.Split(formatted, "\n")

			// Compare line by line for better error reporting
			require.Len(t, lines, len(tt.expected), "Number of lines mismatch")
			for i, expectedLine := range tt.expected {
				require.Equal(t, expectedLine, lines[i], "Line %d mismatch", i+1)
			}
		})
	}
}

func TestFormatter_alterTable(t *testing.T) {
	tests := []struct {
		name     string
		sql      string
		expected []string
	}{
		{
			name: "add column",
			sql:  "ALTER TABLE users ADD COLUMN email String;",
			expected: []string{
				"ALTER TABLE `users`",
				"    ADD COLUMN `email` String;",
			},
		},
		{
			name: "drop column",
			sql:  "ALTER TABLE users DROP COLUMN IF EXISTS old_field;",
			expected: []string{
				"ALTER TABLE `users`",
				"    DROP COLUMN IF EXISTS `old_field`;",
			},
		},
		{
			name: "rename column",
			sql:  "ALTER TABLE users RENAME COLUMN old_name TO new_name;",
			expected: []string{
				"ALTER TABLE `users`",
				"    RENAME COLUMN `old_name` TO `new_name`;",
			},
		},
		{
			name: "add index",
			sql:  "ALTER TABLE users ADD INDEX idx_email email TYPE minmax GRANULARITY 1;",
			expected: []string{
				"ALTER TABLE `users`",
				"    ADD INDEX `idx_email` email TYPE `minmax` GRANULARITY 1;",
			},
		},
		{
			name: "add index if not exists",
			sql:  "ALTER TABLE users ADD INDEX IF NOT EXISTS idx_name name TYPE minmax GRANULARITY 1;",
			expected: []string{
				"ALTER TABLE `users`",
				"    ADD INDEX IF NOT EXISTS `idx_name` name TYPE `minmax` GRANULARITY 1;",
			},
		},
		{
			name: "drop index",
			sql:  "ALTER TABLE users DROP INDEX idx_email;",
			expected: []string{
				"ALTER TABLE `users`",
				"    DROP INDEX `idx_email`;",
			},
		},
		{
			name: "drop index if exists",
			sql:  "ALTER TABLE users DROP INDEX IF EXISTS idx_old;",
			expected: []string{
				"ALTER TABLE `users`",
				"    DROP INDEX IF EXISTS `idx_old`;",
			},
		},
		{
			name: "add constraint",
			sql:  "ALTER TABLE users ADD CONSTRAINT chk_age CHECK age > 0;",
			expected: []string{
				"ALTER TABLE `users`",
				"    ADD CONSTRAINT `chk_age` CHECK age > 0;",
			},
		},
		{
			name: "add constraint if not exists",
			sql:  "ALTER TABLE users ADD CONSTRAINT IF NOT EXISTS chk_email CHECK email != '';",
			expected: []string{
				"ALTER TABLE `users`",
				"    ADD CONSTRAINT IF NOT EXISTS `chk_email` CHECK email != '';",
			},
		},
		{
			name: "drop constraint",
			sql:  "ALTER TABLE users DROP CONSTRAINT chk_age;",
			expected: []string{
				"ALTER TABLE `users`",
				"    DROP CONSTRAINT `chk_age`;",
			},
		},
		{
			name: "drop constraint if exists",
			sql:  "ALTER TABLE users DROP CONSTRAINT IF EXISTS chk_old;",
			expected: []string{
				"ALTER TABLE `users`",
				"    DROP CONSTRAINT IF EXISTS `chk_old`;",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			grammar, err := parser.ParseSQL(tt.sql)
			require.NoError(t, err)
			require.Len(t, grammar.Statements, 1)

			var buf bytes.Buffer
			err = Format(&buf, Defaults, grammar.Statements[0])
			require.NoError(t, err)
			formatted := buf.String()
			lines := strings.Split(formatted, "\n")

			require.Len(t, lines, len(tt.expected), "Number of lines mismatch")
			for i, expectedLine := range tt.expected {
				require.Equal(t, expectedLine, lines[i], "Line %d mismatch", i+1)
			}
		})
	}
}
