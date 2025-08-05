package format_test

import (
	"strings"
	"testing"

	"github.com/pseudomuto/housekeeper/pkg/format"
	"github.com/pseudomuto/housekeeper/pkg/parser"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestFormatter_Dictionary(t *testing.T) {
	tests := []struct {
		name     string
		sql      string
		expected []string
	}{
		{
			name: "simple dictionary",
			sql: `CREATE DICTIONARY user_dict (
				id UInt64,
				name String
			) PRIMARY KEY id
			SOURCE(HTTP(url 'http://example.com/users'))
			LAYOUT(HASHED())
			LIFETIME(3600);`,
			expected: []string{
				"CREATE DICTIONARY `user_dict` (",
				"    `id`   UInt64,",
				"    `name` String",
				")",
				"PRIMARY KEY `id`",
				"SOURCE(HTTP(url 'http://example.com/users'))",
				"LAYOUT(HASHED())",
				"LIFETIME(3600);",
			},
		},
		{
			name: "dictionary with qualified name",
			sql: `CREATE DICTIONARY analytics.users (
				id UInt64,
				email String
			) PRIMARY KEY id
			SOURCE(HTTP(url 'http://api.example.com'))
			LAYOUT(FLAT())
			LIFETIME(MIN 300 MAX 3600);`,
			expected: []string{
				"CREATE DICTIONARY `analytics`.`users` (",
				"    `id`    UInt64,",
				"    `email` String",
				")",
				"PRIMARY KEY `id`",
				"SOURCE(HTTP(url 'http://api.example.com'))",
				"LAYOUT(FLAT())",
				"LIFETIME(MIN 300 MAX 3600);",
			},
		},
		{
			name: "dictionary with composite primary key",
			sql: `CREATE DICTIONARY test_dict (
				id UInt64,
				parent_id UInt64,
				value String
			) PRIMARY KEY id, parent_id
			SOURCE(HTTP(url 'http://test.com'))
			LAYOUT(COMPLEX_KEY_HASHED())
			LIFETIME(1800);`,
			expected: []string{
				"CREATE DICTIONARY `test_dict` (",
				"    `id`        UInt64,",
				"    `parent_id` UInt64,",
				"    `value`     String",
				")",
				"PRIMARY KEY `id`, `parent_id`",
				"SOURCE(HTTP(url 'http://test.com'))",
				"LAYOUT(COMPLEX_KEY_HASHED())",
				"LIFETIME(1800);",
			},
		},
	}

	formatter := format.NewDefault()

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			grammar, err := parser.ParseSQL(tt.sql)
			require.NoError(t, err)
			require.Len(t, grammar.Statements, 1)

			formatted := formatter.Statement(grammar.Statements[0])
			lines := strings.Split(formatted, "\n")

			// Compare line by line for better error reporting
			require.Len(t, lines, len(tt.expected), "Number of lines mismatch")
			for i, expectedLine := range tt.expected {
				assert.Equal(t, expectedLine, lines[i], "Line %d mismatch", i+1)
			}
		})
	}
}
