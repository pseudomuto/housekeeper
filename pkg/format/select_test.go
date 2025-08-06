package format_test

import (
	"bytes"
	"strings"
	"testing"

	. "github.com/pseudomuto/housekeeper/pkg/format"
	"github.com/pseudomuto/housekeeper/pkg/parser"
	"github.com/stretchr/testify/require"
)

func TestFormatter_selectStatement(t *testing.T) {
	tests := []struct {
		name     string
		sql      string
		expected []string
	}{
		{
			name: "simple select",
			sql:  "SELECT id FROM users;",
			expected: []string{
				"SELECT id",
				"FROM `users`;",
			},
		},
		{
			name: "select with multiple columns",
			sql:  "SELECT id, name, email FROM users;",
			expected: []string{
				"SELECT",
				"    id,",
				"    name,",
				"    email",
				"FROM `users`;",
			},
		},
		{
			name: "select with alias",
			sql:  "SELECT id AS user_id, name FROM users;",
			expected: []string{
				"SELECT",
				"    id AS `user_id`,",
				"    name",
				"FROM `users`;",
			},
		},
		{
			name: "select with where clause",
			sql:  "SELECT id FROM users WHERE active = 1;",
			expected: []string{
				"SELECT id",
				"FROM `users`",
				"WHERE active = 1;",
			},
		},
		{
			name: "select with join",
			sql:  "SELECT u.id, p.title FROM users AS u LEFT JOIN posts AS p ON u.id = p.user_id;",
			expected: []string{
				"SELECT",
				"    u.id,",
				"    p.title",
				"FROM `users` AS `u`",
				"LEFT JOIN `posts` AS `p` ON u.id = p.user_id;",
			},
		},
		{
			name: "select with group by and order by",
			sql:  "SELECT status, count() FROM users GROUP BY status ORDER BY count() DESC;",
			expected: []string{
				"SELECT",
				"    status,",
				"    count()",
				"FROM `users`",
				"GROUP BY status",
				"ORDER BY count() DESC;",
			},
		},
		{
			name: "select with limit",
			sql:  "SELECT id FROM users ORDER BY id LIMIT 10;",
			expected: []string{
				"SELECT id",
				"FROM `users`",
				"ORDER BY id",
				"LIMIT 10;",
			},
		},
		{
			name: "select distinct",
			sql:  "SELECT DISTINCT status FROM users;",
			expected: []string{
				"SELECT DISTINCT status",
				"FROM `users`;",
			},
		},
		{
			name: "select star",
			sql:  "SELECT * FROM users;",
			expected: []string{
				"SELECT *",
				"FROM `users`;",
			},
		},
		{
			name: "complex select with all clauses",
			sql:  "SELECT u.id, u.name, count(p.id) AS post_count FROM users AS u LEFT JOIN posts AS p ON u.id = p.user_id WHERE u.active = 1 GROUP BY u.id, u.name HAVING count(p.id) > 0 ORDER BY post_count DESC LIMIT 5;",
			expected: []string{
				"SELECT",
				"    u.id,",
				"    u.name,",
				"    count(p.id) AS `post_count`",
				"FROM `users` AS `u`",
				"LEFT JOIN `posts` AS `p` ON u.id = p.user_id",
				"WHERE u.active = 1",
				"GROUP BY u.id, u.name",
				"HAVING count(p.id) > 0",
				"ORDER BY post_count DESC",
				"LIMIT 5;",
			},
		},
		{
			name: "select with single CTE",
			sql:  "WITH stats AS (SELECT user_id, count() AS cnt FROM events GROUP BY user_id) SELECT user_id, cnt FROM stats WHERE cnt > 100;",
			expected: []string{
				"WITH",
				"    `stats` AS (",
				"        SELECT",
				"            user_id,",
				"            count() AS `cnt`",
				"        FROM `events`",
				"        GROUP BY user_id",
				"    )",
				"SELECT",
				"    user_id,",
				"    cnt",
				"FROM `stats`",
				"WHERE cnt > 100;",
			},
		},
		{
			name: "select with multiple CTEs",
			sql:  "WITH daily AS (SELECT date, count() AS cnt FROM events GROUP BY date), weekly AS (SELECT toStartOfWeek(date) AS week, sum(cnt) AS total FROM daily GROUP BY week) SELECT week, total FROM weekly ORDER BY week;",
			expected: []string{
				"WITH",
				"    `daily` AS (",
				"        SELECT",
				"            date,",
				"            count() AS `cnt`",
				"        FROM `events`",
				"        GROUP BY date",
				"    ),",
				"    `weekly` AS (",
				"        SELECT",
				"            toStartOfWeek(date) AS `week`,",
				"            sum(cnt) AS `total`",
				"        FROM `daily`",
				"        GROUP BY week",
				"    )",
				"SELECT",
				"    week,",
				"    total",
				"FROM `weekly`",
				"ORDER BY week;",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			grammar, err := parser.ParseSQL(tt.sql)
			require.NoError(t, err)
			require.Len(t, grammar.Statements, 1)
			require.NotNil(t, grammar.Statements[0].SelectStatement)

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

func TestFormatter_SelectInView(t *testing.T) {
	tests := []struct {
		name     string
		sql      string
		expected []string
	}{
		{
			name: "materialized view with simple select",
			sql:  "CREATE MATERIALIZED VIEW mv_users AS SELECT id, name FROM users;",
			expected: []string{
				"CREATE MATERIALIZED VIEW `mv_users`",
				"AS SELECT",
				"    id,",
				"    name",
				"FROM `users`;",
			},
		},
		{
			name: "view with complex select",
			sql:  "CREATE VIEW user_stats AS SELECT user_id, count() AS post_count FROM posts GROUP BY user_id;",
			expected: []string{
				"CREATE VIEW `user_stats`",
				"AS SELECT",
				"    user_id,",
				"    count() AS `post_count`",
				"FROM `posts`",
				"GROUP BY user_id;",
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
