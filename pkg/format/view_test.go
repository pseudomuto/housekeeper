package format_test

import (
	"strings"
	"testing"

	"github.com/pseudomuto/housekeeper/pkg/format"
	"github.com/pseudomuto/housekeeper/pkg/parser"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestFormatter_View(t *testing.T) {
	tests := []struct {
		name     string
		sql      string
		expected []string
	}{
		{
			name: "simple view",
			sql:  "CREATE VIEW user_summary AS SELECT id, name FROM users;",
			expected: []string{
				"CREATE VIEW `user_summary`",
				"AS SELECT",
				"    id,",
				"    name",
				"FROM `users`;",
			},
		},
		{
			name: "qualified view name",
			sql:  "CREATE VIEW analytics.summary AS SELECT count() FROM events;",
			expected: []string{
				"CREATE VIEW `analytics`.`summary`",
				"AS SELECT count()",
				"FROM `events`;",
			},
		},
		{
			name: "view with or replace",
			sql:  "CREATE OR REPLACE VIEW test_view AS SELECT 1;",
			expected: []string{
				"CREATE OR REPLACE VIEW `test_view`",
				"AS SELECT 1;",
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

func TestFormatter_MaterializedView(t *testing.T) {
	tests := []struct {
		name     string
		sql      string
		expected []string
	}{
		{
			name: "simple materialized view",
			sql:  "CREATE MATERIALIZED VIEW mv_simple AS SELECT count() FROM events;",
			expected: []string{
				"CREATE MATERIALIZED VIEW `mv_simple`",
				"AS SELECT count()",
				"FROM `events`;",
			},
		},
		{
			name: "materialized view with engine",
			sql:  "CREATE MATERIALIZED VIEW mv_with_engine ENGINE = MergeTree() AS SELECT date FROM events;",
			expected: []string{
				"CREATE MATERIALIZED VIEW `mv_with_engine`",
				"ENGINE = MergeTree()",
				"AS SELECT date",
				"FROM `events`;",
			},
		},
		{
			name: "materialized view with engine and order by",
			sql:  "CREATE MATERIALIZED VIEW mv_ordered ENGINE = MergeTree() ORDER BY date AS SELECT toDate(timestamp) AS date FROM events;",
			expected: []string{
				"CREATE MATERIALIZED VIEW `mv_ordered`",
				"ENGINE = MergeTree() ORDER BY date",
				"AS SELECT toDate(timestamp) AS `date`",
				"FROM `events`;",
			},
		},
		{
			name: "materialized view with engine and multiple clauses",
			sql:  "CREATE MATERIALIZED VIEW analytics.mv_complex ENGINE = MergeTree() ORDER BY (date, user_id) PARTITION BY toYYYYMM(date) AS SELECT toDate(ts) AS date, user_id, count() AS cnt FROM events GROUP BY date, user_id;",
			expected: []string{
				"CREATE MATERIALIZED VIEW `analytics`.`mv_complex`",
				"ENGINE = MergeTree() ORDER BY (date,user_id) PARTITION BY toYYYYMM(date)",
				"AS SELECT",
				"    toDate(ts) AS `date`,",
				"    user_id,",
				"    count() AS `cnt`",
				"FROM `events`",
				"GROUP BY date, user_id;",
			},
		},
		{
			name: "materialized view with TO table",
			sql:  "CREATE MATERIALIZED VIEW mv_to_table TO target_table AS SELECT * FROM source;",
			expected: []string{
				"CREATE MATERIALIZED VIEW `mv_to_table`",
				"TO `target_table`",
				"AS SELECT *",
				"FROM `source`;",
			},
		},
		{
			name: "materialized view with POPULATE",
			sql:  "CREATE MATERIALIZED VIEW mv_populate ENGINE = MergeTree() ORDER BY date POPULATE AS SELECT toDate(ts) AS date FROM events;",
			expected: []string{
				"CREATE MATERIALIZED VIEW `mv_populate`",
				"ENGINE = MergeTree() ORDER BY date",
				"POPULATE",
				"AS SELECT toDate(ts) AS `date`",
				"FROM `events`;",
			},
		},
		{
			name: "materialized view with OR REPLACE and IF NOT EXISTS",
			sql:  "CREATE OR REPLACE MATERIALIZED VIEW IF NOT EXISTS mv_conditional ENGINE = MergeTree() ORDER BY id AS SELECT id FROM users;",
			expected: []string{
				"CREATE OR REPLACE MATERIALIZED VIEW IF NOT EXISTS `mv_conditional`",
				"ENGINE = MergeTree() ORDER BY id",
				"AS SELECT id",
				"FROM `users`;",
			},
		},
		{
			name: "materialized view with ON CLUSTER",
			sql:  "CREATE MATERIALIZED VIEW mv_cluster ON CLUSTER production ENGINE = MergeTree() ORDER BY id AS SELECT id FROM events;",
			expected: []string{
				"CREATE MATERIALIZED VIEW `mv_cluster` ON CLUSTER `production`",
				"ENGINE = MergeTree() ORDER BY id",
				"AS SELECT id",
				"FROM `events`;",
			},
		},
		{
			name: "materialized view with ReplacingMergeTree",
			sql:  "CREATE MATERIALIZED VIEW mv_replacing ENGINE = ReplacingMergeTree(version) ORDER BY id AS SELECT id, version FROM products;",
			expected: []string{
				"CREATE MATERIALIZED VIEW `mv_replacing`",
				"ENGINE = ReplacingMergeTree(`version`) ORDER BY id",
				"AS SELECT",
				"    id,",
				"    version",
				"FROM `products`;",
			},
		},
		{
			name: "complex materialized view with all features",
			sql:  "CREATE OR REPLACE MATERIALIZED VIEW IF NOT EXISTS analytics.mv_full ON CLUSTER prod TO analytics.target ENGINE = AggregatingMergeTree() ORDER BY (date, region) PARTITION BY toYYYYMM(date) PRIMARY KEY (date, region) POPULATE AS SELECT toDate(ts) AS date, region, sum(amount) AS total FROM sales GROUP BY date, region;",
			expected: []string{
				"CREATE OR REPLACE MATERIALIZED VIEW IF NOT EXISTS `analytics`.`mv_full` ON CLUSTER `prod`",
				"TO `analytics`.`target`",
				"ENGINE = AggregatingMergeTree() ORDER BY (date,region) PARTITION BY toYYYYMM(date) PRIMARY KEY (date,region)",
				"POPULATE",
				"AS SELECT",
				"    toDate(ts) AS `date`,",
				"    region,",
				"    sum(amount) AS `total`",
				"FROM `sales`",
				"GROUP BY date, region;",
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

func TestFormatter_ViewOperations(t *testing.T) {
	tests := []struct {
		name     string
		sql      string
		expected string
	}{
		{
			name:     "attach view",
			sql:      "ATTACH VIEW analytics.summary;",
			expected: "ATTACH VIEW `analytics`.`summary`;",
		},
		{
			name:     "attach view if not exists",
			sql:      "ATTACH VIEW IF NOT EXISTS summary ON CLUSTER prod;",
			expected: "ATTACH VIEW IF NOT EXISTS `summary` ON CLUSTER `prod`;",
		},
		{
			name:     "detach view",
			sql:      "DETACH VIEW analytics.summary;",
			expected: "DETACH VIEW `analytics`.`summary`;",
		},
		{
			name:     "detach view if exists",
			sql:      "DETACH VIEW IF EXISTS summary PERMANENTLY;",
			expected: "DETACH VIEW IF EXISTS `summary` PERMANENTLY;",
		},
		{
			name:     "detach view sync",
			sql:      "DETACH VIEW summary ON CLUSTER prod SYNC;",
			expected: "DETACH VIEW `summary` ON CLUSTER `prod` SYNC;",
		},
		{
			name:     "drop view",
			sql:      "DROP VIEW analytics.summary;",
			expected: "DROP VIEW `analytics`.`summary`;",
		},
		{
			name:     "drop view if exists",
			sql:      "DROP VIEW IF EXISTS summary ON CLUSTER prod SYNC;",
			expected: "DROP VIEW IF EXISTS `summary` ON CLUSTER `prod` SYNC;",
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
