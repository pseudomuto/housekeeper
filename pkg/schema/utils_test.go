package schema

import (
	"testing"

	"github.com/pseudomuto/housekeeper/pkg/parser"
	"github.com/stretchr/testify/require"
)

func TestNormalizeIdentifier(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{
			name:     "no backticks",
			input:    "column_name",
			expected: "column_name",
		},
		{
			name:     "with backticks",
			input:    "`column_name`",
			expected: "column_name",
		},
		{
			name:     "empty string",
			input:    "",
			expected: "",
		},
		{
			name:     "single character",
			input:    "a",
			expected: "a",
		},
		{
			name:     "single backtick start only",
			input:    "`name",
			expected: "`name",
		},
		{
			name:     "single backtick end only",
			input:    "name`",
			expected: "name`",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := normalizeIdentifier(tt.input)
			require.Equal(t, tt.expected, result)
		})
	}
}

func TestIdentifiersEqual(t *testing.T) {
	tests := []struct {
		name     string
		id1      *parser.IdentifierExpr
		id2      *parser.IdentifierExpr
		expected bool
	}{
		{
			name:     "both nil",
			id1:      nil,
			id2:      nil,
			expected: true,
		},
		{
			name:     "first nil",
			id1:      nil,
			id2:      &parser.IdentifierExpr{Name: "column"},
			expected: false,
		},
		{
			name:     "second nil",
			id1:      &parser.IdentifierExpr{Name: "column"},
			id2:      nil,
			expected: false,
		},
		{
			name:     "equal simple names",
			id1:      &parser.IdentifierExpr{Name: "column"},
			id2:      &parser.IdentifierExpr{Name: "column"},
			expected: true,
		},
		{
			name:     "equal names case insensitive",
			id1:      &parser.IdentifierExpr{Name: "Column"},
			id2:      &parser.IdentifierExpr{Name: "column"},
			expected: true,
		},
		{
			name:     "equal names with backticks on first",
			id1:      &parser.IdentifierExpr{Name: "`column`"},
			id2:      &parser.IdentifierExpr{Name: "column"},
			expected: true,
		},
		{
			name:     "equal names with backticks on second",
			id1:      &parser.IdentifierExpr{Name: "column"},
			id2:      &parser.IdentifierExpr{Name: "`column`"},
			expected: true,
		},
		{
			name:     "equal names with backticks on both",
			id1:      &parser.IdentifierExpr{Name: "`column`"},
			id2:      &parser.IdentifierExpr{Name: "`column`"},
			expected: true,
		},
		{
			name:     "backticks with case difference",
			id1:      &parser.IdentifierExpr{Name: "`Column`"},
			id2:      &parser.IdentifierExpr{Name: "column"},
			expected: true,
		},
		{
			name:     "different names",
			id1:      &parser.IdentifierExpr{Name: "column1"},
			id2:      &parser.IdentifierExpr{Name: "column2"},
			expected: false,
		},
		{
			name:     "different names with backticks",
			id1:      &parser.IdentifierExpr{Name: "`column1`"},
			id2:      &parser.IdentifierExpr{Name: "`column2`"},
			expected: false,
		},
		{
			name:     "equal with database qualifier",
			id1:      &parser.IdentifierExpr{Database: stringPtr("db"), Name: "column"},
			id2:      &parser.IdentifierExpr{Database: stringPtr("db"), Name: "column"},
			expected: true,
		},
		{
			name:     "equal with database qualifier backticks",
			id1:      &parser.IdentifierExpr{Database: stringPtr("`db`"), Name: "`column`"},
			id2:      &parser.IdentifierExpr{Database: stringPtr("db"), Name: "column"},
			expected: true,
		},
		{
			name:     "different database qualifiers",
			id1:      &parser.IdentifierExpr{Database: stringPtr("db1"), Name: "column"},
			id2:      &parser.IdentifierExpr{Database: stringPtr("db2"), Name: "column"},
			expected: false,
		},
		{
			name:     "one with database qualifier one without",
			id1:      &parser.IdentifierExpr{Database: stringPtr("db"), Name: "column"},
			id2:      &parser.IdentifierExpr{Name: "column"},
			expected: false,
		},
		{
			name:     "equal with table qualifier",
			id1:      &parser.IdentifierExpr{Table: stringPtr("tbl"), Name: "column"},
			id2:      &parser.IdentifierExpr{Table: stringPtr("tbl"), Name: "column"},
			expected: true,
		},
		{
			name:     "equal with table qualifier backticks",
			id1:      &parser.IdentifierExpr{Table: stringPtr("`tbl`"), Name: "`column`"},
			id2:      &parser.IdentifierExpr{Table: stringPtr("tbl"), Name: "column"},
			expected: true,
		},
		{
			name:     "full qualification with backticks",
			id1:      &parser.IdentifierExpr{Database: stringPtr("`db`"), Table: stringPtr("`tbl`"), Name: "`column`"},
			id2:      &parser.IdentifierExpr{Database: stringPtr("db"), Table: stringPtr("tbl"), Name: "column"},
			expected: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := identifiersEqual(tt.id1, tt.id2)
			require.Equal(t, tt.expected, result, "identifiersEqual(%v, %v) = %v, want %v", tt.id1, tt.id2, result, tt.expected)
		})
	}
}

// TestViewBacktickNormalization tests that views with backticks in identifiers
// are correctly compared as equal to views without backticks.
// ClickHouse stores views with backticks but users typically write them without.
func TestViewBacktickNormalization(t *testing.T) {
	// View without backticks (user schema file)
	schemaSQL := `CREATE MATERIALIZED VIEW mydb.my_view ON CLUSTER mycluster
REFRESH EVERY 10 SECONDS
APPEND TO mydb.target_table
AS
WITH pending AS (SELECT id, name FROM mydb.source_table GROUP BY id, name HAVING max(status) = 1 LIMIT 1000)
SELECT id, name FROM mydb.data_table WHERE (id, name) IN pending GROUP BY id, name;`

	// View with backticks (ClickHouse output)
	clickhouseSQL := "CREATE MATERIALIZED VIEW `mydb`.`my_view` ON CLUSTER `mycluster` " +
		"REFRESH EVERY 10 SECONDS " +
		"APPEND TO `mydb`.`target_table` " +
		"AS " +
		"WITH `pending` AS (SELECT `id`, `name` FROM `mydb`.`source_table` GROUP BY `id`, `name` HAVING max(`status`) = 1 LIMIT 1000) " +
		"SELECT `id`, `name` FROM `mydb`.`data_table` WHERE (`id`, `name`) IN `pending` GROUP BY `id`, `name`;"

	schemaParsed, err := parser.ParseString(schemaSQL)
	require.NoError(t, err)
	clickhouseParsed, err := parser.ParseString(clickhouseSQL)
	require.NoError(t, err)

	schemaViews := extractViewsFromSQL(schemaParsed)
	clickhouseViews := extractViewsFromSQL(clickhouseParsed)

	schemaView := schemaViews["mydb.my_view"]
	clickhouseView := clickhouseViews["mydb.my_view"]

	require.NotNil(t, schemaView)
	require.NotNil(t, clickhouseView)

	equal := viewsAreEqual(clickhouseView, schemaView)
	require.True(t, equal, "views should be equal despite backtick differences")
}

// TestINExpressionWithParentheses tests that IN expressions with and without parentheses
// around the CTE reference are considered equal.
// ClickHouse wraps CTE references in parentheses: IN (cte_name) vs IN cte_name
func TestINExpressionWithParentheses(t *testing.T) {
	withParens := `SELECT * FROM t WHERE (a, b) IN (my_cte);`
	withoutParens := `SELECT * FROM t WHERE (a, b) IN my_cte;`

	parsedWith, err := parser.ParseString(withParens)
	require.NoError(t, err)
	parsedWithout, err := parser.ParseString(withoutParens)
	require.NoError(t, err)

	selectWith := parsedWith.Statements[0].SelectStatement
	selectWithout := parsedWithout.Statements[0].SelectStatement

	equal := selectStatementsAreEqualAST(&selectWith.SelectStatement, &selectWithout.SelectStatement)
	require.True(t, equal, "IN (cte) and IN cte should be considered equal")
}

// TestNOTINExpressionWithParentheses tests that NOT IN expressions work like IN expressions
func TestNOTINExpressionWithParentheses(t *testing.T) {
	withParens := `SELECT * FROM t WHERE (a, b) NOT IN (my_cte);`
	withoutParens := `SELECT * FROM t WHERE (a, b) NOT IN my_cte;`

	parsedWith, err := parser.ParseString(withParens)
	require.NoError(t, err)
	parsedWithout, err := parser.ParseString(withoutParens)
	require.NoError(t, err)

	selectWith := parsedWith.Statements[0].SelectStatement
	selectWithout := parsedWithout.Statements[0].SelectStatement

	equal := selectStatementsAreEqualAST(&selectWith.SelectStatement, &selectWithout.SelectStatement)
	require.True(t, equal, "NOT IN (cte) and NOT IN cte should be considered equal")
}

// TestTimeUnitNormalization tests that SECOND/SECONDS and other time units
// are treated as equivalent (singular vs plural).
func TestTimeUnitNormalization(t *testing.T) {
	tests := []struct {
		name     string
		unit1    string
		unit2    string
		expected bool
	}{
		{"SECOND equals SECONDS", "SECOND", "SECONDS", true},
		{"SECONDS equals SECOND", "SECONDS", "SECOND", true},
		{"MINUTE equals MINUTES", "MINUTE", "MINUTES", true},
		{"HOUR equals HOURS", "HOUR", "HOURS", true},
		{"DAY equals DAYS", "DAY", "DAYS", true},
		{"case insensitive", "second", "SECONDS", true},
		{"different units", "SECOND", "MINUTE", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := timeUnitsAreEqual(tt.unit1, tt.unit2)
			require.Equal(t, tt.expected, result)
		})
	}
}

// TestRefreshIntervalNormalization tests that equivalent refresh intervals
// are considered equal (e.g., 60 SECOND == 1 MINUTE).
func TestRefreshIntervalNormalization(t *testing.T) {
	// Schema: REFRESH EVERY 60 SECONDS
	schemaSQL := `CREATE MATERIALIZED VIEW mydb.my_view
REFRESH EVERY 60 SECONDS
APPEND TO mydb.target
AS SELECT id FROM mydb.source;`

	// ClickHouse normalizes to: REFRESH EVERY 1 MINUTE
	clickhouseSQL := `CREATE MATERIALIZED VIEW mydb.my_view
REFRESH EVERY 1 MINUTE
APPEND TO mydb.target
AS SELECT id FROM mydb.source;`

	schemaParsed, err := parser.ParseString(schemaSQL)
	require.NoError(t, err)
	clickhouseParsed, err := parser.ParseString(clickhouseSQL)
	require.NoError(t, err)

	schemaViews := extractViewsFromSQL(schemaParsed)
	clickhouseViews := extractViewsFromSQL(clickhouseParsed)

	schemaView := schemaViews["mydb.my_view"]
	clickhouseView := clickhouseViews["mydb.my_view"]

	t.Run("refresh clauses equal", func(t *testing.T) {
		equal := refreshClausesAreEqual(schemaView.Statement.Refresh, clickhouseView.Statement.Refresh)
		require.True(t, equal, "60 SECONDS and 1 MINUTE should be equal")
	})

	t.Run("full view comparison", func(t *testing.T) {
		equal := viewsAreEqual(clickhouseView, schemaView)
		require.True(t, equal)
	})
}

// TestIntervalVsToIntervalFunction tests that INTERVAL expressions are considered
// equal to their toIntervalXxx() function equivalents.
// ClickHouse converts "INTERVAL 3 HOUR" to "toIntervalHour(3)".
func TestIntervalVsToIntervalFunction(t *testing.T) {
	// Schema uses INTERVAL syntax in a view
	schemaSQL := `CREATE MATERIALIZED VIEW mydb.my_view
REFRESH EVERY 10 SECONDS APPEND TO mydb.target
AS SELECT id FROM mydb.source WHERE ts < now() - INTERVAL 3 HOUR;`

	// ClickHouse converts to function call
	clickhouseSQL := `CREATE MATERIALIZED VIEW mydb.my_view
REFRESH EVERY 10 SECONDS APPEND TO mydb.target
AS SELECT id FROM mydb.source WHERE ts < now() - toIntervalHour(3);`

	schemaParsed, err := parser.ParseString(schemaSQL)
	require.NoError(t, err)
	clickhouseParsed, err := parser.ParseString(clickhouseSQL)
	require.NoError(t, err)

	schemaViews := extractViewsFromSQL(schemaParsed)
	clickhouseViews := extractViewsFromSQL(clickhouseParsed)

	schemaView := schemaViews["mydb.my_view"]
	clickhouseView := clickhouseViews["mydb.my_view"]

	equal := viewsAreEqual(clickhouseView, schemaView)
	require.True(t, equal, "INTERVAL 3 HOUR and toIntervalHour(3) should be equal")
}

// TestClusterIgnoredInComparison tests that ON CLUSTER is ignored when comparing views.
// ClickHouse doesn't return ON CLUSTER in create_table_query from system.tables.
func TestClusterIgnoredInComparison(t *testing.T) {
	// Schema has ON CLUSTER
	schemaSQL := `CREATE MATERIALIZED VIEW mydb.my_view ON CLUSTER mycluster
REFRESH EVERY 10 SECONDS
APPEND TO mydb.target
AS SELECT id FROM mydb.source;`

	// ClickHouse output doesn't have ON CLUSTER
	clickhouseSQL := `CREATE MATERIALIZED VIEW mydb.my_view
REFRESH EVERY 10 SECONDS
APPEND TO mydb.target
AS SELECT id FROM mydb.source;`

	schemaParsed, err := parser.ParseString(schemaSQL)
	require.NoError(t, err)
	clickhouseParsed, err := parser.ParseString(clickhouseSQL)
	require.NoError(t, err)

	schemaViews := extractViewsFromSQL(schemaParsed)
	clickhouseViews := extractViewsFromSQL(clickhouseParsed)

	schemaView := schemaViews["mydb.my_view"]
	clickhouseView := clickhouseViews["mydb.my_view"]

	require.NotNil(t, schemaView)
	require.NotNil(t, clickhouseView)

	// Verify clusters are different
	require.Equal(t, "mycluster", schemaView.Cluster)
	require.Equal(t, "", clickhouseView.Cluster)

	// But views should still be considered equal
	equal := viewsAreEqual(clickhouseView, schemaView)
	require.True(t, equal, "views should be equal even with different cluster settings")
}

// TestCompleteClickHouseNormalization tests all ClickHouse normalizations together:
// - Backticks
// - 60 SECOND -> 1 MINUTE
// - INTERVAL -> toIntervalXxx()
// - IN cte -> IN (cte)
// - Missing ON CLUSTER
func TestCompleteClickHouseNormalization(t *testing.T) {
	// User's schema file
	schemaSQL := `CREATE MATERIALIZED VIEW mydb.my_view ON CLUSTER mycluster
REFRESH EVERY 60 SECOND APPEND TO mydb.target
AS
WITH pending AS (
    SELECT DISTINCT id, name FROM mydb.lifecycle WHERE status = 'done'
)
SELECT id, name, 'done' AS status, now64(3, 'UTC') AS ts
FROM mydb.source
WHERE ts < now64(3, 'UTC') - INTERVAL 3 HOUR
  AND (id, name) NOT IN pending
LIMIT 1000;`

	// ClickHouse output after cleanViewStatement (DEFINER and column defs stripped)
	clickhouseSQL := `CREATE MATERIALIZED VIEW mydb.my_view
REFRESH EVERY 1 MINUTE APPEND TO mydb.target
AS WITH pending AS (
    SELECT DISTINCT id, name FROM mydb.lifecycle WHERE status = 'done'
)
SELECT id, name, 'done' AS status, now64(3, 'UTC') AS ts
FROM mydb.source
WHERE (ts < (now64(3, 'UTC') - toIntervalHour(3))) AND ((id, name) NOT IN (pending))
LIMIT 1000;`

	schemaParsed, err := parser.ParseString(schemaSQL)
	require.NoError(t, err, "Failed to parse schema SQL")
	clickhouseParsed, err := parser.ParseString(clickhouseSQL)
	require.NoError(t, err, "Failed to parse ClickHouse SQL")

	schemaViews := extractViewsFromSQL(schemaParsed)
	clickhouseViews := extractViewsFromSQL(clickhouseParsed)

	schemaView := schemaViews["mydb.my_view"]
	clickhouseView := clickhouseViews["mydb.my_view"]

	require.NotNil(t, schemaView)
	require.NotNil(t, clickhouseView)

	t.Run("refresh interval normalization", func(t *testing.T) {
		equal := refreshClausesAreEqual(schemaView.Statement.Refresh, clickhouseView.Statement.Refresh)
		require.True(t, equal, "60 SECOND and 1 MINUTE should be equal")
	})

	t.Run("SELECT with all normalizations", func(t *testing.T) {
		equal := selectClausesAreEqualWithTolerance(schemaView.Statement.AsSelect, clickhouseView.Statement.AsSelect)
		require.True(t, equal, "SELECT statements should match after normalization")
	})

	t.Run("full view comparison", func(t *testing.T) {
		equal := viewsAreEqual(clickhouseView, schemaView)
		require.True(t, equal, "views should be considered equal")
	})
}
