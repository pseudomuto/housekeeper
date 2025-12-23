package schema

import (
	"testing"

	"github.com/pseudomuto/housekeeper/pkg/parser"
	"github.com/stretchr/testify/require"
)

// TestTTLCompatibility tests TTL clause comparison handling ClickHouse-specific formatting
func TestTTLCompatibility(t *testing.T) {
	t.Run("INTERVAL vs toIntervalDay should be equivalent", func(t *testing.T) {
		// Schema uses INTERVAL syntax
		schemaSQL := `CREATE TABLE test.t1 (id Int32, ts DateTime64(3))
ENGINE = MergeTree ORDER BY id
TTL toDateTime(ts) + INTERVAL 7 DAY;`

		// ClickHouse returns toIntervalDay function
		clickhouseSQL := `CREATE TABLE test.t1 (id Int32, ts DateTime64(3))
ENGINE = MergeTree ORDER BY id
TTL toDateTime(ts) + toIntervalDay(7);`

		schemaParsed, err := parser.ParseString(schemaSQL)
		require.NoError(t, err)
		clickhouseParsed, err := parser.ParseString(clickhouseSQL)
		require.NoError(t, err)

		diff, err := GenerateDiff(clickhouseParsed, schemaParsed)
		require.ErrorIs(t, err, ErrNoDiff)
		require.Nil(t, diff)
	})

	t.Run("DELETE keyword is default - no diff expected", func(t *testing.T) {
		// Schema has explicit DELETE
		schemaSQL := `CREATE TABLE test.t1 (id Int32, ts DateTime64(3))
ENGINE = MergeTree ORDER BY id
TTL toDateTime(ts) + INTERVAL 4 DAY DELETE;`

		// ClickHouse omits DELETE (it's the default)
		clickhouseSQL := `CREATE TABLE test.t1 (id Int32, ts DateTime64(3))
ENGINE = MergeTree ORDER BY id
TTL toDateTime(ts) + toIntervalDay(4);`

		schemaParsed, err := parser.ParseString(schemaSQL)
		require.NoError(t, err)
		clickhouseParsed, err := parser.ParseString(clickhouseSQL)
		require.NoError(t, err)

		diff, err := GenerateDiff(clickhouseParsed, schemaParsed)
		require.ErrorIs(t, err, ErrNoDiff)
		require.Nil(t, diff)
	})

	t.Run("different TTL values should be detected", func(t *testing.T) {
		currentSQL := `CREATE TABLE test.t1 (id Int32, ts DateTime64(3))
ENGINE = MergeTree ORDER BY id
TTL toDateTime(ts) + toIntervalDay(7);`

		targetSQL := `CREATE TABLE test.t1 (id Int32, ts DateTime64(3))
ENGINE = MergeTree ORDER BY id
TTL toDateTime(ts) + INTERVAL 4 DAY DELETE;`

		currentParsed, err := parser.ParseString(currentSQL)
		require.NoError(t, err)
		targetParsed, err := parser.ParseString(targetSQL)
		require.NoError(t, err)

		diffs, err := compareTables(currentParsed, targetParsed)
		require.NoError(t, err)
		require.Len(t, diffs, 1, "should detect TTL value change")
	})

	t.Run("DELETE WHERE clause should be detected", func(t *testing.T) {
		currentSQL := `CREATE TABLE test.t1 (id Int32, ts DateTime64(3))
ENGINE = MergeTree ORDER BY id
TTL toDateTime(ts) + INTERVAL 7 DAY;`

		targetSQL := `CREATE TABLE test.t1 (id Int32, ts DateTime64(3))
ENGINE = MergeTree ORDER BY id
TTL toDateTime(ts) + INTERVAL 7 DAY DELETE WHERE id > 100;`

		currentParsed, err := parser.ParseString(currentSQL)
		require.NoError(t, err)
		targetParsed, err := parser.ParseString(targetSQL)
		require.NoError(t, err)

		diffs, err := compareTables(currentParsed, targetParsed)
		require.NoError(t, err)
		require.Len(t, diffs, 1, "should detect DELETE WHERE addition")
	})
}

// TestRefreshIntervalCompatibility tests REFRESH clause comparison
func TestRefreshIntervalCompatibility(t *testing.T) {
	t.Run("SECONDS vs SECOND should be equivalent", func(t *testing.T) {
		schemaSQL := `CREATE MATERIALIZED VIEW test.mv
REFRESH EVERY 10 SECONDS APPEND TO test.target
AS SELECT id FROM test.source;`

		clickhouseSQL := `CREATE MATERIALIZED VIEW test.mv
REFRESH EVERY 10 SECOND APPEND TO test.target
AS SELECT id FROM test.source;`

		schemaParsed, err := parser.ParseString(schemaSQL)
		require.NoError(t, err)
		clickhouseParsed, err := parser.ParseString(clickhouseSQL)
		require.NoError(t, err)

		diff, err := GenerateDiff(clickhouseParsed, schemaParsed)
		require.ErrorIs(t, err, ErrNoDiff)
		require.Nil(t, diff)
	})
}

// TestIntervalInViewQueries tests INTERVAL normalization in view SELECT statements
func TestIntervalInViewQueries(t *testing.T) {
	t.Run("INTERVAL in WHERE should match toInterval function", func(t *testing.T) {
		schemaSQL := `CREATE MATERIALIZED VIEW test.mv
REFRESH EVERY 10 SECOND APPEND TO test.target
AS SELECT id FROM test.source WHERE ts > now() - INTERVAL 1 DAY;`

		clickhouseSQL := `CREATE MATERIALIZED VIEW test.mv
REFRESH EVERY 10 SECOND APPEND TO test.target
AS SELECT id FROM test.source WHERE ts > now() - toIntervalDay(1);`

		schemaParsed, err := parser.ParseString(schemaSQL)
		require.NoError(t, err)
		clickhouseParsed, err := parser.ParseString(clickhouseSQL)
		require.NoError(t, err)

		diff, err := GenerateDiff(clickhouseParsed, schemaParsed)
		require.ErrorIs(t, err, ErrNoDiff)
		require.Nil(t, diff)
	})

	t.Run("extra parentheses in expressions should be equivalent", func(t *testing.T) {
		schemaSQL := `CREATE MATERIALIZED VIEW test.mv
REFRESH EVERY 10 SECOND APPEND TO test.target
AS SELECT id FROM test.source WHERE ts > now() - INTERVAL 1 DAY;`

		// ClickHouse adds parentheses around arithmetic
		clickhouseSQL := `CREATE MATERIALIZED VIEW test.mv
REFRESH EVERY 10 SECOND APPEND TO test.target
AS SELECT id FROM test.source WHERE ts > (now() - toIntervalDay(1));`

		schemaParsed, err := parser.ParseString(schemaSQL)
		require.NoError(t, err)
		clickhouseParsed, err := parser.ParseString(clickhouseSQL)
		require.NoError(t, err)

		diff, err := GenerateDiff(clickhouseParsed, schemaParsed)
		require.ErrorIs(t, err, ErrNoDiff)
		require.Nil(t, diff)
	})
}

// TestCTECompatibility tests CTE (WITH clause) comparison
func TestCTECompatibility(t *testing.T) {
	t.Run("IN cte vs IN (cte) should be equivalent", func(t *testing.T) {
		schemaSQL := `CREATE MATERIALIZED VIEW test.mv
REFRESH EVERY 10 SECOND APPEND TO test.target
AS WITH cte AS (SELECT id FROM t1)
SELECT * FROM t2 WHERE id IN cte;`

		clickhouseSQL := `CREATE MATERIALIZED VIEW test.mv
REFRESH EVERY 10 SECOND APPEND TO test.target
AS WITH cte AS (SELECT id FROM t1)
SELECT * FROM t2 WHERE id IN (cte);`

		schemaParsed, err := parser.ParseString(schemaSQL)
		require.NoError(t, err)
		clickhouseParsed, err := parser.ParseString(clickhouseSQL)
		require.NoError(t, err)

		diff, err := GenerateDiff(clickhouseParsed, schemaParsed)
		require.ErrorIs(t, err, ErrNoDiff)
		require.Nil(t, diff)
	})

	t.Run("CTE WHERE clause change should be detected", func(t *testing.T) {
		currentSQL := `CREATE MATERIALIZED VIEW test.mv AS
WITH cte AS (SELECT id FROM t1)
SELECT * FROM cte;`

		targetSQL := `CREATE MATERIALIZED VIEW test.mv AS
WITH cte AS (SELECT id FROM t1 WHERE x > 1)
SELECT * FROM cte;`

		currentParsed, err := parser.ParseString(currentSQL)
		require.NoError(t, err)
		targetParsed, err := parser.ParseString(targetSQL)
		require.NoError(t, err)

		views1 := extractViewsFromSQL(currentParsed)
		views2 := extractViewsFromSQL(targetParsed)

		require.False(t, viewsAreEqual(views1["test.mv"], views2["test.mv"]))
	})

	t.Run("CTE HAVING clause change should be detected", func(t *testing.T) {
		currentSQL := `CREATE MATERIALIZED VIEW test.mv AS
WITH cte AS (SELECT id FROM t1 GROUP BY id)
SELECT * FROM cte;`

		targetSQL := `CREATE MATERIALIZED VIEW test.mv AS
WITH cte AS (SELECT id FROM t1 GROUP BY id HAVING count(*) > 1)
SELECT * FROM cte;`

		currentParsed, err := parser.ParseString(currentSQL)
		require.NoError(t, err)
		targetParsed, err := parser.ParseString(targetSQL)
		require.NoError(t, err)

		views1 := extractViewsFromSQL(currentParsed)
		views2 := extractViewsFromSQL(targetParsed)

		require.False(t, viewsAreEqual(views1["test.mv"], views2["test.mv"]))
	})
}

// TestSettingsCompatibility tests SETTINGS clause comparison
func TestSettingsCompatibility(t *testing.T) {
	t.Run("extra index_granularity setting should be ignored", func(t *testing.T) {
		schemaSQL := `CREATE TABLE test.t1 (id Int32)
ENGINE = MergeTree ORDER BY id
SETTINGS storage_policy = 'tiered';`

		// ClickHouse adds default index_granularity
		clickhouseSQL := `CREATE TABLE test.t1 (id Int32)
ENGINE = MergeTree ORDER BY id
SETTINGS storage_policy = 'tiered', index_granularity = 8192;`

		schemaParsed, err := parser.ParseString(schemaSQL)
		require.NoError(t, err)
		clickhouseParsed, err := parser.ParseString(clickhouseSQL)
		require.NoError(t, err)

		diff, err := GenerateDiff(clickhouseParsed, schemaParsed)
		require.ErrorIs(t, err, ErrNoDiff)
		require.Nil(t, diff)
	})

	t.Run("view SETTINGS change should be detected", func(t *testing.T) {
		currentSQL := `CREATE MATERIALIZED VIEW test.mv
REFRESH EVERY 1 MINUTE APPEND TO test.target
AS SELECT id FROM test.source;`

		targetSQL := `CREATE MATERIALIZED VIEW test.mv
REFRESH EVERY 1 MINUTE APPEND TO test.target
AS SELECT id FROM test.source
SETTINGS max_memory_usage = 3000000000;`

		currentParsed, err := parser.ParseString(currentSQL)
		require.NoError(t, err)
		targetParsed, err := parser.ParseString(targetSQL)
		require.NoError(t, err)

		views1 := extractViewsFromSQL(currentParsed)
		views2 := extractViewsFromSQL(targetParsed)

		require.False(t, viewsAreEqual(views1["test.mv"], views2["test.mv"]))
	})
}

// TestViewQueryChanges tests that meaningful query changes are detected
func TestViewQueryChanges(t *testing.T) {
	t.Run("LIMIT change should be detected", func(t *testing.T) {
		currentSQL := `CREATE MATERIALIZED VIEW test.mv
REFRESH EVERY 1 MINUTE APPEND TO test.target
AS SELECT id FROM test.source LIMIT 100000;`

		targetSQL := `CREATE MATERIALIZED VIEW test.mv
REFRESH EVERY 1 MINUTE APPEND TO test.target
AS SELECT id FROM test.source LIMIT 500000;`

		currentParsed, err := parser.ParseString(currentSQL)
		require.NoError(t, err)
		targetParsed, err := parser.ParseString(targetSQL)
		require.NoError(t, err)

		views1 := extractViewsFromSQL(currentParsed)
		views2 := extractViewsFromSQL(targetParsed)

		require.False(t, viewsAreEqual(views1["test.mv"], views2["test.mv"]))
	})

	t.Run("UNION ALL addition should be detected", func(t *testing.T) {
		currentSQL := `CREATE MATERIALIZED VIEW test.mv
REFRESH EVERY 1 MINUTE APPEND TO test.target
AS SELECT id FROM test.source;`

		targetSQL := `CREATE MATERIALIZED VIEW test.mv
REFRESH EVERY 1 MINUTE APPEND TO test.target
AS SELECT id FROM (
    SELECT id FROM test.source1
    UNION ALL
    SELECT id FROM test.source2
);`

		currentParsed, err := parser.ParseString(currentSQL)
		require.NoError(t, err)
		targetParsed, err := parser.ParseString(targetSQL)
		require.NoError(t, err)

		views1 := extractViewsFromSQL(currentParsed)
		views2 := extractViewsFromSQL(targetParsed)

		require.False(t, viewsAreEqual(views1["test.mv"], views2["test.mv"]))
	})
}

// TestTableRoundTrip tests that a table is stable after migration
func TestTableRoundTrip(t *testing.T) {
	t.Run("table should be stable after migration", func(t *testing.T) {
		// What migration generates
		migrationSQL := `CREATE TABLE test.t1 (
    id UUID,
    name String,
    ts DateTime64(3, 'UTC') DEFAULT now64(3, 'UTC')
)
ENGINE = MergeTree
PARTITION BY toStartOfDay(ts)
ORDER BY id
TTL toDateTime(ts) + INTERVAL 4 DAY DELETE
SETTINGS storage_policy = 'tiered';`

		// What ClickHouse returns
		clickhouseSQL := `CREATE TABLE test.t1 (
    id UUID,
    name String,
    ts DateTime64(3, 'UTC') DEFAULT now64(3, 'UTC')
)
ENGINE = MergeTree
PARTITION BY toStartOfDay(ts)
ORDER BY id
TTL toDateTime(ts) + toIntervalDay(4)
SETTINGS storage_policy = 'tiered', index_granularity = 8192;`

		migrationParsed, err := parser.ParseString(migrationSQL)
		require.NoError(t, err)
		clickhouseParsed, err := parser.ParseString(clickhouseSQL)
		require.NoError(t, err)

		diff, err := GenerateDiff(clickhouseParsed, migrationParsed)
		require.ErrorIs(t, err, ErrNoDiff, "table should be stable after migration")
		require.Nil(t, diff)
	})
}

// TestViewRoundTrip tests that a view is stable after migration
func TestViewRoundTrip(t *testing.T) {
	t.Run("view with CTE should be stable after migration", func(t *testing.T) {
		// What migration generates
		migrationSQL := `CREATE MATERIALIZED VIEW test.mv
REFRESH EVERY 10 SECONDS APPEND TO test.target
AS WITH cte AS (
    SELECT id FROM test.source
    WHERE ts > now() - INTERVAL 1 DAY
    GROUP BY id
    HAVING count(*) > 1
    LIMIT 100000
)
SELECT id FROM test.data WHERE id IN cte;`

		// What ClickHouse returns
		clickhouseSQL := `CREATE MATERIALIZED VIEW test.mv
REFRESH EVERY 10 SECOND APPEND TO test.target
AS WITH cte AS (
    SELECT id FROM test.source
    WHERE ts > (now() - toIntervalDay(1))
    GROUP BY id
    HAVING count(*) > 1
    LIMIT 100000
)
SELECT id FROM test.data WHERE id IN (cte);`

		migrationParsed, err := parser.ParseString(migrationSQL)
		require.NoError(t, err)
		clickhouseParsed, err := parser.ParseString(clickhouseSQL)
		require.NoError(t, err)

		diff, err := GenerateDiff(clickhouseParsed, migrationParsed)
		require.ErrorIs(t, err, ErrNoDiff, "view should be stable after migration")
		require.Nil(t, diff)
	})
}
