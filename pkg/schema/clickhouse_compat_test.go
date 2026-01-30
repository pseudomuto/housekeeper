package schema

import (
	"bytes"
	"testing"

	"github.com/pseudomuto/housekeeper/pkg/format"
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

	t.Run("raw_sessions_mv_ws_path_any_vs_nullIf_should_be_detected", func(t *testing.T) {
		// Old: any(ws_path) - empty string stored as-is
		currentSQL := `CREATE MATERIALIZED VIEW perceptor.raw_sessions_mv ON CLUSTER explorations
TO perceptor.raw_sessions_local
AS SELECT domain, session_id, any(ws_path) AS ws_path FROM perceptor.raw_events_local GROUP BY domain, session_id;`

		// New: any(nullIf(ws_path,'')) - empty string normalized to NULL
		targetSQL := `CREATE MATERIALIZED VIEW perceptor.raw_sessions_mv ON CLUSTER explorations
TO perceptor.raw_sessions_local
AS SELECT domain, session_id, any(nullIf(ws_path, '')) AS ws_path FROM perceptor.raw_events_local GROUP BY domain, session_id;`

		currentParsed, err := parser.ParseString(currentSQL)
		require.NoError(t, err)
		targetParsed, err := parser.ParseString(targetSQL)
		require.NoError(t, err)

		views1 := extractViewsFromSQL(currentParsed)
		views2 := extractViewsFromSQL(targetParsed)

		require.Contains(t, views1, "perceptor.raw_sessions_mv")
		require.Contains(t, views2, "perceptor.raw_sessions_mv")
		require.False(t, viewsAreEqual(views1["perceptor.raw_sessions_mv"], views2["perceptor.raw_sessions_mv"]),
			"change from any(ws_path) to any(nullIf(ws_path,'')) should be detected")
	})

	t.Run("session_lifecycle_timeout_signal_mv_where_predicates_should_be_detected", func(t *testing.T) {
		// Old: no time bounds in CTE or main WHERE
		currentSQL := `CREATE MATERIALIZED VIEW perceptor.session_lifecycle_timeout_signal_mv ON CLUSTER explorations
REFRESH EVERY 60 SECOND APPEND TO perceptor.session_lifecycle_local
AS WITH already_ended AS (
    SELECT DISTINCT domain, session_id FROM perceptor.session_lifecycle_local WHERE signal = 'session_end'
)
SELECT domain, session_id, 'session_end' AS signal, now64(3, 'UTC') AS received_at
FROM perceptor.raw_sessions_local
WHERE max_event_received_at < now64(3, 'UTC') - INTERVAL 3 HOUR AND (domain, session_id) NOT IN already_ended
LIMIT 100000;`

		// New: add AND max_event_received_at > now64(...) - INTERVAL 36 HOUR and AND received_at > now() - INTERVAL 2 DAY
		targetSQL := `CREATE MATERIALIZED VIEW perceptor.session_lifecycle_timeout_signal_mv ON CLUSTER explorations
REFRESH EVERY 60 SECOND APPEND TO perceptor.session_lifecycle_local
AS WITH already_ended AS (
    SELECT DISTINCT domain, session_id FROM perceptor.session_lifecycle_local
    WHERE signal = 'session_end' AND received_at > now() - INTERVAL 2 DAY
)
SELECT domain, session_id, 'session_end' AS signal, now64(3, 'UTC') AS received_at
FROM perceptor.raw_sessions_local
WHERE max_event_received_at < now64(3, 'UTC') - INTERVAL 3 HOUR AND max_event_received_at > now64(3, 'UTC') - INTERVAL 36 HOUR AND (domain, session_id) NOT IN already_ended
LIMIT 100000;`

		currentParsed, err := parser.ParseString(currentSQL)
		require.NoError(t, err)
		targetParsed, err := parser.ParseString(targetSQL)
		require.NoError(t, err)

		views1 := extractViewsFromSQL(currentParsed)
		views2 := extractViewsFromSQL(targetParsed)

		require.Contains(t, views1, "perceptor.session_lifecycle_timeout_signal_mv")
		require.Contains(t, views2, "perceptor.session_lifecycle_timeout_signal_mv")
		require.False(t, viewsAreEqual(views1["perceptor.session_lifecycle_timeout_signal_mv"], views2["perceptor.session_lifecycle_timeout_signal_mv"]),
			"addition of WHERE predicates (max_event_received_at > 36h, received_at > 2d) should be detected")
	})
}

// TestGenerateDiff_DetectsMVChanges_Integration tests that GenerateDiff produces view ALTERs
// when current (DB) has old MV definitions and target (schema files) has the updated definitions:
// raw_sessions_mv ws_path: any(ws_path) -> any(nullIf(ws_path,”)); session_lifecycle_timeout_signal_mv:
// two added WHERE predicates (max_event_received_at > 36h, received_at > 2d).
func TestGenerateDiff_DetectsMVChanges_Integration(t *testing.T) {
	// Current = state after applying existing migrations (old MV definitions)
	currentSQL := `CREATE MATERIALIZED VIEW perceptor.raw_sessions_mv ON CLUSTER explorations
TO perceptor.raw_sessions_local
AS SELECT domain, session_id, any(ws_path) AS ws_path FROM perceptor.raw_events_local GROUP BY domain, session_id;

CREATE MATERIALIZED VIEW perceptor.session_lifecycle_timeout_signal_mv ON CLUSTER explorations
REFRESH EVERY 60 SECOND APPEND TO perceptor.session_lifecycle_local
AS WITH already_ended AS (
    SELECT DISTINCT domain, session_id FROM perceptor.session_lifecycle_local WHERE signal = 'session_end'
)
SELECT domain, session_id, 'session_end' AS signal, now64(3, 'UTC') AS received_at
FROM perceptor.raw_sessions_local
WHERE max_event_received_at < now64(3, 'UTC') - INTERVAL 3 HOUR AND (domain, session_id) NOT IN already_ended
LIMIT 100000;`

	// Target = compiled schema with user edits (new MV definitions)
	targetSQL := `CREATE MATERIALIZED VIEW perceptor.raw_sessions_mv ON CLUSTER explorations
TO perceptor.raw_sessions_local
AS SELECT domain, session_id, any(nullIf(ws_path, '')) AS ws_path FROM perceptor.raw_events_local GROUP BY domain, session_id;

CREATE MATERIALIZED VIEW perceptor.session_lifecycle_timeout_signal_mv ON CLUSTER explorations
REFRESH EVERY 60 SECOND APPEND TO perceptor.session_lifecycle_local
AS WITH already_ended AS (
    SELECT DISTINCT domain, session_id FROM perceptor.session_lifecycle_local
    WHERE signal = 'session_end' AND received_at > now() - INTERVAL 2 DAY
)
SELECT domain, session_id, 'session_end' AS signal, now64(3, 'UTC') AS received_at
FROM perceptor.raw_sessions_local
WHERE max_event_received_at < now64(3, 'UTC') - INTERVAL 3 HOUR AND max_event_received_at > now64(3, 'UTC') - INTERVAL 36 HOUR AND (domain, session_id) NOT IN already_ended
LIMIT 100000;`

	currentParsed, err := parser.ParseString(currentSQL)
	require.NoError(t, err)
	targetParsed, err := parser.ParseString(targetSQL)
	require.NoError(t, err)

	diff, err := GenerateDiff(currentParsed, targetParsed)
	require.NoError(t, err, "GenerateDiff should succeed and produce a diff")
	require.NotNil(t, diff, "diff should not be nil")
	require.False(t, len(diff.Statements) == 0, "diff should contain statements")

	var buf bytes.Buffer
	err = format.FormatSQL(&buf, format.Defaults, diff)
	require.NoError(t, err)
	diffStr := buf.String()

	require.Contains(t, diffStr, "raw_sessions_mv", "diff should include raw_sessions_mv (DROP/CREATE)")
	require.Contains(t, diffStr, "session_lifecycle_timeout_signal_mv", "diff should include session_lifecycle_timeout_signal_mv (DROP/CREATE)")
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

// TestIN_cte_vs_IN_cte_WHERE_equal ensures WHERE (a,b) IN (cte) (dump) compares equal to WHERE (a,b) IN cte (schema).
// So finalize_sessions_mv-style views are not incorrectly flagged for alter.
func TestIN_cte_vs_IN_cte_WHERE_equal(t *testing.T) {
	// Schema: IN cte (no parens around CTE name)
	schemaSQL := `CREATE MATERIALIZED VIEW test.mv TO test.target
AS WITH cte AS (SELECT domain, session_id FROM test.t)
SELECT domain, session_id FROM test.source WHERE (domain, session_id) IN cte;`

	// Dump: IN (cte) (ClickHouse wraps CTE ref in parens)
	dumpSQL := `CREATE MATERIALIZED VIEW test.mv TO test.target
AS WITH cte AS (SELECT domain, session_id FROM test.t)
SELECT domain, session_id FROM test.source WHERE (domain, session_id) IN (cte);`

	schemaParsed, err := parser.ParseString(schemaSQL)
	require.NoError(t, err)
	dumpParsed, err := parser.ParseString(dumpSQL)
	require.NoError(t, err)

	viewsSchema := extractViewsFromSQL(schemaParsed)
	viewsDump := extractViewsFromSQL(dumpParsed)
	require.Len(t, viewsSchema, 1)
	require.Len(t, viewsDump, 1)

	equal := viewsAreEqual(viewsDump["test.mv"], viewsSchema["test.mv"])
	require.True(t, equal, "IN (cte) (dump) should compare equal to IN cte (schema)")
}

// TestNoDiffWhenOnlyFormattingDiffers ensures GenerateDiff returns no view diff when current and target
// only differ by formatting (backticks, IN (cte) vs IN cte). So re-running diff does not regenerate.
func TestNoDiffWhenOnlyFormattingDiffers(t *testing.T) {
	// Schema (no backticks, IN cte)
	schemaSQL := `CREATE MATERIALIZED VIEW test.finalize_mv ON CLUSTER explorations
REFRESH EVERY 4 MINUTE APPEND TO test.finalized_local
AS WITH pending AS (SELECT domain, session_id FROM test.lifecycle WHERE received_at > now() - INTERVAL 6 HOUR GROUP BY domain, session_id HAVING max(signal) = 1 LIMIT 350000)
SELECT r.domain, r.session_id, max(r.ws_path) AS ws_path FROM test.raw_local AS r WHERE (r.domain, r.session_id) IN pending GROUP BY r.domain, r.session_id;`

	// Dump (backticks, IN (pending))
	dumpSQL := "CREATE MATERIALIZED VIEW test.finalize_mv ON CLUSTER explorations\n" +
		"REFRESH EVERY 4 MINUTE APPEND TO test.finalized_local\n" +
		"AS WITH pending AS (SELECT domain, session_id FROM test.lifecycle WHERE received_at > now() - INTERVAL 6 HOUR GROUP BY domain, session_id HAVING max(signal) = 1 LIMIT 350000)\n" +
		"SELECT `r`.`domain`, `r`.`session_id`, max(`r`.`ws_path`) AS `ws_path` FROM test.raw_local AS r WHERE (r.domain, r.session_id) IN (pending) GROUP BY r.domain, r.session_id;"

	schemaParsed, err := parser.ParseString(schemaSQL)
	require.NoError(t, err)
	dumpParsed, err := parser.ParseString(dumpSQL)
	require.NoError(t, err)

	_, err = GenerateDiff(dumpParsed, schemaParsed)
	require.ErrorIs(t, err, ErrNoDiff, "dump (backticks, IN (cte)) should match schema so diff:force does not regenerate")
}

// TestAlterOnlyForChangedViews ensures only views that actually changed get alter diffs;
// unchanged views (e.g. finalize_sessions_mv) must not be dropped/recreated.
func TestAlterOnlyForChangedViews(t *testing.T) {
	// Current: 3 MVs. finalize_mv unchanged; raw_mv old ws_path; timeout_mv without extra WHERE predicates.
	currentSQL := `CREATE MATERIALIZED VIEW test.finalize_mv TO test.finalized AS WITH cte AS (SELECT id FROM t) SELECT id, max(ws_path) AS ws_path FROM test.raw GROUP BY id;
CREATE MATERIALIZED VIEW test.raw_mv TO test.raw_local AS SELECT domain, session_id, any(ws_path) AS ws_path FROM test.events GROUP BY domain, session_id;
CREATE MATERIALIZED VIEW test.timeout_mv TO test.lifecycle AS SELECT domain, session_id FROM test.raw WHERE ts < now() - INTERVAL 3 HOUR LIMIT 1000;`

	// Target: same 3 MVs. finalize_mv still unchanged; raw_mv new ws_path; timeout_mv with extra WHERE.
	targetSQL := `CREATE MATERIALIZED VIEW test.finalize_mv TO test.finalized AS WITH cte AS (SELECT id FROM t) SELECT id, max(ws_path) AS ws_path FROM test.raw GROUP BY id;
CREATE MATERIALIZED VIEW test.raw_mv TO test.raw_local AS SELECT domain, session_id, any(nullIf(ws_path, '')) AS ws_path FROM test.events GROUP BY domain, session_id;
CREATE MATERIALIZED VIEW test.timeout_mv TO test.lifecycle AS SELECT domain, session_id FROM test.raw WHERE ts < now() - INTERVAL 3 HOUR AND ts > now() - INTERVAL 36 HOUR LIMIT 1000;`

	currentParsed, err := parser.ParseString(currentSQL)
	require.NoError(t, err)
	targetParsed, err := parser.ParseString(targetSQL)
	require.NoError(t, err)

	diff, err := GenerateDiff(currentParsed, targetParsed)
	require.NoError(t, err)
	require.NotNil(t, diff)

	var buf bytes.Buffer
	err = format.FormatSQL(&buf, format.Defaults, diff)
	require.NoError(t, err)
	diffStr := buf.String()

	// Should alter raw_mv and timeout_mv only
	require.Contains(t, diffStr, "raw_mv", "diff should include raw_mv (changed)")
	require.Contains(t, diffStr, "timeout_mv", "diff should include timeout_mv (changed)")
	// finalize_mv must NOT be in the diff (unchanged)
	require.NotContains(t, diffStr, "finalize_mv", "diff must not drop/recreate finalize_mv (unchanged)")
}
