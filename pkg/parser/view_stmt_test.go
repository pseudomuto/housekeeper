package parser_test

import "testing"

func TestCreateView(t *testing.T) {
	t.Parallel()

	tests := []statementTest{
		{name: "basic", sql: `CREATE VIEW analytics.daily_summary AS SELECT date, count(*) AS total FROM events GROUP BY date;`},
		{name: "if_not_exists", sql: `CREATE VIEW IF NOT EXISTS users_view AS SELECT id, name FROM users WHERE active = 1;`},
		{name: "on_cluster", sql: `CREATE VIEW stats_view ON CLUSTER production AS SELECT * FROM statistics;`},
		{name: "or_replace", sql: `CREATE OR REPLACE VIEW analytics.updated_view AS SELECT id, name, updated_at FROM users ORDER BY updated_at DESC;`},
		{name: "with_backticks", sql: "CREATE VIEW `analytics-db`.`daily-summary` AS SELECT `order-date` AS `date`, count(*) AS `total-orders` FROM `orders-table` GROUP BY `order-date`;"},
		{name: "with_window_functions", sql: `CREATE VIEW analytics.user_rankings AS SELECT user_id, name, score, row_number() OVER (ORDER BY score DESC) AS rank, rank() OVER (PARTITION BY category ORDER BY score DESC) AS category_rank FROM user_scores ORDER BY score DESC;`},
	}

	runStatementTests(t, "view/create", tests)
}

func TestCreateMaterializedView(t *testing.T) {
	t.Parallel()

	tests := []statementTest{
		{name: "basic", sql: `CREATE MATERIALIZED VIEW mv_daily_stats AS SELECT toDate(timestamp) AS date, count() AS cnt FROM events GROUP BY date;`},
		{name: "with_engine", sql: `CREATE MATERIALIZED VIEW analytics.mv_aggregated ENGINE = MergeTree() ORDER BY (date, user_id) AS SELECT toDate(timestamp) AS date, user_id, count() AS events_count FROM events GROUP BY date, user_id;`},
		{name: "to_table", sql: `CREATE MATERIALIZED VIEW mv_to_table TO analytics.target_table AS SELECT * FROM source_table WHERE status = 'active';`},
		{name: "populate", sql: `CREATE MATERIALIZED VIEW mv_with_populate ENGINE = AggregatingMergeTree() ORDER BY date POPULATE AS SELECT toDate(timestamp) AS date, sum(amount) AS total FROM transactions GROUP BY date;`},
		{name: "full_options", sql: `CREATE OR REPLACE MATERIALIZED VIEW IF NOT EXISTS analytics.mv_complex ON CLUSTER production TO analytics.destination_table ENGINE = ReplacingMergeTree(version) POPULATE AS SELECT id, name, max(version) AS version, argMax(data, version) AS data FROM source GROUP BY id, name;`},
		{name: "with_joins", sql: `CREATE MATERIALIZED VIEW analytics.mv_joins ENGINE = MergeTree() ORDER BY (date, category) AS SELECT toDate(e.timestamp) AS date, e.user_id, u.name AS user_name, e.category, count() AS event_count, sum(e.value) AS total_value FROM events AS e LEFT JOIN users AS u ON e.user_id = u.id WHERE e.status = 'completed' GROUP BY date, e.user_id, u.name, e.category;`},
		{name: "with_states", sql: `CREATE MATERIALIZED VIEW metrics.mv_user_stats_state TO metrics.user_stats_aggregated AS SELECT toDate(timestamp) AS date, user_id, sumState(amount) AS total_amount_state, avgState(duration) AS avg_duration_state, uniqState(session_id) AS unique_sessions_state FROM raw_events GROUP BY date, user_id;`},
		// Refreshable materialized views
		{name: "refresh_every_seconds", sql: `CREATE MATERIALIZED VIEW mv_refresh REFRESH EVERY 10 SECONDS AS SELECT count() AS cnt FROM events;`},
		{name: "refresh_every_minutes", sql: `CREATE MATERIALIZED VIEW mv_refresh_min REFRESH EVERY 5 MINUTES AS SELECT count() AS cnt FROM events;`},
		{name: "refresh_after_hours", sql: `CREATE MATERIALIZED VIEW mv_refresh_after REFRESH AFTER 1 HOUR AS SELECT count() AS cnt FROM events;`},
		{name: "refresh_append_to", sql: `CREATE MATERIALIZED VIEW mv_refresh_append REFRESH EVERY 10 SECONDS APPEND TO target_table AS SELECT * FROM source;`},
		{name: "refresh_on_cluster", sql: `CREATE MATERIALIZED VIEW db.mv_refresh ON CLUSTER mycluster REFRESH EVERY 30 SECONDS APPEND TO db.target AS SELECT id, name FROM source;`},
		{name: "refresh_with_offset", sql: `CREATE MATERIALIZED VIEW mv_offset REFRESH EVERY 1 DAY OFFSET 6 HOURS AS SELECT toDate(ts) AS dt, count() FROM events GROUP BY dt;`},
		{name: "refresh_with_cte", sql: `CREATE MATERIALIZED VIEW mv_cte REFRESH EVERY 10 SECONDS APPEND TO target AS WITH pending AS (SELECT id FROM lifecycle GROUP BY id HAVING max(signal) = 1) SELECT id, min(ts) AS min_ts FROM raw WHERE id IN pending GROUP BY id;`},
	}

	runStatementTests(t, "view/create_materialized", tests)
}

func TestAttachView(t *testing.T) {
	t.Parallel()

	tests := []statementTest{
		{name: "basic", sql: `ATTACH VIEW analytics.daily_summary;`},
		{name: "if_not_exists", sql: `ATTACH VIEW IF NOT EXISTS users_view;`},
		{name: "on_cluster", sql: `ATTACH VIEW stats_view ON CLUSTER production;`},
		// Materialized views use ATTACH TABLE
		{name: "table_basic", sql: `ATTACH TABLE mv_daily_stats;`},
		{name: "table_if_not_exists", sql: `ATTACH TABLE IF NOT EXISTS analytics.mv_aggregated;`},
		{name: "table_on_cluster", sql: `ATTACH TABLE mv_complex ON CLUSTER production;`},
	}

	runStatementTests(t, "view/attach", tests)
}

func TestDetachView(t *testing.T) {
	t.Parallel()

	tests := []statementTest{
		{name: "basic", sql: `DETACH VIEW analytics.daily_summary;`},
		{name: "if_exists", sql: `DETACH VIEW IF EXISTS users_view;`},
		{name: "permanently", sql: `DETACH VIEW db.my_view PERMANENTLY;`},
		{name: "sync", sql: `DETACH VIEW analytics.updated_view SYNC;`},
		{name: "full_options", sql: `DETACH VIEW IF EXISTS old_view ON CLUSTER production PERMANENTLY SYNC;`},
		// Materialized views use DETACH TABLE
		{name: "table_basic", sql: `DETACH TABLE mv_daily_stats;`},
		{name: "table_permanently_sync", sql: `DETACH TABLE analytics.mv_joins PERMANENTLY SYNC;`},
		{name: "table_full_options", sql: `DETACH TABLE IF EXISTS analytics.mv_old ON CLUSTER analytics_cluster PERMANENTLY SYNC;`},
	}

	runStatementTests(t, "view/detach", tests)
}

func TestDropView(t *testing.T) {
	t.Parallel()

	tests := []statementTest{
		{name: "basic", sql: `DROP VIEW analytics.daily_summary;`},
		{name: "if_exists", sql: `DROP VIEW IF EXISTS users_view;`},
		{name: "on_cluster", sql: `DROP VIEW stats_view ON CLUSTER production;`},
		{name: "sync", sql: `DROP VIEW db.my_view SYNC;`},
		{name: "full_options", sql: `DROP VIEW IF EXISTS old_view ON CLUSTER production SYNC;`},
		// Materialized views use DROP TABLE
		{name: "table_basic", sql: `DROP TABLE mv_daily_stats;`},
		{name: "table_if_exists", sql: `DROP TABLE IF EXISTS analytics.mv_aggregated;`},
		{name: "table_on_cluster", sql: `DROP TABLE mv_complex ON CLUSTER production;`},
		{name: "table_full_options", sql: `DROP TABLE IF EXISTS analytics.mv_old ON CLUSTER analytics_cluster SYNC;`},
	}

	runStatementTests(t, "view/drop", tests)
}
