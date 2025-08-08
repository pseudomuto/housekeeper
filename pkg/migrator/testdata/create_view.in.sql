-- Current state: no views exist
;
-- Target state: create new regular view and materialized view
CREATE VIEW analytics.stats AS SELECT count(*) AS total FROM events;
CREATE MATERIALIZED VIEW analytics.mv_stats ENGINE = MergeTree() ORDER BY date AS SELECT toDate(timestamp) AS date, count() AS cnt FROM events GROUP BY date;