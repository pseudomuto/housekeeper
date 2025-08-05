-- View operations with SELECT statements
CREATE VIEW analytics.daily_summary AS SELECT toDate(timestamp) AS date, count() AS total_events, uniq(user_id) AS unique_users FROM analytics.events GROUP BY date ORDER BY date;

CREATE OR REPLACE MATERIALIZED VIEW analytics.mv_user_stats 
ENGINE = SummingMergeTree() 
ORDER BY (user_id, date) 
PARTITION BY toYYYYMM(date)
POPULATE
AS SELECT user_id, toDate(timestamp) AS date, count() AS event_count, countIf(event_type = 'purchase') AS purchase_count FROM analytics.events GROUP BY user_id, date;

CREATE MATERIALIZED VIEW analytics.mv_hourly_metrics 
ON CLUSTER production
TO analytics.hourly_data
AS SELECT toStartOfHour(timestamp) AS hour, event_type, count() AS cnt FROM analytics.events GROUP BY hour, event_type;