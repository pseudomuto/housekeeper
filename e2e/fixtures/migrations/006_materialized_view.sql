-- Materialized view for real-time analytics
CREATE MATERIALIZED VIEW analytics.daily_stats
ENGINE = MergeTree()
ORDER BY (date, event_type)
POPULATE
AS SELECT
    toDate(timestamp) as date,
    event_type,
    count() as event_count,
    uniq(user_id) as unique_users,
    uniq(session_id) as unique_sessions,
    max(timestamp) as last_event_time
FROM analytics.events
GROUP BY date, event_type;