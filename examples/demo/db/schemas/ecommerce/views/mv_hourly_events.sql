-- Real-time events materialized view
CREATE MATERIALIZED VIEW ecommerce.mv_hourly_events ON CLUSTER demo
ENGINE = MergeTree()
ORDER BY (event_hour, event_type)
AS SELECT 
    toStartOfHour(timestamp) as event_hour,
    event_type,
    country,
    count() as event_count,
    uniq(user_id) as unique_users,
    uniq(session_id) as unique_sessions
FROM ecommerce.events
GROUP BY event_hour, event_type, country;