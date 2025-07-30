-- Example ClickHouse view definitions for Housekeeper
-- This file demonstrates various view types and configurations

-- Basic view for daily event summaries
CREATE VIEW analytics.daily_summary 
AS SELECT 
    toDate(timestamp) as date, 
    count(*) as total_events,
    uniq(user_id) as unique_users
FROM events 
GROUP BY date;

-- Materialized view with MergeTree engine for performance
CREATE MATERIALIZED VIEW analytics.mv_daily_stats
ENGINE = MergeTree() 
ORDER BY date
POPULATE  -- Populate with existing data
AS SELECT 
    toDate(timestamp) as date, 
    count() as event_count,
    uniq(user_id) as unique_users,
    countIf(event_type = 'purchase') as purchases,
    sumIf(amount, event_type = 'purchase') as revenue
FROM events 
GROUP BY date;

-- Materialized view with TO table clause
CREATE MATERIALIZED VIEW analytics.mv_user_stats
TO analytics.user_statistics  -- Insert into existing table
AS SELECT 
    user_id,
    count() as total_events,
    max(timestamp) as last_activity,
    sum(amount) as total_spent
FROM events
GROUP BY user_id;

-- Clustered materialized view for distributed setup
CREATE MATERIALIZED VIEW logs.mv_error_summary ON CLUSTER production
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/error_summary', '{replica}')
ORDER BY (date, service)
AS SELECT 
    toDate(timestamp) as date,
    service,
    level,
    count() as error_count,
    uniq(message) as unique_errors
FROM logs.errors
WHERE level IN ('ERROR', 'FATAL')
GROUP BY date, service, level;

-- View with OR REPLACE for easy updates
CREATE OR REPLACE VIEW analytics.current_month_stats
AS SELECT 
    toDate(timestamp) as date,
    count() as events,
    uniq(user_id) as users
FROM events
WHERE timestamp >= toStartOfMonth(now())
GROUP BY date
ORDER BY date;