-- Create logs database
CREATE DATABASE IF NOT EXISTS logs;

-- Application logs table
CREATE TABLE IF NOT EXISTS logs.application_logs (
    timestamp DateTime64(3),
    level Enum8('DEBUG' = 1, 'INFO' = 2, 'WARN' = 3, 'ERROR' = 4),
    service String,
    message String,
    trace_id Nullable(String),
    span_id Nullable(String),
    labels Map(String, String) COMMENT 'Key-value labels for log entry'
)
ENGINE = MergeTree()
PARTITION BY toYYYYMMDD(timestamp)
ORDER BY (timestamp, service, level)
TTL timestamp + INTERVAL 30 DAY
SETTINGS index_granularity = 8192;

-- Error logs materialized view for fast error querying
CREATE VIEW IF NOT EXISTS logs.error_logs AS
SELECT 
    timestamp,
    service,
    message,
    trace_id,
    span_id,
    labels
FROM logs.application_logs
WHERE level = 'ERROR';