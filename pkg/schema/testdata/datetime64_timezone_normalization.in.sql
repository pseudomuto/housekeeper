-- Current schema: DateTime64 with UTC timezone (as defined by user)
CREATE TABLE test_table (
    event_occurred_at DateTime64(3, UTC),
    event_received_at DateTime64(3, UTC),
    other_timestamp DateTime64(6, 'America/New_York')
) ENGINE = MergeTree() ORDER BY event_occurred_at;

-- Target state: DateTime64 without timezone (as returned by ClickHouse system.tables)
CREATE TABLE test_table (
    event_occurred_at DateTime64(3),
    event_received_at DateTime64(3), 
    other_timestamp DateTime64(6)
) ENGINE = MergeTree() ORDER BY event_occurred_at;