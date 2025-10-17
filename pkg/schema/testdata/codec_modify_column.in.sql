-- Current schema without CODEC
CREATE TABLE test_table (
    event_occurred_at DateTime64(3, UTC),
    event_received_at DateTime64(3, UTC)
) ENGINE = MergeTree() ORDER BY event_occurred_at;

-- Target state: schema with CODEC - should generate MODIFY COLUMN with CODEC
CREATE TABLE test_table (
    event_occurred_at DateTime64(3, UTC) CODEC(DoubleDelta),
    event_received_at DateTime64(3, UTC) CODEC(DoubleDelta)
) ENGINE = MergeTree() ORDER BY event_occurred_at;