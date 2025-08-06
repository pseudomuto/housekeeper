-- Staging environment schema
CREATE DATABASE staging_db ENGINE = Atomic COMMENT 'Staging database';

CREATE TABLE staging_db.events (
    id UInt64,
    event_type String,
    timestamp DateTime,
    data String
) ENGINE = MergeTree() ORDER BY timestamp;