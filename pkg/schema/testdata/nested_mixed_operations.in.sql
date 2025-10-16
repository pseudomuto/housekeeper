-- Current state: table with basic nested field
CREATE TABLE events (
    id UInt64,
    `metadata.source` Array(String)
) ENGINE = MergeTree() ORDER BY id;
-- Target state: add regular column and modify nested structure
CREATE TABLE events (
    id UInt64,
    created_at DateTime DEFAULT now(),
    metadata Nested(
        source String,
        version UInt16,
        tags Array(String)
    )
) ENGINE = MergeTree() ORDER BY id;