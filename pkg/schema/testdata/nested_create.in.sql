-- Current state: no table exists
;
-- Target state: create new table with Nested column
CREATE TABLE events (
    id UInt64,
    metadata Nested(
        source String,
        tags Array(String)
    )
) ENGINE = MergeTree() ORDER BY id;