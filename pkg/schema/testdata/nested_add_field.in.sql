-- Current state: table with existing nested fields
CREATE TABLE users (
    id UInt64,
    `profile.name` Array(String)
) ENGINE = MergeTree() ORDER BY id;
-- Target state: add new field to nested structure
CREATE TABLE users (
    id UInt64,
    profile Nested(
        name String,
        email String
    )
) ENGINE = MergeTree() ORDER BY id;