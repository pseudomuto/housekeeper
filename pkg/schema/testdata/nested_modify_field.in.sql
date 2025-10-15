-- Current state: table with nested fields
CREATE TABLE users (
    id UInt64,
    `profile.name` Array(String),
    `profile.age` Array(UInt8)
) ENGINE = MergeTree() ORDER BY id;
-- Target state: modify nested field type
CREATE TABLE users (
    id UInt64,
    profile Nested(
        name String,
        age UInt16
    )
) ENGINE = MergeTree() ORDER BY id;