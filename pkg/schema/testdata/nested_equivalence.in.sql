-- Current state: ClickHouse flattened representation
CREATE TABLE users (
    id UInt64,
    `profile.name` Array(String),
    `profile.age` Array(UInt8)
) ENGINE = MergeTree() ORDER BY id;
-- Target state: User's Nested syntax
CREATE TABLE users (
    id UInt64,
    profile Nested(
        name String,
        age UInt8
    )
) ENGINE = MergeTree() ORDER BY id;