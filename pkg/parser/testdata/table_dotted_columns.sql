-- Test tables with dotted column names (flattened Nested representation)
CREATE TABLE test_flattened (
    id UInt64,
    `profile.name` Array(String),
    `profile.age` Array(UInt8),
    `metadata.tags` Array(String),
    `metadata.source` Array(String)
) ENGINE = MergeTree()
ORDER BY id;

-- Mixed regular and dotted columns
CREATE TABLE mixed_columns (
    user_id UInt64,
    `settings.key` Array(String),
    `settings.value` Array(String),
    regular_field String,
    `attributes.nested.deep` Array(UInt32)
) ENGINE = Memory;