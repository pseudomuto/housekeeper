CREATE TABLE test_db.users (
    id UInt64,
    name String,
    created_at DateTime DEFAULT now()
) ENGINE = MergeTree() ORDER BY id;