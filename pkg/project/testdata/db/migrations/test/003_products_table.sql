CREATE TABLE test_db.products (
    id UInt64,
    name String,
    price Decimal64(2),
    created_at DateTime DEFAULT now()
) ENGINE = MergeTree() ORDER BY id;