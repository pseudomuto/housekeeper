-- Products table creation
CREATE TABLE test.products (
    id UInt64,
    name String,
    price Decimal(10, 2),
    created_at DateTime DEFAULT now()
) ENGINE = MergeTree() 
ORDER BY id;