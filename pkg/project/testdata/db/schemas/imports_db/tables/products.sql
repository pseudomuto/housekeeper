-- Products table definition
CREATE TABLE imports_db.products (
    id UInt64,
    name String,
    price Decimal(10,2),
    category String,
    stock UInt32 DEFAULT 0
) ENGINE = MergeTree() ORDER BY id;