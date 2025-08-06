-- Simple main schema file
CREATE DATABASE test_db ENGINE = Atomic COMMENT 'Test database';

CREATE TABLE test_db.users (
    id UInt64,
    name String,
    email String
) ENGINE = MergeTree() ORDER BY id;

CREATE TABLE test_db.orders (
    id UInt64,
    user_id UInt64,
    amount Decimal(10,2),
    created_at DateTime DEFAULT now()
) ENGINE = MergeTree() ORDER BY id;