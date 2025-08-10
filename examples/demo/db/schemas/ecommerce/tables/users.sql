-- Users table
CREATE TABLE ecommerce.users ON CLUSTER demo (
    id UInt64,
    email String,
    first_name String,
    last_name String,
    date_of_birth Nullable(Date),
    registration_date Date,
    country LowCardinality(String),
    city String DEFAULT '',
    is_premium Bool DEFAULT false,
    total_orders UInt32 DEFAULT 0,
    total_spent Decimal64(2) DEFAULT 0,
    last_login_at Nullable(DateTime),
    created_at DateTime DEFAULT now(),
    updated_at DateTime DEFAULT now()
) ENGINE = ReplacingMergeTree(updated_at)
ORDER BY id
COMMENT 'User profiles with aggregate metrics';