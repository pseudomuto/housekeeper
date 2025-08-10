-- Orders table for e-commerce transactions
CREATE TABLE ecommerce.orders ON CLUSTER demo (
    id UInt64,
    user_id UInt64,
    order_date Date,
    order_timestamp DateTime,
    status LowCardinality(String),
    total_amount Decimal64(2),
    currency LowCardinality(String) DEFAULT 'USD',
    payment_method LowCardinality(String),
    shipping_address_id Nullable(UInt64),
    discount_amount Decimal64(2) DEFAULT 0,
    tax_amount Decimal64(2) DEFAULT 0,
    created_at DateTime DEFAULT now(),
    updated_at DateTime DEFAULT now()
) ENGINE = MergeTree()
ORDER BY (user_id, order_date)
PARTITION BY toYYYYMM(order_date)
TTL order_timestamp + INTERVAL 7 YEAR
COMMENT 'Customer orders';