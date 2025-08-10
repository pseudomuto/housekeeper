-- Order items for detailed product information
CREATE TABLE ecommerce.order_items ON CLUSTER demo (
    order_id UInt64,
    product_id UInt64,
    quantity UInt32,
    unit_price Decimal64(2),
    total_price Decimal64(2) MATERIALIZED quantity * unit_price,
    discount_amount Decimal64(2) DEFAULT 0,
    created_at DateTime DEFAULT now()
) ENGINE = MergeTree()
ORDER BY (order_id, product_id)
COMMENT 'Individual items within orders';