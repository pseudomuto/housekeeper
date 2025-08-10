-- Product performance materialized view
CREATE MATERIALIZED VIEW ecommerce.mv_product_stats ON CLUSTER demo
ENGINE = MergeTree()
ORDER BY (product_id, order_date)
POPULATE
AS SELECT 
    oi.product_id,
    o.order_date,
    count() as orders_count,
    sum(oi.quantity) as total_quantity,
    sum(oi.total_price) as total_revenue,
    avg(oi.unit_price) as avg_price
FROM ecommerce.order_items oi
JOIN ecommerce.orders o ON oi.order_id = o.id
WHERE o.status = 'completed'
GROUP BY oi.product_id, o.order_date;