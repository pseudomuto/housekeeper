-- Top products by category view
CREATE VIEW ecommerce.top_products ON CLUSTER demo AS
WITH product_metrics AS (
    SELECT 
        p.id,
        p.name,
        p.category,
        p.brand,
        p.price,
        coalesce(stats.total_quantity, 0) as total_sold,
        coalesce(stats.total_revenue, 0) as revenue
    FROM ecommerce.products p
    LEFT JOIN (
        SELECT 
            product_id,
            sum(quantity) as total_quantity,
            sum(total_price) as total_revenue
        FROM ecommerce.order_items oi
        JOIN ecommerce.orders o ON oi.order_id = o.id
        WHERE o.status = 'completed'
          AND o.order_date >= today() - 90
        GROUP BY product_id
    ) stats ON p.id = stats.product_id
)
SELECT 
    category,
    id as product_id,
    name as product_name,
    brand,
    price,
    total_sold,
    revenue,
    row_number() OVER (PARTITION BY category ORDER BY revenue DESC) as rank_in_category
FROM product_metrics
WHERE total_sold > 0
ORDER BY category, rank_in_category;