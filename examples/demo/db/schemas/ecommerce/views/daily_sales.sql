-- Daily sales summary view
CREATE VIEW ecommerce.daily_sales ON CLUSTER demo AS
SELECT 
    order_date,
    count() as total_orders,
    sum(total_amount) as total_revenue,
    avg(total_amount) as avg_order_value,
    uniq(user_id) as unique_customers
FROM ecommerce.orders
WHERE status = 'completed'
GROUP BY order_date
ORDER BY order_date DESC;