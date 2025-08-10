-- User activity summary view
CREATE VIEW ecommerce.user_activity ON CLUSTER demo AS
SELECT 
    u.id,
    u.email,
    u.country,
    u.total_orders,
    u.total_spent,
    last_event.last_activity,
    CASE 
        WHEN last_event.last_activity >= today() - 7 THEN 'Active'
        WHEN last_event.last_activity >= today() - 30 THEN 'Recent'
        ELSE 'Inactive'
    END as activity_status
FROM ecommerce.users u
LEFT JOIN (
    SELECT 
        user_id,
        max(timestamp) as last_activity
    FROM ecommerce.events
    GROUP BY user_id
) last_event ON u.id = last_event.user_id;