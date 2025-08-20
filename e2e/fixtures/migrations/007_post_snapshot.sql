-- Post-snapshot migration to test snapshot + migration workflow
-- Add index to users table
ALTER TABLE analytics.users ADD INDEX idx_email email TYPE minmax GRANULARITY 4;

-- Create simple view
CREATE VIEW analytics.active_users AS
SELECT
    id,
    email,
    name,
    created_at
FROM analytics.users
WHERE is_active = 1 AND created_at >= now() - INTERVAL 30 DAY;