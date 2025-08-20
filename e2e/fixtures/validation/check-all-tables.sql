-- Verify all expected tables exist
SELECT 'users_table' as check_name, count(*) as result
FROM system.tables
WHERE database = 'analytics' AND name = 'users'
HAVING result = 1

UNION ALL

SELECT 'events_table' as check_name, count(*) as result
FROM system.tables
WHERE database = 'analytics' AND name = 'events'
HAVING result = 1;