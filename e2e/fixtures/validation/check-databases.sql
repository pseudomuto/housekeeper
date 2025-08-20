-- Verify required databases exist
SELECT 'analytics_database' as check_name, count(*) as result
FROM system.databases 
WHERE name = 'analytics'
HAVING result = 1;