-- Verify materialized view exists
SELECT 'daily_stats_mv' as check_name, count(*) as result
FROM system.tables
WHERE database = 'analytics' 
  AND name = 'daily_stats' 
  AND engine LIKE '%MaterializedView%'
HAVING result = 1;