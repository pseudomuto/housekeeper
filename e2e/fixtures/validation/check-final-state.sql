-- Comprehensive final state validation
SELECT 'total_databases' as check_name, count(*) as result
FROM system.databases 
WHERE name IN ('housekeeper', 'analytics')
HAVING result = 2

UNION ALL

SELECT 'total_tables' as check_name, count(*) as result
FROM system.tables
WHERE (database = 'housekeeper' AND name = 'revisions')
   OR (database = 'analytics' AND name IN ('users', 'events', 'countries', 'daily_stats', 'active_users'))
HAVING result = 6

UNION ALL

SELECT 'total_dictionaries' as check_name, count(*) as result
FROM system.dictionaries
WHERE database = 'analytics' AND name IN ('user_status_dict', 'geo_data')
HAVING result = 2

UNION ALL

SELECT 'materialized_view_exists' as check_name, count(*) as result
FROM system.tables
WHERE database = 'analytics' 
  AND name = 'daily_stats' 
  AND engine LIKE '%MaterializedView%'
HAVING result = 1

UNION ALL

SELECT 'regular_view_exists' as check_name, count(*) as result
FROM system.tables
WHERE database = 'analytics'
  AND name = 'active_users'
  AND engine = 'View'
HAVING result = 1

UNION ALL

SELECT 'total_revisions' as check_name, count(*) as result
FROM housekeeper.revisions
HAVING result >= 2  -- At least snapshot + post-snapshot

UNION ALL

SELECT 'all_revisions_successful' as check_name, count(*) as result
FROM housekeeper.revisions
WHERE error IS NULL
HAVING result >= 2;