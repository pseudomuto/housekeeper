-- Verify all evolution migration revisions
SELECT 'evolution_revisions_count' as check_name, count(*) as result
FROM housekeeper.revisions
WHERE version IN ('004_events_table', '005_complex_dictionary', '006_materialized_view')
HAVING result = 3

UNION ALL

SELECT 'all_evolution_completed' as check_name, count(*) as result
FROM housekeeper.revisions
WHERE version IN ('004_events_table', '005_complex_dictionary', '006_materialized_view')
  AND error IS NULL
HAVING result = 3

UNION ALL

SELECT 'total_standard_revisions' as check_name, count(*) as result
FROM housekeeper.revisions
WHERE kind = 'migration'
HAVING result = 6;