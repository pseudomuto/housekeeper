-- Verify initial migration revisions
SELECT 'initial_revisions_count' as check_name, count(*) as result
FROM housekeeper.revisions
WHERE version IN ('001_initial', '002_users_table', '003_basic_dictionary')
HAVING result = 3

UNION ALL

SELECT 'all_initial_completed' as check_name, count(*) as result
FROM housekeeper.revisions
WHERE version IN ('001_initial', '002_users_table', '003_basic_dictionary')
  AND error IS NULL
HAVING result = 3;