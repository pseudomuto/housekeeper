-- Verify post-snapshot migration and mixed revision types
SELECT 'active_users_view' as check_name, count(*) as result
FROM system.tables
WHERE database = 'analytics' AND name = 'active_users'
HAVING result = 1

UNION ALL

SELECT 'snapshot_revision_exists' as check_name, count(*) as result
FROM housekeeper.revisions
WHERE kind = 'snapshot'
HAVING result = 1

UNION ALL

SELECT 'post_snapshot_revision' as check_name, count(*) as result
FROM housekeeper.revisions
WHERE version = '007_post_snapshot' AND kind = 'migration'
HAVING result = 1;