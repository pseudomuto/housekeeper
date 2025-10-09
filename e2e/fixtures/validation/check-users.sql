-- Validate that test users exist in the system
SELECT
    'users_check' as check_name, count(*) as result
FROM system.users
WHERE name IN ('test_reader', 'test_writer')
HAVING result = 2
