-- Verify initial dictionaries exist
SELECT 'user_status_dict' as check_name, count(*) as result
FROM system.dictionaries
WHERE database = 'analytics' AND name = 'user_status_dict'
HAVING result = 1;