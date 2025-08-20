-- Verify all expected dictionaries exist
SELECT 'user_status_dict' as check_name, count(*) as result
FROM system.dictionaries
WHERE database = 'analytics' AND name = 'user_status_dict'
HAVING result = 1

UNION ALL

SELECT 'geo_data_dict' as check_name, count(*) as result
FROM system.dictionaries  
WHERE database = 'analytics' AND name = 'geo_data'
HAVING result = 1;