-- Verify housekeeper database and revisions table exist
SELECT 'housekeeper_database' as check_name, count(*) as result
FROM system.databases 
WHERE name = 'housekeeper'
HAVING result = 1

UNION ALL

SELECT 'revisions_table' as check_name, count(*) as result  
FROM system.tables
WHERE database = 'housekeeper' AND name = 'revisions'  
HAVING result = 1;