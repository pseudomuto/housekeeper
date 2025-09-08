-- Basic ALTER ROLE
ALTER ROLE admin RENAME TO administrator;

-- ALTER ROLE with IF EXISTS
ALTER ROLE IF EXISTS developer RENAME TO dev;

-- ALTER ROLE with ON CLUSTER
ALTER ROLE analyst ON CLUSTER production RENAME TO data_analyst;

-- ALTER ROLE with SETTINGS
ALTER ROLE reader SETTINGS max_memory_usage = 5000000000;

-- ALTER ROLE with multiple SETTINGS
ALTER ROLE writer SETTINGS max_memory_usage = 10000000000, readonly = 0;

-- ALTER ROLE with RENAME and SETTINGS
ALTER ROLE poweruser RENAME TO super_user SETTINGS max_threads = 4;

-- ALTER ROLE with all options
ALTER ROLE IF EXISTS dbadmin ON CLUSTER staging RENAME TO database_admin SETTINGS max_memory_usage = 20000000000;