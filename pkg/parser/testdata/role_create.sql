-- Basic CREATE ROLE
CREATE ROLE admin;

-- CREATE ROLE with IF NOT EXISTS
CREATE ROLE IF NOT EXISTS developer;

-- CREATE ROLE with OR REPLACE
CREATE OR REPLACE ROLE analyst;

-- CREATE ROLE with ON CLUSTER
CREATE ROLE reader ON CLUSTER production;

-- CREATE ROLE with SETTINGS
CREATE ROLE writer SETTINGS max_memory_usage = 10000000000;

-- CREATE ROLE with multiple SETTINGS
CREATE ROLE poweruser SETTINGS max_memory_usage = 10000000000, readonly = 0;

-- CREATE ROLE with all options
CREATE ROLE IF NOT EXISTS superuser ON CLUSTER production SETTINGS max_memory_usage = 10000000000, readonly = 0;

-- CREATE OR REPLACE ROLE with all options
CREATE OR REPLACE ROLE dbadmin ON CLUSTER staging SETTINGS max_threads = 8;