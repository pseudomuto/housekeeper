-- Basic DROP ROLE
DROP ROLE admin;

-- DROP ROLE with IF EXISTS
DROP ROLE IF EXISTS developer;

-- DROP ROLE with multiple roles
DROP ROLE analyst, writer, reader;

-- DROP ROLE with ON CLUSTER
DROP ROLE poweruser ON CLUSTER production;

-- DROP ROLE with IF EXISTS and multiple roles
DROP ROLE IF EXISTS admin, developer, tester;

-- DROP ROLE with all options
DROP ROLE IF EXISTS dbadmin, superuser ON CLUSTER staging;