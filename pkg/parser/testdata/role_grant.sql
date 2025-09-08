-- Basic GRANT role to user  
GRANT admin TO john;

-- GRANT multiple roles
GRANT reader, writer TO alice, bob;

-- GRANT with WITH ADMIN OPTION
GRANT developer TO lead WITH ADMIN OPTION;