-- Basic REVOKE role from user
REVOKE admin FROM john;

-- REVOKE multiple roles
REVOKE reader, writer FROM alice, bob;