-- SET ROLE to DEFAULT
SET ROLE DEFAULT;

-- SET ROLE to NONE
SET ROLE NONE;

-- SET ROLE to ALL
SET ROLE ALL;

-- SET ROLE to specific role
SET ROLE admin;

-- SET ROLE to multiple roles
SET ROLE reader, writer;

-- SET DEFAULT ROLE NONE
SET DEFAULT ROLE NONE TO john;

-- SET DEFAULT ROLE ALL
SET DEFAULT ROLE ALL TO alice;

-- SET DEFAULT ROLE specific
SET DEFAULT ROLE reader TO bob;

-- SET DEFAULT ROLE to multiple users
SET DEFAULT ROLE developer TO alice, bob, charlie;