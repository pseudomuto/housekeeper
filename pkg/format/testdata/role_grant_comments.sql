-- Role and permission management
-- Create admin role

CREATE ROLE `admin`;

-- Create read-only role
-- This role has limited permissions

CREATE ROLE `readonly` SETTINGS `readonly` = 1;

-- Grant SELECT to readonly
-- Allow reading from analytics database

GRANT `SELECT` ON `analytics`.* TO `readonly`;

-- Grant all privileges to admin

GRANT ALL ON *.* TO `admin` WITH GRANT OPTION;

-- Revoke DELETE from readonly

REVOKE `DELETE` ON `analytics`.`users` FROM `readonly`;

-- Drop old role

DROP ROLE IF EXISTS `deprecated_role`;
