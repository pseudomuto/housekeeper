-- Current state: temporary database exists
CREATE DATABASE temp_db ENGINE = Atomic COMMENT 'Temporary database';
-- Target state: database should be removed
-- empty target state