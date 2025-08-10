-- Current state: multiple databases with different purposes
CREATE DATABASE old_db ENGINE = Atomic COMMENT 'Old comment';
CREATE DATABASE temp_db ENGINE = Atomic COMMENT 'Temporary database';
CREATE DATABASE new_db ENGINE = Atomic COMMENT 'New database';
-- Target state: only old_db remains with updated comment (temp_db and new_db removed)
CREATE DATABASE old_db ENGINE = Atomic COMMENT 'Updated comment';