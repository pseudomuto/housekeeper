-- Current state: database with old comment
CREATE DATABASE analytics ENGINE = Atomic COMMENT 'Old comment';
-- Target state: same database with updated comment  
CREATE DATABASE analytics ENGINE = Atomic COMMENT 'Updated comment';