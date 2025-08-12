-- Current state: database with old name
CREATE DATABASE old_analytics ENGINE = Atomic COMMENT 'Analytics database';
-- Target state: same database with new name
CREATE DATABASE analytics ENGINE = Atomic COMMENT 'Analytics database';