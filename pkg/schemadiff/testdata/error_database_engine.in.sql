-- Current state: database with Atomic engine
CREATE DATABASE analytics ENGINE = Atomic COMMENT 'Analytics database';
-- Target state: attempting to change engine to Memory (should fail)
CREATE DATABASE analytics ENGINE = Memory COMMENT 'Analytics database';