-- Current state: database without cluster configuration
CREATE DATABASE analytics ENGINE = Atomic COMMENT 'Analytics database'
;
-- Target state: attempting to add cluster configuration (should fail)
CREATE DATABASE analytics ON CLUSTER production ENGINE = Atomic COMMENT 'Analytics database';