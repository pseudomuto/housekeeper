-- Database operations with various features
CREATE DATABASE analytics ENGINE = Atomic COMMENT 'Analytics database';

CREATE DATABASE IF NOT EXISTS warehouse ON CLUSTER production ENGINE = MaterializedMySQL('localhost:3306', 'warehouse', 'user', 'password') COMMENT 'Data warehouse';

ALTER DATABASE analytics MODIFY COMMENT 'Updated analytics database';

RENAME DATABASE old_analytics TO analytics, temp_warehouse TO warehouse;

DROP DATABASE IF EXISTS legacy_db ON CLUSTER production SYNC;