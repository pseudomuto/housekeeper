-- Basic ALTER DATABASE
ALTER DATABASE basic_alter_db MODIFY COMMENT 'Updated comment';

-- ALTER DATABASE with ON CLUSTER
ALTER DATABASE cluster_alter_db ON CLUSTER production MODIFY COMMENT 'Production database';

-- CREATE then ALTER sequence (testing modification of existing database)
CREATE DATABASE modify_test_db ENGINE = Atomic COMMENT 'Original comment';
ALTER DATABASE modify_test_db MODIFY COMMENT 'Updated comment';

-- ALTER with cluster override (setting cluster on existing database)
CREATE DATABASE cluster_override_db COMMENT 'Original comment';
ALTER DATABASE cluster_override_db ON CLUSTER production MODIFY COMMENT 'Updated with cluster';