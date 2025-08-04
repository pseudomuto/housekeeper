-- Basic CREATE DATABASE
CREATE DATABASE basic_db;

-- CREATE DATABASE with IF NOT EXISTS
CREATE DATABASE IF NOT EXISTS conditional_db;

-- CREATE DATABASE with ON CLUSTER
CREATE DATABASE IF NOT EXISTS cluster_db ON CLUSTER my_cluster;

-- CREATE DATABASE with ENGINE
CREATE DATABASE engine_db ENGINE = Atomic;

-- CREATE DATABASE with ENGINE parameters
CREATE DATABASE remote_db ENGINE = MySQL('localhost:3306', 'database', 'user', 'password');

-- CREATE DATABASE with ENGINE and numeric parameters
CREATE DATABASE materialized_db ENGINE = MaterializedMySQL('localhost:3306', 'database', 'user', 'password', 5000);

-- CREATE DATABASE with COMMENT
CREATE DATABASE comment_db COMMENT 'This is a test database';

-- CREATE DATABASE with all options
CREATE DATABASE IF NOT EXISTS full_db ON CLUSTER production ENGINE = Atomic COMMENT 'Full featured database';

-- CREATE DATABASE with backtick identifiers
CREATE DATABASE `user-database` ENGINE = Atomic;
CREATE DATABASE IF NOT EXISTS `order-db` ON CLUSTER `prod-cluster` COMMENT 'Database with special chars';