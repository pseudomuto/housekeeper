-- Basic ATTACH DATABASE
ATTACH DATABASE basic_attach_db;

-- ATTACH DATABASE with IF NOT EXISTS
ATTACH DATABASE IF NOT EXISTS conditional_attach_db;

-- ATTACH DATABASE with ENGINE
ATTACH DATABASE engine_attach_db ENGINE = MySQL('localhost:3306', 'database', 'user', 'password');

-- ATTACH DATABASE with ON CLUSTER
ATTACH DATABASE cluster_attach_db ON CLUSTER production;

-- ATTACH DATABASE with all options
ATTACH DATABASE IF NOT EXISTS full_attach_db ENGINE = Atomic ON CLUSTER production;