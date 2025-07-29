-- Basic DROP DATABASE
DROP DATABASE basic_drop_db;

-- DROP DATABASE with IF EXISTS
DROP DATABASE IF EXISTS conditional_drop_db;

-- DROP DATABASE with ON CLUSTER
DROP DATABASE cluster_drop_db ON CLUSTER production;

-- DROP DATABASE with SYNC
DROP DATABASE sync_drop_db SYNC;

-- DROP DATABASE with all options
DROP DATABASE IF EXISTS full_drop_db ON CLUSTER production SYNC;