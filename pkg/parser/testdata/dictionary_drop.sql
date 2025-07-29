-- Basic DROP DICTIONARY
DROP DICTIONARY drop_basic_dict;

-- DROP DICTIONARY with IF EXISTS
DROP DICTIONARY IF EXISTS analytics.drop_ifexists_dict;

-- DROP DICTIONARY with ON CLUSTER
DROP DICTIONARY drop_cluster_dict ON CLUSTER production;

-- DROP DICTIONARY with SYNC
DROP DICTIONARY IF EXISTS analytics.drop_full_dict SYNC;