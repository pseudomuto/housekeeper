-- Basic ATTACH DICTIONARY
ATTACH DICTIONARY attach_basic_dict;

-- ATTACH DICTIONARY with IF NOT EXISTS
ATTACH DICTIONARY IF NOT EXISTS analytics.attach_ifnotexists_dict;

-- ATTACH DICTIONARY with ON CLUSTER
ATTACH DICTIONARY attach_cluster_dict ON CLUSTER production;

-- ATTACH DICTIONARY with database prefix and cluster
ATTACH DICTIONARY IF NOT EXISTS analytics.attach_full_dict ON CLUSTER production;