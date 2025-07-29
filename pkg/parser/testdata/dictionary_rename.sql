RENAME DICTIONARY old_dict TO new_dict;
RENAME DICTIONARY db.old_dict TO db.new_dict;
RENAME DICTIONARY dict1 TO dict2, db.dict3 TO db.dict4;
RENAME DICTIONARY old_dict TO new_dict ON CLUSTER my_cluster;
RENAME DICTIONARY db1.dict1 TO db2.dict2, dict3 TO dict4 ON CLUSTER production;