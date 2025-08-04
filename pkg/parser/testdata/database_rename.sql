RENAME DATABASE old_db TO new_db;
RENAME DATABASE db1 TO db2, db3 TO db4;
RENAME DATABASE old_db TO new_db ON CLUSTER my_cluster;
RENAME DATABASE db1 TO db2, db3 TO db4 ON CLUSTER production;
RENAME DATABASE `old-name` TO `new-name` ON CLUSTER `prod-cluster`;