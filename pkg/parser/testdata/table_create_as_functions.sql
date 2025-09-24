-- Test CREATE TABLE AS with various table functions

-- Remote functions
CREATE TABLE remote_copy AS remote('host:9000', 'db', 'table') ENGINE = MergeTree() ORDER BY id;

CREATE TABLE secure_remote AS remoteSecure('host:9440', 'db', 'table', 'user', 'password') 
ENGINE = MergeTree() ORDER BY timestamp;

-- Cluster functions
CREATE TABLE cluster_data AS cluster('my_cluster', 'default', 'events') 
ENGINE = MergeTree() ORDER BY id;

CREATE TABLE all_replicas AS clusterAllReplicas('prod_cluster', 'analytics', 'metrics')
ENGINE = Memory();

-- S3 table function
CREATE TABLE s3_import AS s3Table('https://bucket.s3.amazonaws.com/data.csv', 'CSV')
ENGINE = MergeTree() ORDER BY tuple();

-- File functions
CREATE TABLE file_import AS file('/path/to/data.tsv', 'TSV')
ENGINE = MergeTree() ORDER BY id;

-- URL function
CREATE TABLE url_import AS url('http://example.com/data.json', 'JSONEachRow')
ENGINE = Memory();

-- HDFS function
CREATE TABLE hdfs_data AS hdfs('hdfs://namenode:9000/data/file.parquet', 'Parquet')
ENGINE = MergeTree() ORDER BY timestamp;

-- Generator functions
CREATE TABLE test_numbers AS numbers(1000000)
ENGINE = Memory();

CREATE TABLE random_data AS generateRandom('id UInt64, name String, value Float64', 10000, 42)
ENGINE = MergeTree() ORDER BY id;

-- Complex function with ON CLUSTER
CREATE TABLE IF NOT EXISTS events_all ON CLUSTER production 
AS remote('shard{01..04}:9000', currentDatabase(), 'events_local', 'user', 'pass')
ENGINE = Distributed('production', currentDatabase(), 'events_local', rand());

-- Table function with database prefix
CREATE TABLE analytics.remote_metrics AS remoteSecure('metrics.server:9440', 'monitoring', 'metrics')
ENGINE = MergeTree() ORDER BY timestamp;

-- PostgreSQL table function
CREATE TABLE pg_import AS postgresql('host:5432', 'database', 'table', 'user', 'password')
ENGINE = MergeTree() ORDER BY id;

-- MySQL table function
CREATE TABLE mysql_import AS mysql('host:3306', 'database', 'table', 'user', 'password')
ENGINE = MergeTree() ORDER BY id;

-- MongoDB table function
CREATE TABLE mongo_import AS mongodb('mongodb://host:27017', 'database', 'collection', 'user', 'password')
ENGINE = MergeTree() ORDER BY id;