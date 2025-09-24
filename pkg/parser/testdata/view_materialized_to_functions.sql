-- Test MATERIALIZED VIEW TO with various table functions

-- Remote table function
CREATE MATERIALIZED VIEW mv_to_remote 
TO remote('remote-host:9000', 'db', 'target_table')
AS SELECT * FROM local_events;

-- RemoteSecure table function
CREATE MATERIALIZED VIEW mv_secure
TO remoteSecure('secure.host:9440', 'analytics', 'aggregated', 'user', 'password')
ENGINE = MergeTree() ORDER BY date
AS SELECT toDate(timestamp) as date, count() as cnt FROM events GROUP BY date;

-- Cluster table function
CREATE MATERIALIZED VIEW mv_cluster
TO cluster('production', 'default', 'distributed_table')
AS SELECT * FROM source_table WHERE status = 'active';

-- ClusterAllReplicas function
CREATE MATERIALIZED VIEW mv_all_replicas
TO clusterAllReplicas('cluster', 'db', 'replicated_table')
ENGINE = MergeTree() ORDER BY id
AS SELECT id, sum(value) as total FROM metrics GROUP BY id;

-- S3 table function (for data export)
CREATE MATERIALIZED VIEW mv_to_s3
TO s3Table('s3://bucket/path/data.parquet', 'Parquet')
AS SELECT * FROM events WHERE date >= today();

-- File table function (for local export)
CREATE MATERIALIZED VIEW mv_to_file
TO file('/var/data/export.csv', 'CSV')
AS SELECT id, name, value FROM table WHERE modified >= yesterday();

-- URL table function
CREATE MATERIALIZED VIEW mv_to_url
TO url('http://webhook.example.com/data', 'JSONEachRow')
AS SELECT * FROM events WHERE type = 'important';

-- HDFS table function
CREATE MATERIALIZED VIEW mv_to_hdfs
TO hdfs('hdfs://namenode:9000/warehouse/table', 'Parquet')
ENGINE = MergeTree() ORDER BY timestamp
AS SELECT * FROM source_data;

-- PostgreSQL table function
CREATE MATERIALIZED VIEW mv_to_postgres
TO postgresql('postgres.host:5432', 'analytics', 'events', 'user', 'pass')
AS SELECT id, timestamp, data FROM clickhouse_events;

-- MySQL table function
CREATE MATERIALIZED VIEW mv_to_mysql
TO mysql('mysql.host:3306', 'db', 'table', 'user', 'password')
AS SELECT * FROM ch_table WHERE active = 1;

-- MongoDB table function
CREATE MATERIALIZED VIEW mv_to_mongo
TO mongodb('mongodb://mongo:27017', 'database', 'collection', 'user', 'pass')
AS SELECT id, json FROM events_json;

-- Complex materialized view with ON CLUSTER and table function
CREATE MATERIALIZED VIEW IF NOT EXISTS analytics.mv_distributed ON CLUSTER production
TO remoteSecure('analytics.server:9440', 'reports', 'daily_stats', 'reporter', 'secure_pass')
ENGINE = MergeTree() 
ORDER BY (date, user_id)
PARTITION BY toYYYYMM(date)
POPULATE
AS SELECT 
    toDate(timestamp) as date,
    user_id,
    count() as event_count,
    uniq(session_id) as unique_sessions
FROM analytics.events
GROUP BY date, user_id;

-- View with OR REPLACE and table function
CREATE OR REPLACE MATERIALIZED VIEW mv_replace
TO cluster('test_cluster', currentDatabase(), 'target')
AS SELECT * FROM source;