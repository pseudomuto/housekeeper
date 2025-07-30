-- Basic CREATE VIEW
CREATE VIEW analytics.daily_summary AS SELECT date, count(*) as total FROM events GROUP BY date;

-- CREATE VIEW with IF NOT EXISTS
CREATE VIEW IF NOT EXISTS users_view AS SELECT id, name FROM users WHERE active = 1;

-- CREATE VIEW with ON CLUSTER
CREATE VIEW stats_view ON CLUSTER production AS SELECT * FROM statistics;

-- CREATE VIEW with database prefix
CREATE VIEW db.my_view AS SELECT col1, col2 FROM db.my_table;

-- CREATE OR REPLACE VIEW
CREATE OR REPLACE VIEW analytics.updated_view AS SELECT id, name, updated_at FROM users ORDER BY updated_at DESC;

-- Basic CREATE MATERIALIZED VIEW
CREATE MATERIALIZED VIEW mv_daily_stats AS SELECT toDate(timestamp) as date, count() as cnt FROM events GROUP BY date;

-- CREATE MATERIALIZED VIEW with ENGINE
CREATE MATERIALIZED VIEW analytics.mv_aggregated
ENGINE = MergeTree()
ORDER BY (date, user_id)
AS SELECT toDate(timestamp) as date, user_id, count() as events_count FROM events GROUP BY date, user_id;

-- CREATE MATERIALIZED VIEW with TO table (simplified single line)
CREATE MATERIALIZED VIEW mv_to_table TO analytics.target_table AS SELECT * FROM source_table WHERE status = 'active';

-- CREATE MATERIALIZED VIEW with POPULATE
CREATE MATERIALIZED VIEW mv_with_populate
ENGINE = AggregatingMergeTree()
ORDER BY date
POPULATE
AS SELECT toDate(timestamp) as date, sum(amount) as total FROM transactions GROUP BY date;

-- CREATE OR REPLACE MATERIALIZED VIEW with full options (simplified single line)
CREATE OR REPLACE MATERIALIZED VIEW IF NOT EXISTS analytics.mv_complex ON CLUSTER production TO analytics.destination_table ENGINE = ReplacingMergeTree(version) POPULATE AS SELECT id, name, max(version) as version, argMax(data, version) as data FROM source GROUP BY id, name;

-- CREATE MATERIALIZED VIEW with complex SELECT
CREATE MATERIALIZED VIEW analytics.mv_joins
ENGINE = MergeTree() ORDER BY (date, category)
AS SELECT 
    toDate(e.timestamp) as date,
    e.user_id,
    u.name as user_name,
    e.category,
    count() as event_count,
    sum(e.value) as total_value
FROM events e
LEFT JOIN users u ON e.user_id = u.id
WHERE e.status = 'completed'
GROUP BY date, e.user_id, u.name, e.category;

