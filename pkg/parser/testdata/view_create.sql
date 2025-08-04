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

-- CREATE VIEW with backtick identifiers
CREATE VIEW `analytics-db`.`daily-summary` AS 
SELECT 
    `order-date` as `date`,
    count(*) as `total-orders`
FROM `orders-table` 
GROUP BY `order-date`;

-- CREATE MATERIALIZED VIEW with backtick identifiers
CREATE MATERIALIZED VIEW `metrics`.`hourly-stats` 
ENGINE = MergeTree() 
ORDER BY `timestamp`
AS SELECT 
    toStartOfHour(`created-at`) as `timestamp`,
    count(*) as `count`
FROM `events-table` 
GROUP BY `timestamp`;



-- CREATE VIEW with window functions
CREATE VIEW analytics.user_rankings AS
SELECT 
    user_id,
    name,
    score,
    row_number() OVER (ORDER BY score DESC) as rank,
    rank() OVER (PARTITION BY category ORDER BY score DESC) as category_rank,
    lag(score, 1) OVER (ORDER BY score DESC) as prev_score
FROM user_scores
ORDER BY score DESC;

-- CREATE MATERIALIZED VIEW with complex window functions and frames
CREATE MATERIALIZED VIEW analytics.mv_sales_trends
ENGINE = MergeTree()
ORDER BY (date, region)
AS SELECT 
    date,
    region,
    sales_amount,
    sum(sales_amount) OVER (
        PARTITION BY region 
        ORDER BY date 
        ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
    ) as seven_day_total,
    avg(sales_amount) OVER (
        PARTITION BY region 
        ORDER BY date 
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) as running_avg,
    dense_rank() OVER (
        PARTITION BY date 
        ORDER BY sales_amount DESC
    ) as daily_rank
FROM daily_sales;
