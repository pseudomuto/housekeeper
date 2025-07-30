-- Basic ATTACH VIEW
ATTACH VIEW analytics.daily_summary;

-- ATTACH VIEW with IF NOT EXISTS
ATTACH VIEW IF NOT EXISTS users_view;

-- ATTACH VIEW with ON CLUSTER
ATTACH VIEW stats_view ON CLUSTER production;

-- ATTACH VIEW with database prefix
ATTACH VIEW db.my_view;

-- Basic ATTACH TABLE for materialized view
ATTACH TABLE mv_daily_stats;

-- ATTACH TABLE for materialized view with IF NOT EXISTS
ATTACH TABLE IF NOT EXISTS analytics.mv_aggregated;

-- ATTACH TABLE for materialized view with ON CLUSTER
ATTACH TABLE mv_complex ON CLUSTER production;

-- ATTACH TABLE for materialized view with database prefix and cluster
ATTACH TABLE IF NOT EXISTS analytics.mv_joins ON CLUSTER analytics_cluster;