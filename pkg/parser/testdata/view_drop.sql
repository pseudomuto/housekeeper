-- Basic DROP VIEW
DROP VIEW analytics.daily_summary;

-- DROP VIEW with IF EXISTS
DROP VIEW IF EXISTS users_view;

-- DROP VIEW with ON CLUSTER
DROP VIEW stats_view ON CLUSTER production;

-- DROP VIEW with SYNC
DROP VIEW db.my_view SYNC;

-- DROP VIEW with all options
DROP VIEW IF EXISTS old_view ON CLUSTER production SYNC;

-- Basic DROP TABLE for materialized view
DROP TABLE mv_daily_stats;

-- DROP TABLE for materialized view with IF EXISTS
DROP TABLE IF EXISTS analytics.mv_aggregated;

-- DROP TABLE for materialized view with ON CLUSTER
DROP TABLE mv_complex ON CLUSTER production;

-- DROP TABLE for materialized view with SYNC
DROP TABLE analytics.mv_joins SYNC;

-- DROP TABLE for materialized view with all options
DROP TABLE IF EXISTS analytics.mv_old ON CLUSTER analytics_cluster SYNC;