-- Basic DETACH VIEW
DETACH VIEW analytics.daily_summary;

-- DETACH VIEW with IF EXISTS
DETACH VIEW IF EXISTS users_view;

-- DETACH VIEW with ON CLUSTER
DETACH VIEW stats_view ON CLUSTER production;

-- DETACH VIEW with PERMANENTLY
DETACH VIEW db.my_view PERMANENTLY;

-- DETACH VIEW with SYNC
DETACH VIEW analytics.updated_view SYNC;

-- DETACH VIEW with all options
DETACH VIEW IF EXISTS old_view ON CLUSTER production PERMANENTLY SYNC;

-- Basic DETACH TABLE for materialized view
DETACH TABLE mv_daily_stats;

-- DETACH TABLE for materialized view with IF EXISTS
DETACH TABLE IF EXISTS analytics.mv_aggregated;

-- DETACH TABLE for materialized view with ON CLUSTER
DETACH TABLE mv_complex ON CLUSTER production;

-- DETACH TABLE for materialized view with PERMANENTLY and SYNC
DETACH TABLE analytics.mv_joins PERMANENTLY SYNC;

-- DETACH TABLE for materialized view with all options
DETACH TABLE IF EXISTS analytics.mv_old ON CLUSTER analytics_cluster PERMANENTLY SYNC;