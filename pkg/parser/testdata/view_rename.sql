-- Basic RENAME TABLE for view
RENAME TABLE old_view TO new_view;

-- RENAME TABLE for view with database prefix
RENAME TABLE analytics.old_summary TO analytics.new_summary;

-- RENAME TABLE for view across databases
RENAME TABLE db1.view1 TO db2.view1;

-- Multiple RENAME TABLE operations for views
RENAME TABLE view1 TO view1_new, analytics.view2 TO analytics.view2_new;

-- RENAME TABLE for view with ON CLUSTER
RENAME TABLE old_stats TO new_stats ON CLUSTER production;

-- Multiple renames with different databases and cluster
RENAME TABLE db1.v1 TO db2.v1, v2 TO v3, analytics.old TO analytics.new ON CLUSTER analytics_cluster;

-- Basic RENAME TABLE for materialized view
RENAME TABLE old_mv TO new_mv;

-- RENAME TABLE for materialized view with database prefix
RENAME TABLE analytics.old_mv_summary TO analytics.new_mv_summary;

-- RENAME TABLE for materialized view across databases
RENAME TABLE db1.mv1 TO db2.mv1;

-- Multiple RENAME TABLE operations for materialized views
RENAME TABLE mv1 TO mv1_new, analytics.mv2 TO analytics.mv2_new;

-- RENAME TABLE for materialized view with ON CLUSTER
RENAME TABLE old_mv_stats TO new_mv_stats ON CLUSTER production;

-- Multiple materialized view renames with different databases and cluster
RENAME TABLE db1.mv1 TO db2.mv1, mv2 TO mv3, analytics.old_mv TO analytics.new_mv ON CLUSTER analytics_cluster;