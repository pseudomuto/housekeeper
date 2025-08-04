-- Basic ALTER TABLE ADD COLUMN
ALTER TABLE users ADD COLUMN age UInt8;

-- ALTER TABLE DROP COLUMN
ALTER TABLE user_profiles DROP COLUMN computed_field;

-- ALTER TABLE RENAME COLUMN
ALTER TABLE measurements RENAME COLUMN device_id TO device_identifier;

-- ALTER TABLE COMMENT COLUMN
ALTER TABLE users COMMENT COLUMN email 'User email address';

-- ALTER TABLE ADD INDEX with simple expression
ALTER TABLE logs ADD INDEX level_idx level TYPE minmax GRANULARITY 1;

-- ALTER TABLE ADD INDEX with complex expression
ALTER TABLE events ADD INDEX user_date_idx (user_id, toDate(timestamp)) TYPE minmax GRANULARITY 1;

-- ALTER TABLE ADD INDEX with CAST expression
ALTER TABLE orders ADD INDEX price_idx CAST(price AS UInt64) TYPE set GRANULARITY 2;

-- ALTER TABLE DROP INDEX
ALTER TABLE logs DROP INDEX level_idx;

-- ALTER TABLE ADD CONSTRAINT
ALTER TABLE users ADD CONSTRAINT id_check CHECK id > 0;

-- ALTER TABLE DROP CONSTRAINT
ALTER TABLE users DROP CONSTRAINT id_check;

-- ALTER TABLE UPDATE
ALTER TABLE users UPDATE age = age + 1 WHERE id < 1000;

-- ALTER TABLE DELETE with function call
ALTER TABLE logs DELETE WHERE timestamp < now();

-- ALTER TABLE FREEZE
ALTER TABLE analytics.events FREEZE;

-- ALTER TABLE DELETE TTL
ALTER TABLE analytics.events DELETE TTL;

-- ALTER TABLE MODIFY TTL with simple expression
ALTER TABLE analytics.events MODIFY TTL timestamp + days(30);

-- ALTER TABLE MODIFY ORDER BY with tuple expression
ALTER TABLE measurements MODIFY ORDER BY (device_identifier, created_at, id);

-- ALTER TABLE MODIFY SAMPLE BY with identifier
ALTER TABLE analytics.events MODIFY SAMPLE BY user_id;

-- ALTER TABLE REMOVE SAMPLE BY
ALTER TABLE analytics.events REMOVE SAMPLE BY;

-- ALTER TABLE MODIFY SETTING
ALTER TABLE analytics.events MODIFY SETTING index_granularity = 16384;

-- ALTER TABLE RESET SETTING
ALTER TABLE analytics.events RESET SETTING index_granularity;

-- ALTER TABLE ATTACH PARTITION
ALTER TABLE analytics.events ATTACH PARTITION '202301';

-- ALTER TABLE DETACH PARTITION
ALTER TABLE analytics.events DETACH PARTITION '202301';

-- ALTER TABLE DROP PARTITION
ALTER TABLE analytics.events DROP PARTITION '202301';

-- ALTER TABLE FREEZE PARTITION
ALTER TABLE analytics.events FREEZE PARTITION '202301';

-- ALTER TABLE CLEAR COLUMN IN PARTITION
ALTER TABLE measurements CLEAR COLUMN value IN PARTITION '202312';

-- ALTER TABLE FETCH PARTITION
ALTER TABLE analytics.events FETCH PARTITION '202301' FROM '/clickhouse/tables/events';

-- ALTER TABLE MOVE PARTITION TO TABLE
ALTER TABLE analytics.events MOVE PARTITION '202301' TO TABLE analytics.events_archive;

-- ALTER TABLE MOVE PARTITION TO DISK
ALTER TABLE analytics.events MOVE PARTITION '202301' TO DISK 'cold_storage';

-- ALTER TABLE MOVE PARTITION TO VOLUME
ALTER TABLE analytics.events MOVE PARTITION '202301' TO VOLUME 'archive_volume';

-- ALTER TABLE REPLACE PARTITION
ALTER TABLE analytics.events REPLACE PARTITION '202301' FROM analytics.events_backup;

-- ALTER TABLE ATTACH PARTITION FROM
ALTER TABLE analytics.events ATTACH PARTITION '202301' FROM staging.events;

-- ALTER TABLE with IF EXISTS
ALTER TABLE IF EXISTS missing_table ADD COLUMN new_col String;

-- ALTER TABLE ADD COLUMN with IF NOT EXISTS
ALTER TABLE users ADD COLUMN IF NOT EXISTS age UInt8;

-- ALTER TABLE DROP COLUMN with IF EXISTS
ALTER TABLE users DROP COLUMN IF EXISTS non_existent_column;

-- ALTER TABLE RENAME COLUMN with IF EXISTS
ALTER TABLE users RENAME COLUMN IF EXISTS old_name TO new_name;

-- ALTER TABLE ADD INDEX with IF NOT EXISTS
ALTER TABLE logs ADD INDEX IF NOT EXISTS level_idx level TYPE minmax GRANULARITY 1;

-- ALTER TABLE DROP INDEX with IF EXISTS
ALTER TABLE logs DROP INDEX IF EXISTS level_idx;

-- ALTER TABLE ADD CONSTRAINT with IF NOT EXISTS
ALTER TABLE users ADD CONSTRAINT IF NOT EXISTS id_check CHECK id > 0;

-- ALTER TABLE DROP CONSTRAINT with IF EXISTS
ALTER TABLE users DROP CONSTRAINT IF EXISTS id_check;

-- ALTER TABLE CLEAR COLUMN with IF EXISTS
ALTER TABLE measurements CLEAR COLUMN IF EXISTS value IN PARTITION '202312';

-- ALTER TABLE ADD COLUMN with AFTER
ALTER TABLE users ADD COLUMN middle_name String AFTER first_name;

-- ALTER TABLE ADD COLUMN with FIRST
ALTER TABLE users ADD COLUMN id UInt64 FIRST;

-- ALTER TABLE FREEZE with WITH NAME
ALTER TABLE analytics.events FREEZE WITH NAME 'backup_20240101';

-- ALTER TABLE FREEZE PARTITION with WITH NAME
ALTER TABLE analytics.events FREEZE PARTITION '202301' WITH NAME 'partition_backup';

-- ALTER TABLE with ON CLUSTER
ALTER TABLE logs ON CLUSTER production ADD COLUMN server_id String;

-- ALTER TABLE with multiple operations
ALTER TABLE analytics.events
ADD COLUMN session_id UUID,
DROP COLUMN tags,
RENAME COLUMN data TO event_data,
COMMENT COLUMN timestamp 'Event timestamp';

-- ALTER TABLE MODIFY COLUMN basic
ALTER TABLE users MODIFY COLUMN name String;

-- ALTER TABLE MODIFY COLUMN with REMOVE DEFAULT
ALTER TABLE users MODIFY COLUMN name String REMOVE DEFAULT;

-- ALTER TABLE MODIFY TTL with DELETE WHERE clause
ALTER TABLE logs MODIFY TTL timestamp + days(30) DELETE WHERE level = 'debug';

-- Multiple partition operations
ALTER TABLE analytics.events
DETACH PARTITION '202301',
DETACH PARTITION '202302',
DETACH PARTITION '202303';

-- ALTER TABLE ADD PROJECTION basic
ALTER TABLE analytics.events ADD PROJECTION user_stats (
    SELECT 
        user_id,
        count() AS event_count
    GROUP BY user_id
);

-- ALTER TABLE ADD PROJECTION with IF NOT EXISTS
ALTER TABLE analytics.events ADD PROJECTION IF NOT EXISTS daily_stats (
    SELECT 
        toDate(timestamp) AS date,
        count() AS total_events,
        uniq(user_id) AS unique_users
    GROUP BY date
    ORDER BY date DESC
);

-- ALTER TABLE ADD PROJECTION with complex SELECT
ALTER TABLE analytics.events ADD PROJECTION revenue_summary (
    SELECT 
        user_id,
        toYYYYMM(timestamp) AS month,
        sum(revenue) AS total_revenue,
        avg(revenue) AS avg_revenue,
        count() AS transaction_count
    GROUP BY user_id, month
    ORDER BY user_id, month
);

-- ALTER TABLE DROP PROJECTION
ALTER TABLE analytics.events DROP PROJECTION user_stats;

-- ALTER TABLE DROP PROJECTION with IF EXISTS
ALTER TABLE analytics.events DROP PROJECTION IF EXISTS old_projection;

-- ALTER TABLE with mixed operations including projections
ALTER TABLE analytics.events
ADD COLUMN revenue Decimal(10, 2) DEFAULT 0,
ADD PROJECTION revenue_stats (
    SELECT 
        toYYYYMM(timestamp) AS month,
        sum(revenue) AS monthly_revenue
    GROUP BY month
),
DROP PROJECTION IF EXISTS old_stats;