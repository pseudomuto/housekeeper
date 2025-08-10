-- Example demonstrating ClickHouse table migration scenarios
-- These examples show various table operations that the schemadiff can handle

-- =================================================================
-- TABLE CREATION: From empty schema to tables with various features
-- =================================================================

-- Simple table creation
CREATE TABLE users (
    id UInt64,
    name String,
    email String
) ENGINE = MergeTree()
ORDER BY id;

-- Complex table with all ClickHouse features
CREATE OR REPLACE TABLE IF NOT EXISTS analytics.events ON CLUSTER production (
    id UInt64,
    user_id UInt64,
    event_type LowCardinality(String),
    timestamp DateTime DEFAULT now(),
    data Map(String, String) DEFAULT map(),
    metadata Nullable(String) CODEC(ZSTD),
    tags Array(String),
    location Tuple(lat Float64, lon Float64),
    settings Nested(
        key String,
        value String
    ),
    temp_data String TTL timestamp + INTERVAL 30 DAY COMMENT 'Temporary data'
) ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/events', '{replica}')
ORDER BY (user_id, timestamp)
PARTITION BY toYYYYMM(timestamp)
PRIMARY KEY user_id
SAMPLE BY id
TTL timestamp + INTERVAL 1 YEAR
SETTINGS index_granularity = 8192, merge_with_ttl_timeout = 3600
COMMENT 'User events table';

-- =================================================================
-- TABLE ALTERATION: Modifying existing table structure
-- =================================================================

-- Example migration: Adding columns to users table
-- FROM:
-- CREATE TABLE users (
--     id UInt64,
--     name String
-- ) ENGINE = MergeTree() ORDER BY id;
--
-- TO:
-- CREATE TABLE users (
--     id UInt64,
--     name LowCardinality(String),  -- Modified type
--     email String,                 -- Added column
--     created_at DateTime,          -- Added column
--     phone Nullable(String)        -- Added column with complex type
-- ) ENGINE = MergeTree() ORDER BY id;
--
-- Generated migration:
-- ALTER TABLE users
--     MODIFY COLUMN name LowCardinality(String),
--     ADD COLUMN email String,
--     ADD COLUMN created_at DateTime,
--     ADD COLUMN phone Nullable(String);

-- =================================================================
-- TABLE RENAMING: Intelligent rename detection
-- =================================================================

-- The schemadiff detects when tables with identical structure 
-- are renamed rather than dropped and recreated
-- FROM: old_users → TO: users (same structure)
-- Generated: RENAME TABLE old_users TO users;

-- =================================================================
-- COMPLEX SCENARIOS: Mixed operations
-- =================================================================

-- Multi-table operations with proper dependency ordering:
-- 1. Databases (if any)
-- 2. Tables (base tables first)
-- 3. Views (depend on tables)
-- 4. Dictionaries (can depend on tables)

-- Example: E-commerce schema evolution
CREATE TABLE products (
    id UInt64,
    name String,
    price Decimal(10, 2),
    category_id UInt32
) ENGINE = MergeTree()
ORDER BY id;

CREATE TABLE orders (
    id UInt64,
    user_id UInt64,
    product_id UInt64,
    quantity UInt16,
    total_amount Decimal(12, 2),
    created_at DateTime
) ENGINE = MergeTree()
ORDER BY (user_id, created_at)
PARTITION BY toYYYYMM(created_at);

-- =================================================================
-- MIGRATION FEATURES SUPPORTED:
-- =================================================================

-- ✅ CREATE TABLE - All ClickHouse features
--    - OR REPLACE, IF NOT EXISTS
--    - ON CLUSTER support
--    - All data types (primitives, complex, nested)
--    - Column modifiers (DEFAULT, CODEC, TTL, COMMENT)
--    - Engine with parameters
--    - Table options (ORDER BY, PARTITION BY, PRIMARY KEY, SAMPLE BY)
--    - Table-level TTL and SETTINGS
--    - Table comments

-- ✅ ALTER TABLE - Column-level changes
--    - ADD COLUMN with all options
--    - DROP COLUMN
--    - MODIFY COLUMN (type and options)
--    - Multiple changes in single statement

-- ✅ RENAME TABLE - Intelligent detection
--    - Single and multiple table renames
--    - Cross-database renames
--    - Cluster-aware operations

-- ✅ DROP TABLE - Complete cleanup
--    - Proper DOWN migration (recreate with full DDL)
--    - Cluster-aware operations

-- ❌ ENGINE changes - Not supported (would require data migration)
-- ❌ CLUSTER changes - Not supported (infrastructure change)

-- =================================================================
-- MIGRATION ORDER AND SAFETY:
-- =================================================================

-- UP migrations:   databases → tables → views → dictionaries
-- DOWN migrations: dictionaries → views → tables → databases
-- Within each type: CREATE → ALTER → RENAME → DROP

-- This ensures proper dependency handling and safe rollbacks.