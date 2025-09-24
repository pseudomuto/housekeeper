# Writing Schemas

Learn how to write effective ClickHouse schemas using Housekeeper's features and best practices.

## Schema File Structure

### Basic Schema File

A basic schema file contains DDL statements for your ClickHouse objects:

```sql
-- Basic schema example
-- File: db/main.sql

-- Create the database
CREATE DATABASE ecommerce ENGINE = Atomic COMMENT 'E-commerce platform database';

-- Create core tables
CREATE TABLE ecommerce.users (
    id UInt64,
    email String,
    name String,
    created_at DateTime DEFAULT now(),
    updated_at DateTime DEFAULT now()
) ENGINE = ReplacingMergeTree(updated_at) ORDER BY id;

CREATE TABLE ecommerce.products (
    id UInt64,
    name String,
    category_id UInt32,
    price Decimal64(2),
    created_at DateTime DEFAULT now()
) ENGINE = ReplacingMergeTree(created_at) ORDER BY id;
```

### Modular Schema Organization

For larger projects, split schemas into logical modules:

```sql
-- File: db/main.sql
-- Main entrypoint that imports other schema files

-- Global objects first (roles, users, settings profiles)
-- housekeeper:import schemas/_global/roles/main.sql

-- Database definitions
-- housekeeper:import schemas/databases/ecommerce.sql

-- Named collections (connection configs)
-- housekeeper:import schemas/_global/collections/api_configs.sql
-- housekeeper:import schemas/_global/collections/kafka_configs.sql

-- Core tables
-- housekeeper:import schemas/tables/users.sql
-- housekeeper:import schemas/tables/products.sql
-- housekeeper:import schemas/tables/orders.sql

-- Reference data
-- housekeeper:import schemas/dictionaries/countries.sql
-- housekeeper:import schemas/dictionaries/categories.sql

-- Analytics views
-- housekeeper:import schemas/views/sales_summary.sql
-- housekeeper:import schemas/views/user_analytics.sql
```

### Import Order and Dependencies

The order of imports matters for proper migration generation. Housekeeper processes schema objects in dependency order:

1. **Global Objects** (roles, users, settings profiles) - Available cluster-wide
2. **Databases** - Container for other objects
3. **Named Collections** - Global connection configurations
4. **Tables** - Data storage structures
5. **Dictionaries** - External data lookups
6. **Views** - Query abstractions that may depend on tables/dictionaries

```sql
-- File: db/main.sql - Proper import ordering
-- ✅ Correct order

-- 1. Global objects first - processed before anything else
-- housekeeper:import schemas/_global/roles/main.sql

-- 2. Database definitions  
-- housekeeper:import schemas/databases/main.sql

-- 3. Named collections (if needed by tables)
-- housekeeper:import schemas/_global/collections/main.sql

-- 4. Tables and core data structures
-- housekeeper:import schemas/tables/main.sql

-- 5. Dictionaries that may reference tables
-- housekeeper:import schemas/dictionaries/main.sql

-- 6. Views that query tables and dictionaries
-- housekeeper:import schemas/views/main.sql
```

**Why Global Objects Come First:**

Global objects like roles are cluster-wide and may be referenced by other objects:

```sql
-- Role must exist before being granted to table operations
CREATE ROLE IF NOT EXISTS data_writer;

-- Later in the schema, tables may reference the role in grants
CREATE TABLE analytics.events (...) ENGINE = MergeTree() ORDER BY timestamp;
GRANT INSERT ON analytics.events TO data_writer;
```

See the [Role Management](role-management.md) guide for comprehensive role management patterns.

## Database Design

### Database Creation

```sql
-- Basic database
CREATE DATABASE analytics ENGINE = Atomic;

-- Database with cluster support
CREATE DATABASE analytics ON CLUSTER my_cluster ENGINE = Atomic;

-- Database with comment
CREATE DATABASE analytics 
ENGINE = Atomic 
COMMENT 'Analytics and reporting database';

-- Database with external engine
CREATE DATABASE mysql_replica 
ENGINE = MaterializedMySQL('mysql-server:3306', 'source_db', 'user', 'password')
SETTINGS allows_query_when_mysql_lost = 1;
```

### Database Engines

Choose appropriate database engines based on your needs:

```sql
-- Atomic (default) - Transactional database
CREATE DATABASE prod_data ENGINE = Atomic;

-- MySQL integration
CREATE DATABASE mysql_data 
ENGINE = MySQL('mysql-host:3306', 'database', 'user', 'password');

-- PostgreSQL integration  
CREATE DATABASE postgres_data
ENGINE = PostgreSQL('postgres-host:5432', 'database', 'user', 'password');
```

## Named Collections

Named collections provide a centralized way to store connection parameters and configuration that can be reused across multiple tables, especially useful for integration engines.

### Basic Named Collections

```sql
-- API configuration
CREATE NAMED COLLECTION api_config AS
    host = 'api.example.com',
    port = 443,
    ssl = TRUE,
    timeout = 30;

-- Database connection
CREATE NAMED COLLECTION postgres_config AS
    host = 'postgres-host',
    port = 5432,
    user = 'clickhouse_user',
    password = 'secure_password',
    database = 'production_db';

-- Kafka configuration  
CREATE NAMED COLLECTION kafka_cluster AS
    kafka_broker_list = 'kafka1:9092,kafka2:9092,kafka3:9092',
    kafka_security_protocol = 'SASL_SSL',
    kafka_sasl_mechanism = 'PLAIN',
    kafka_sasl_username = 'clickhouse',
    kafka_sasl_password = 'secret';
```

### Using Named Collections with Tables

Named collections are particularly powerful with integration engine tables:

```sql
-- Kafka table using named collection
CREATE TABLE events (
    id UInt64,
    event_type String,
    timestamp DateTime,
    data String
) ENGINE = Kafka(
    kafka_cluster,  -- Reference to named collection
    'events_topic',
    'analytics_group',
    'JSONEachRow'
);

-- PostgreSQL table using named collection
CREATE TABLE users_staging (
    id UInt64,
    email String,
    name String
) ENGINE = PostgreSQL(
    postgres_config,  -- Reference to named collection
    'users'
);
```

### Named Collection Features

- **Centralized Configuration**: Define connection parameters once, use everywhere
- **Security**: Keeps credentials out of individual table definitions
- **Cluster Support**: Full `ON CLUSTER` support for distributed deployments
- **Global Scope**: Named collections are available across all databases
- **Immutable**: Use `CREATE OR REPLACE` for modifications (no `ALTER` support)

### Configuration vs DDL-Managed Collections

Housekeeper distinguishes between two types of named collections:

1. **DDL-Managed Collections**: Created via `CREATE NAMED COLLECTION` statements
   - Stored in system.named_collections table
   - Managed through Housekeeper migrations
   - Can be created, replaced, and dropped via DDL

2. **Configuration-Managed Collections**: Defined in ClickHouse XML/YAML config files
   - Also appear in system.named_collections table
   - Typically prefixed with `builtin_` by convention
   - Require configuration file changes and server restarts
   - **Automatically excluded from schema extraction**

> **Note**: Collections with names starting with `builtin_` are filtered out during schema extraction to prevent conflicts between DDL and configuration management approaches. This ensures that configuration-defined collections remain under config management control.

### Cluster-Aware Named Collections

```sql
-- Named collection for cluster deployment
CREATE NAMED COLLECTION kafka_cluster 
ON CLUSTER production
AS
    kafka_broker_list = 'kafka1:9092,kafka2:9092',
    kafka_security_protocol = 'SASL_SSL',
    kafka_sasl_mechanism = 'PLAIN',
    kafka_sasl_username = 'clickhouse',
    kafka_sasl_password = 'secret';
```

## Table Design

### Table Engines

#### MergeTree Family (Most Common)

```sql
-- Basic MergeTree for time-series data
CREATE TABLE analytics.events (
    timestamp DateTime,
    user_id UInt64,
    event_type String,
    properties Map(String, String)
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(timestamp)
ORDER BY (timestamp, user_id);

-- ReplacingMergeTree for mutable data
CREATE TABLE ecommerce.users (
    id UInt64,
    email String,
    name String,
    updated_at DateTime
)
ENGINE = ReplacingMergeTree(updated_at)  -- Version column
ORDER BY id;

-- SummingMergeTree for pre-aggregated data
CREATE TABLE analytics.daily_stats (
    date Date,
    metric String,
    value UInt64
)
ENGINE = SummingMergeTree(value)         -- Sum column
ORDER BY (date, metric);
```

#### Integration Engines

```sql
-- Kafka integration
CREATE TABLE streaming.kafka_events (
    timestamp DateTime,
    user_id UInt64,
    event_data String
)
ENGINE = Kafka()
SETTINGS 
    kafka_broker_list = 'localhost:9092',
    kafka_topic_list = 'events',
    kafka_group_name = 'clickhouse_consumer',
    kafka_format = 'JSONEachRow';

-- MySQL integration
CREATE TABLE external.mysql_users (
    id UInt64,
    name String,
    email String
)
ENGINE = MySQL('mysql-host:3306', 'database', 'users', 'user', 'password');
```

### Column Definitions

#### Data Types

```sql
CREATE TABLE comprehensive_example (
    -- Numeric types
    id UInt64,
    age UInt8,
    balance Decimal64(2),
    score Float32,
    
    -- String types
    name String,
    status LowCardinality(String),       -- For repeated values
    country_code FixedString(2),         -- Fixed length
    
    -- Date/time types
    created_at DateTime,
    event_time DateTime64(3),            -- Millisecond precision
    birth_date Date,
    
    -- Boolean and UUID
    is_active Bool,
    session_id UUID,
    
    -- Complex types
    tags Array(String),
    metadata Map(String, String),
    coordinates Tuple(Float64, Float64),
    
    -- Nullable types
    phone Nullable(String),
    last_login Nullable(DateTime)
) ENGINE = MergeTree() ORDER BY id;
```

#### Column Attributes

```sql
CREATE TABLE advanced_columns (
    id UInt64,
    
    -- Default values
    created_at DateTime DEFAULT now(),
    status String DEFAULT 'pending',
    
    -- Materialized columns (computed on insert)
    date Date MATERIALIZED toDate(created_at),
    month_year String MATERIALIZED formatDateTime(created_at, '%Y-%m'),
    
    -- Alias columns (computed on read)
    age_years UInt8 ALIAS dateDiff('year', birth_date, today()),
    
    -- Compression
    large_text String CODEC(ZSTD(3)),
    metrics Array(Float64) CODEC(Delta, LZ4),
    
    -- Comments
    user_id UInt64 COMMENT 'Reference to users table',
    
    -- TTL for specific columns
    sensitive_data Nullable(String) TTL created_at + INTERVAL 30 DAY,
    
    birth_date Date
) ENGINE = MergeTree() ORDER BY id;
```

### Table Properties

#### Partitioning

```sql
-- Time-based partitioning
CREATE TABLE time_series (
    timestamp DateTime,
    value Float64
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(timestamp)         -- Monthly partitions
ORDER BY timestamp;

-- Multi-dimensional partitioning
CREATE TABLE events (
    timestamp DateTime,
    event_type LowCardinality(String),
    user_id UInt64
)
ENGINE = MergeTree()
PARTITION BY (toYYYYMM(timestamp), event_type)  -- By month and type
ORDER BY (timestamp, user_id);

-- Custom partitioning expression
CREATE TABLE user_data (
    user_id UInt64,
    data String,
    created_at DateTime
)
ENGINE = MergeTree()
PARTITION BY intDiv(user_id, 1000000)    -- Partition by user ID ranges
ORDER BY user_id;
```

#### Ordering and Primary Keys

```sql
-- Basic ordering
CREATE TABLE simple_table (
    timestamp DateTime,
    user_id UInt64,
    event_type String
)
ENGINE = MergeTree()
ORDER BY timestamp;

-- Compound ordering
CREATE TABLE compound_order (
    user_id UInt64,
    timestamp DateTime,
    session_id String
)
ENGINE = MergeTree()
ORDER BY (user_id, timestamp, session_id);

-- Explicit primary key (subset of ORDER BY)
CREATE TABLE optimized_table (
    user_id UInt64,
    timestamp DateTime,
    event_type String,
    data String
)
ENGINE = MergeTree()
ORDER BY (user_id, timestamp, event_type)
PRIMARY KEY (user_id, timestamp);       -- Smaller primary key for performance
```

#### TTL Policies

```sql
-- Table-level TTL
CREATE TABLE temporary_data (
    timestamp DateTime,
    data String
)
ENGINE = MergeTree()
ORDER BY timestamp
TTL timestamp + INTERVAL 7 DAY;         -- Delete after 7 days

-- Multi-level TTL
CREATE TABLE tiered_storage (
    timestamp DateTime,
    data String
)
ENGINE = MergeTree()
ORDER BY timestamp
TTL timestamp + INTERVAL 30 DAY TO DISK 'cold',      -- Move to cold storage
    timestamp + INTERVAL 365 DAY DELETE;             -- Delete after 1 year

-- Column-specific TTL
CREATE TABLE privacy_aware (
    timestamp DateTime,
    user_id UInt64,
    personal_data Nullable(String) TTL timestamp + INTERVAL 30 DAY,
    analytics_data String
)
ENGINE = MergeTree()
ORDER BY timestamp
TTL timestamp + INTERVAL 2 YEAR;        -- Keep analytics longer
```

### CREATE TABLE AS - Schema Copying

The `CREATE TABLE AS` syntax allows you to create a new table by copying the schema from an existing table. This is particularly useful for:
- Creating distributed tables that mirror local tables
- Creating backup tables with identical structure
- Quickly duplicating table schemas with different engines

#### Basic Syntax

```sql
-- Copy schema from existing table
CREATE TABLE copy AS source ENGINE = MergeTree() ORDER BY id;

-- With database qualifiers
CREATE TABLE db1.table_copy AS db2.source_table ENGINE = Memory;

-- With all options
CREATE OR REPLACE TABLE IF NOT EXISTS backup_users AS users 
ENGINE = MergeTree() 
ORDER BY user_id;
```

#### Distributed Table Pattern

The most common use case is creating distributed tables that mirror local table schemas:

```sql
-- Define local table
CREATE TABLE events_local (
    id UInt64,
    timestamp DateTime,
    event_type LowCardinality(String),
    user_id UInt64,
    data Map(String, String)
) ENGINE = MergeTree()
ORDER BY (timestamp, user_id)
PARTITION BY toYYYYMM(timestamp);

-- Create distributed table with same schema
CREATE TABLE events_all ON CLUSTER production AS events_local
ENGINE = Distributed(production, currentDatabase(), events_local, rand());

-- Create distributed table with specific sharding
CREATE TABLE events_distributed ON CLUSTER analytics AS events_local  
ENGINE = Distributed(analytics, currentDatabase(), events_local, cityHash64(user_id));
```

#### Backup Table Pattern

Create backup tables with identical structure but different engines or settings:

```sql
-- Original table
CREATE TABLE users (
    id UInt64,
    name String,
    email String,
    created_at DateTime DEFAULT now()
) ENGINE = MergeTree() ORDER BY id;

-- Create backup with same schema
CREATE TABLE users_backup AS users 
ENGINE = MergeTree() 
ORDER BY id
SETTINGS index_granularity = 1024;  -- Different settings

-- Create memory-based staging table
CREATE TABLE users_staging AS users ENGINE = Memory;
```

#### Migration Behavior

When using `CREATE TABLE AS` with Housekeeper, understand these key behaviors:

1. **Schema Resolution**: The AS reference is resolved at schema processing time, copying all column definitions from the source table.

2. **Independence**: Once created, AS tables are independent entities. They can be dropped, renamed, or modified without affecting the source.

3. **Column Propagation**: When the source table gets column changes (ADD/DROP/MODIFY), Housekeeper automatically propagates these to AS dependents:
   - **MergeTree family**: Uses ALTER TABLE to preserve data
   - **Distributed/Memory engines**: Uses DROP+CREATE (safe, as they don't store local data)

4. **Structural Operations Don't Propagate**: DROP, DETACH, ATTACH, or RENAME of the source table has NO effect on AS dependents.

Example migration scenario:

```sql
-- Current schema
CREATE TABLE metrics_local (
    timestamp DateTime,
    value Float64
) ENGINE = MergeTree() ORDER BY timestamp;

CREATE TABLE metrics_all AS metrics_local 
ENGINE = Distributed(cluster, currentDatabase(), metrics_local, rand());

-- When metrics_local gets new column, migration generates:
-- 1. ALTER TABLE metrics_local ADD COLUMN metric_name String;
-- 2. DROP TABLE metrics_all; 
-- 3. CREATE TABLE metrics_all (...with new column...) ENGINE = Distributed(...);
```

#### Best Practices

- Use `CREATE TABLE AS` for distributed tables that should mirror local table schemas
- Use it for creating backup or staging tables with same structure
- Remember that AS tables are independent after creation - the source can be dropped
- Place AS table definitions after their source tables in schema files
- Consider using AS for tables that should maintain schema consistency with a source

## Dictionary Design

### Basic Dictionaries

```sql
-- Simple lookup dictionary
CREATE DICTIONARY reference.countries (
    code String,
    name String,
    continent String
)
PRIMARY KEY code
SOURCE(FILE(path '/data/countries.csv' format 'CSVWithNames'))
LAYOUT(HASHED())
LIFETIME(86400);                         -- Reload daily
```

### Advanced Dictionary Features

```sql
-- Complex dictionary with multiple attributes
CREATE DICTIONARY analytics.user_segments (
    user_id UInt64 IS_OBJECT_ID,           -- Object identifier
    parent_user_id UInt64 DEFAULT 0 HIERARCHICAL,  -- Hierarchy support
    segment String INJECTIVE,              -- One-to-one mapping
    score Float32,
    created_at DateTime,
    
    -- Computed attributes
    segment_level UInt8 EXPRESSION 
        CASE segment 
            WHEN 'premium' THEN 3
            WHEN 'standard' THEN 2
            ELSE 1
        END
)
PRIMARY KEY user_id, parent_user_id
SOURCE(CLICKHOUSE(
    host 'localhost' port 9000
    user 'default' password ''
    db 'ml_models' table 'user_segments'
    query 'SELECT user_id, parent_user_id, segment, score, created_at FROM user_segments WHERE updated_at > {created_at}'
))
LAYOUT(COMPLEX_KEY_HASHED(size_in_cells 1000000))
LIFETIME(MIN 300 MAX 600)                -- Random refresh between 5-10 minutes
SETTINGS(max_threads = 4)
COMMENT 'ML-generated user segmentation data';
```

### Dictionary Sources

```sql
-- HTTP source with authentication
CREATE DICTIONARY external.api_data (
    id UInt64,
    value String
)
PRIMARY KEY id
SOURCE(HTTP(
    url 'https://api.example.com/data'
    format 'JSONEachRow'
    headers('Authorization' 'Bearer YOUR_TOKEN', 'Content-Type' 'application/json')
))
LAYOUT(HASHED())
LIFETIME(3600);

-- MySQL source
CREATE DICTIONARY external.mysql_lookup (
    id UInt64,
    name String
)
PRIMARY KEY id
SOURCE(MYSQL(
    host 'mysql-server' port 3306
    user 'readonly' password 'secret'
    db 'reference' table 'lookup_data'
    update_field 'updated_at'             -- Incremental updates
))
LAYOUT(FLAT())                           -- Best for UInt64 keys
LIFETIME(600);

-- File source
CREATE DICTIONARY reference.static_data (
    code String,
    description String
)
PRIMARY KEY code
SOURCE(FILE(path '/opt/data/reference.tsv' format 'TabSeparated'))
LAYOUT(HASHED())
LIFETIME(0);                             -- Never reload (static data)
```

## View Design

### Simple Views

```sql
-- Basic view for common queries
CREATE VIEW analytics.active_users AS
SELECT 
    user_id,
    max(timestamp) as last_activity,
    count() as event_count
FROM analytics.events
WHERE timestamp >= now() - INTERVAL 30 DAY
GROUP BY user_id
HAVING event_count > 10;

-- View with complex joins
CREATE VIEW ecommerce.order_summary AS
SELECT 
    o.id as order_id,
    o.user_id,
    u.email,
    o.total_amount,
    o.created_at,
    groupArray(p.name) as product_names
FROM ecommerce.orders o
JOIN ecommerce.users u ON o.user_id = u.id
JOIN ecommerce.order_items oi ON o.id = oi.order_id
JOIN ecommerce.products p ON oi.product_id = p.id
GROUP BY o.id, o.user_id, u.email, o.total_amount, o.created_at;
```

### Materialized Views

```sql
-- Real-time aggregation
CREATE MATERIALIZED VIEW analytics.hourly_stats
ENGINE = SummingMergeTree((event_count, unique_users))
ORDER BY (date, hour, event_type)
POPULATE                                 -- Backfill existing data
AS SELECT 
    toDate(timestamp) as date,
    toHour(timestamp) as hour,
    event_type,
    count() as event_count,
    uniq(user_id) as unique_users
FROM analytics.events
GROUP BY date, hour, event_type;

-- Materialized view with target table
CREATE TABLE analytics.user_stats_target (
    date Date,
    user_id UInt64,
    event_count UInt32,
    last_activity DateTime
) ENGINE = ReplacingMergeTree(last_activity) ORDER BY (date, user_id);

CREATE MATERIALIZED VIEW analytics.user_stats_mv
TO analytics.user_stats_target
AS SELECT 
    toDate(timestamp) as date,
    user_id,
    count() as event_count,
    max(timestamp) as last_activity
FROM analytics.events
GROUP BY date, user_id;
```

## Advanced Patterns

### Polymorphic Tables

```sql
-- Single table for multiple entity types
CREATE TABLE analytics.entity_events (
    timestamp DateTime,
    entity_type LowCardinality(String),   -- 'user', 'product', 'order'
    entity_id UInt64,
    event_type LowCardinality(String),
    properties Map(String, String)
)
ENGINE = MergeTree()
PARTITION BY (toYYYYMM(timestamp), entity_type)
ORDER BY (timestamp, entity_type, entity_id);
```

### Event Sourcing Pattern

```sql
-- Event store table
CREATE TABLE event_store.events (
    event_id UUID DEFAULT generateUUIDv4(),
    aggregate_id UInt64,
    aggregate_type LowCardinality(String),
    event_type LowCardinality(String),
    event_version UInt32,
    event_data String,                    -- JSON payload
    metadata Map(String, String),
    timestamp DateTime DEFAULT now()
)
ENGINE = MergeTree()
PARTITION BY (toYYYYMM(timestamp), aggregate_type)
ORDER BY (aggregate_id, event_version, timestamp);

-- Materialized view for current state
CREATE MATERIALIZED VIEW event_store.user_current_state
ENGINE = ReplacingMergeTree(event_version)
ORDER BY aggregate_id
AS SELECT 
    aggregate_id as user_id,
    event_version,
    JSONExtractString(event_data, 'name') as name,
    JSONExtractString(event_data, 'email') as email,
    JSONExtractString(event_data, 'status') as status,
    timestamp as updated_at
FROM event_store.events
WHERE aggregate_type = 'user';
```

### Time Series with Retention

```sql
-- Multi-resolution time series
CREATE TABLE metrics.raw_metrics (
    timestamp DateTime,
    metric_name LowCardinality(String),
    value Float64,
    tags Map(String, String)
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(timestamp)
ORDER BY (metric_name, timestamp)
TTL timestamp + INTERVAL 7 DAY;         -- Keep raw data for 7 days

-- 1-minute aggregates
CREATE MATERIALIZED VIEW metrics.minute_aggregates
ENGINE = MergeTree()
ORDER BY (metric_name, timestamp)
TTL timestamp + INTERVAL 30 DAY         -- Keep for 30 days
AS SELECT 
    toStartOfMinute(timestamp) as timestamp,
    metric_name,
    avg(value) as avg_value,
    min(value) as min_value,
    max(value) as max_value,
    count() as sample_count,
    tags
FROM metrics.raw_metrics
GROUP BY timestamp, metric_name, tags;

-- 1-hour aggregates  
CREATE MATERIALIZED VIEW metrics.hour_aggregates
ENGINE = MergeTree()
ORDER BY (metric_name, timestamp)
TTL timestamp + INTERVAL 365 DAY        -- Keep for 1 year
AS SELECT 
    toStartOfHour(timestamp) as timestamp,
    metric_name,
    avg(avg_value) as avg_value,
    min(min_value) as min_value,
    max(max_value) as max_value,
    sum(sample_count) as sample_count,
    tags
FROM metrics.minute_aggregates
GROUP BY timestamp, metric_name, tags;
```

## Best Practices

### Naming Conventions

```sql
-- Use clear, descriptive names
CREATE DATABASE user_analytics;          -- Not: db1, analytics_db
CREATE TABLE user_analytics.page_views;  -- Not: pv, page_view_table

-- Use consistent prefixes
CREATE MATERIALIZED VIEW mv_daily_stats;  -- Prefix: mv_
CREATE DICTIONARY dict_countries;         -- Prefix: dict_
```

### Documentation

```sql
-- Use comments extensively
CREATE DATABASE ecommerce 
ENGINE = Atomic 
COMMENT 'E-commerce platform database - contains users, products, and orders';

CREATE TABLE ecommerce.users (
    id UInt64 COMMENT 'Unique user identifier',
    email String COMMENT 'User email address (unique)',
    name String COMMENT 'Full user name',
    created_at DateTime DEFAULT now() COMMENT 'Account creation timestamp',
    status LowCardinality(String) DEFAULT 'active' COMMENT 'User status: active, inactive, suspended'
) 
ENGINE = ReplacingMergeTree(created_at)
ORDER BY id
COMMENT 'User profiles and account information';
```

### Performance Considerations

```sql
-- Optimize for your query patterns
CREATE TABLE analytics.events (
    timestamp DateTime,
    user_id UInt64,
    event_type LowCardinality(String),
    session_id String
)
ENGINE = MergeTree()
-- Order by most selective columns first for your queries
ORDER BY (user_id, timestamp, event_type)  -- If querying by user_id most often
-- ORDER BY (timestamp, event_type, user_id)  -- If querying by time ranges most often
SETTINGS index_granularity = 8192;
```

## Next Steps

- **[Migration Process](migration-process.md)** - Learn how to apply schema changes
- **[Schema Management](schema-management.md)** - Advanced schema design patterns
- **[Configuration](configuration.md)** - Configure Housekeeper for your environment
- **[Examples](../examples/ecommerce-demo.md)** - See complete schema examples