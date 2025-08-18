# Schema Management

Learn how to effectively design, organize, and maintain ClickHouse schemas with Housekeeper.

## Schema Design Principles

### Start Simple, Evolve Gradually

Begin with basic table structures and add complexity as your requirements become clear:

```sql
-- Start simple
CREATE TABLE analytics.events (
    id UUID DEFAULT generateUUIDv4(),
    timestamp DateTime,
    event_type String,
    user_id UInt64
) ENGINE = MergeTree() ORDER BY timestamp;

-- Evolve over time
CREATE TABLE analytics.events (
    id UUID DEFAULT generateUUIDv4(),
    timestamp DateTime,
    event_type LowCardinality(String),      -- Optimize for repeated values
    user_id UInt64,
    session_id String,
    properties Map(String, String) DEFAULT map(),  -- Flexible metadata
    
    -- Materialized columns for performance
    date Date MATERIALIZED toDate(timestamp),
    hour UInt8 MATERIALIZED toHour(timestamp)
) 
ENGINE = MergeTree() 
PARTITION BY (toYYYYMM(timestamp), event_type)     -- Better partitioning
ORDER BY (timestamp, user_id, event_type)          -- Optimized ordering
TTL timestamp + INTERVAL 365 DAY;                  -- Data lifecycle
```

### Choose Appropriate Data Types

ClickHouse offers specialized data types for better performance and storage efficiency:

#### Optimized String Types
```sql
-- Use LowCardinality for repeated string values
status LowCardinality(String),              -- 'active', 'inactive', 'pending'
country LowCardinality(FixedString(2)),     -- Country codes
event_type LowCardinality(String),          -- Limited set of event types

-- Use FixedString for known-length strings
ip_address FixedString(16),                 -- IPv6 addresses
currency_code FixedString(3),               -- 'USD', 'EUR', 'GBP'
```

#### Numeric Type Optimization
```sql
-- Choose smallest sufficient numeric types
user_id UInt64,                            -- Large range needed
age UInt8,                                 -- 0-255 is sufficient
price Decimal64(2),                        -- Financial precision
percentage Float32,                        -- Scientific calculations
```

#### Modern Date/Time Types
```sql
-- Use appropriate temporal precision
created_at DateTime,                       -- Second precision
event_timestamp DateTime64(3),            -- Millisecond precision
birth_date Date,                          -- Day precision only
```

### Partitioning Strategy

Design partitions to optimize query performance and data management:

#### Time-Based Partitioning
```sql
-- Monthly partitioning for time-series data
CREATE TABLE analytics.events (
    timestamp DateTime,
    -- other columns
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(timestamp)          -- One partition per month
ORDER BY timestamp;

-- Daily partitioning for high-volume data
PARTITION BY toDate(timestamp)            -- One partition per day

-- Yearly partitioning for historical data
PARTITION BY toYear(timestamp)            -- One partition per year
```

#### Multi-Dimensional Partitioning
```sql
-- Partition by time and category
CREATE TABLE analytics.events (
    timestamp DateTime,
    event_type LowCardinality(String),
    -- other columns
)
ENGINE = MergeTree()
PARTITION BY (toYYYYMM(timestamp), event_type)
ORDER BY (timestamp, user_id);
```

### Ordering Keys

Choose ordering keys based on your query patterns:

#### Primary Query Patterns
```sql
-- For time-series queries by user
ORDER BY (user_id, timestamp)

-- For time-range analytics
ORDER BY (timestamp, event_type, user_id)

-- For user analytics
ORDER BY (user_id, timestamp, event_type)
```

#### Compound Ordering
```sql
-- Multi-level ordering for different query types
CREATE TABLE analytics.user_events (
    user_id UInt64,
    timestamp DateTime,
    event_type LowCardinality(String),
    session_id String
)
ENGINE = MergeTree()
ORDER BY (user_id, timestamp, event_type)    -- Supports multiple query patterns
PRIMARY KEY (user_id, timestamp);            -- Explicit primary key for performance
```

## Schema Organization Patterns

### Database Organization

#### By Domain/Function
```sql
-- Separate databases by business domain
CREATE DATABASE user_data ENGINE = Atomic COMMENT 'User profiles and authentication';
CREATE DATABASE analytics ENGINE = Atomic COMMENT 'Event tracking and analytics';
CREATE DATABASE inventory ENGINE = Atomic COMMENT 'Product catalog and inventory';
CREATE DATABASE financial ENGINE = Atomic COMMENT 'Orders, payments, and billing';
```

#### By Environment
```sql
-- Environment-specific databases
CREATE DATABASE prod_analytics ENGINE = Atomic;
CREATE DATABASE staging_analytics ENGINE = Atomic;
CREATE DATABASE dev_analytics ENGINE = Atomic;
```

### Table Organization

#### Core Tables
```sql
-- User management
CREATE TABLE user_data.users (
    id UInt64,
    email String,
    created_at DateTime,
    updated_at DateTime
) ENGINE = ReplacingMergeTree(updated_at) ORDER BY id;

-- Event tracking
CREATE TABLE analytics.events (
    id UUID DEFAULT generateUUIDv4(),
    user_id UInt64,
    timestamp DateTime,
    event_type LowCardinality(String)
) ENGINE = MergeTree() ORDER BY timestamp;
```

#### Aggregation Tables
```sql
-- Pre-aggregated data for performance
CREATE TABLE analytics.daily_user_stats (
    date Date,
    user_id UInt64,
    event_count UInt32,
    session_count UInt32,
    last_activity DateTime
) ENGINE = SummingMergeTree((event_count, session_count))
ORDER BY (date, user_id);
```

#### Lookup Tables
```sql
-- Reference data
CREATE TABLE reference.countries (
    code FixedString(2),
    name String,
    continent LowCardinality(String)
) ENGINE = MergeTree() ORDER BY code;
```

## Advanced Schema Patterns

### Materialized Columns

Use materialized columns for commonly queried derived values:

```sql
CREATE TABLE analytics.events (
    timestamp DateTime,
    user_id UInt64,
    url String,
    
    -- Materialized columns (computed on insert)
    date Date MATERIALIZED toDate(timestamp),
    hour UInt8 MATERIALIZED toHour(timestamp),
    domain String MATERIALIZED domain(url),
    
    -- Can be used in ORDER BY and WHERE clauses
) 
ENGINE = MergeTree() 
ORDER BY (date, hour, user_id);
```

### Map Columns for Flexible Data

Use Map columns for flexible, schema-less data:

```sql
CREATE TABLE analytics.events (
    id UUID DEFAULT generateUUIDv4(),
    timestamp DateTime,
    event_type LowCardinality(String),
    
    -- Flexible properties
    properties Map(String, String) DEFAULT map(),
    
    -- Typed maps for better performance
    metrics Map(String, Float64) DEFAULT map(),
    flags Map(String, UInt8) DEFAULT map()
) ENGINE = MergeTree() ORDER BY timestamp;

-- Query map data
SELECT 
    event_type,
    properties['page_url'] as page_url,
    metrics['duration'] as duration,
    flags['is_mobile'] as is_mobile
FROM analytics.events
WHERE properties['page_url'] LIKE '%checkout%';
```

### Nested Columns

For structured, repeated data:

```sql
CREATE TABLE analytics.user_sessions (
    user_id UInt64,
    session_id String,
    start_time DateTime,
    
    -- Nested structure for page views
    pages Nested(
        url String,
        title String,
        view_time DateTime,
        duration UInt32
    )
) ENGINE = MergeTree() ORDER BY (user_id, start_time);

-- Query nested data
SELECT 
    user_id,
    pages.url,
    pages.duration
FROM analytics.user_sessions
ARRAY JOIN pages
WHERE pages.url LIKE '%product%';
```

### TTL Policies

Implement data lifecycle management:

```sql
-- Table-level TTL
CREATE TABLE analytics.raw_events (
    timestamp DateTime,
    data String
) 
ENGINE = MergeTree() 
ORDER BY timestamp
TTL timestamp + INTERVAL 30 DAY;           -- Delete after 30 days

-- Column-level TTL
CREATE TABLE analytics.events (
    timestamp DateTime,
    user_id UInt64,
    sensitive_data Nullable(String) TTL timestamp + INTERVAL 7 DAY,  -- GDPR compliance
    analytics_data String
) 
ENGINE = MergeTree() 
ORDER BY timestamp
TTL timestamp + INTERVAL 365 DAY;          -- Keep analytics for 1 year
```

## Dictionary Patterns

### External API Integration

```sql
CREATE DICTIONARY reference.countries_dict (
    code String,
    name String,
    continent String,
    population UInt64 DEFAULT 0
)
PRIMARY KEY code
SOURCE(HTTP(
    url 'https://api.example.com/countries'
    format 'JSONEachRow'
    headers('Authorization' 'Bearer YOUR_TOKEN')
))
LAYOUT(HASHED())
LIFETIME(MIN 3600 MAX 7200)                -- Refresh every 1-2 hours
COMMENT 'Country reference data from external API';
```

### Hierarchical Data

```sql
CREATE DICTIONARY reference.categories_dict (
    id UInt64 IS_OBJECT_ID,
    parent_id UInt64 DEFAULT 0 HIERARCHICAL,
    name String,
    level UInt8
)
PRIMARY KEY id, parent_id
SOURCE(CLICKHOUSE(
    host 'localhost' port 9000
    user 'default' password ''
    db 'reference' table 'category_source'
))
LAYOUT(COMPLEX_KEY_HASHED())
LIFETIME(600)                              -- 10 minute refresh
COMMENT 'Product category hierarchy';
```

### High-Performance Lookups

```sql
CREATE DICTIONARY analytics.user_segments_dict (
    user_id UInt64 IS_OBJECT_ID,
    segment LowCardinality(String) INJECTIVE,  -- One-to-one mapping
    score Float32,
    last_updated DateTime
)
PRIMARY KEY user_id
SOURCE(MYSQL(
    host 'ml-server' port 3306
    user 'readonly' password 'secret'
    db 'ml_models' table 'user_segments'
))
LAYOUT(FLAT())                             -- Fastest for UInt64 keys
LIFETIME(300)                              -- 5 minute refresh
COMMENT 'ML-generated user segments';
```

## View Patterns

### Aggregation Views

```sql
-- Daily summary view
CREATE VIEW analytics.daily_summary AS
SELECT 
    toDate(timestamp) as date,
    event_type,
    count() as event_count,
    uniq(user_id) as unique_users,
    uniq(session_id) as unique_sessions,
    quantile(0.5)(duration) as median_duration
FROM analytics.events
WHERE date >= today() - INTERVAL 30 DAY
GROUP BY date, event_type
ORDER BY date DESC, event_count DESC;
```

### Materialized Views for Real-time Analytics

```sql
-- Real-time user activity aggregation
CREATE MATERIALIZED VIEW analytics.user_activity_mv
ENGINE = SummingMergeTree((event_count, session_count))
ORDER BY (date, user_id)
POPULATE                                   -- Backfill existing data
AS SELECT 
    toDate(timestamp) as date,
    user_id,
    count() as event_count,
    uniq(session_id) as session_count,
    max(timestamp) as last_activity
FROM analytics.events
GROUP BY date, user_id;
```

### Complex Analytical Views

```sql
-- User cohort analysis
CREATE VIEW analytics.user_cohorts AS
WITH user_first_activity AS (
    SELECT 
        user_id,
        min(toDate(timestamp)) as first_activity_date
    FROM analytics.events
    GROUP BY user_id
)
SELECT 
    first_activity_date,
    toDate(e.timestamp) as activity_date,
    dateDiff('day', first_activity_date, toDate(e.timestamp)) as days_since_first,
    count(DISTINCT e.user_id) as active_users
FROM analytics.events e
JOIN user_first_activity ufa ON e.user_id = ufa.user_id
GROUP BY first_activity_date, activity_date, days_since_first
ORDER BY first_activity_date, days_since_first;
```

## Performance Optimization

### Indexing Strategy

```sql
-- Primary index optimization
CREATE TABLE analytics.user_events (
    user_id UInt64,
    timestamp DateTime,
    event_type LowCardinality(String)
)
ENGINE = MergeTree()
ORDER BY (user_id, timestamp)             -- Primary index
PRIMARY KEY user_id                       -- Subset of ORDER BY for performance
SETTINGS index_granularity = 8192;        -- Tune granularity
```

### Data Skipping Indexes

```sql
-- Add skipping indexes for better query performance
CREATE TABLE analytics.events (
    timestamp DateTime,
    user_id UInt64,
    event_type LowCardinality(String),
    url String
)
ENGINE = MergeTree()
ORDER BY timestamp
SETTINGS index_granularity = 8192;

-- Add indexes after table creation
ALTER TABLE analytics.events ADD INDEX idx_user_id user_id TYPE minmax GRANULARITY 4;
ALTER TABLE analytics.events ADD INDEX idx_event_type event_type TYPE set(100) GRANULARITY 1;
ALTER TABLE analytics.events ADD INDEX idx_url_domain domain(url) TYPE bloom_filter GRANULARITY 1;
```

### Compression Optimization

```sql
-- Use appropriate codecs for different data types
CREATE TABLE analytics.events (
    timestamp DateTime CODEC(Delta, ZSTD),     -- Delta for timestamps
    user_id UInt64 CODEC(Delta, ZSTD),         -- Delta for sequential IDs
    event_type LowCardinality(String),         -- Already optimized
    raw_data String CODEC(ZSTD(3))             -- High compression for text
) ENGINE = MergeTree() ORDER BY timestamp;
```

## Schema Evolution Best Practices

### Backwards Compatibility

```sql
-- Add columns with defaults for backwards compatibility
ALTER TABLE analytics.events ADD COLUMN session_id String DEFAULT '';

-- Use Nullable for optional new columns
ALTER TABLE analytics.events ADD COLUMN metadata Nullable(String);

-- Avoid breaking changes
-- ❌ Don't change column types without migration plan
-- ❌ Don't remove columns without deprecation period
```

### Deprecation Strategy

```sql
-- Mark columns as deprecated in comments
CREATE TABLE analytics.events (
    old_field String COMMENT 'DEPRECATED: Use new_field instead',
    new_field String,
    -- other columns
) ENGINE = MergeTree() ORDER BY timestamp;

-- Plan removal in future migrations
-- 1. Add new column
-- 2. Migrate data
-- 3. Update applications
-- 4. Remove old column
```

### Version Management

```sql
-- Include version information in schema
CREATE DATABASE analytics ENGINE = Atomic 
COMMENT 'Analytics database v2.1 - Added user segmentation support';

CREATE TABLE analytics.events (
    -- Include schema version in metadata
    schema_version UInt8 DEFAULT 2,
    -- other columns
) ENGINE = MergeTree() ORDER BY timestamp;
```

## Next Steps

- **[Migration Process](migration-process.md)** - Learn how to apply schema changes
- **[Configuration](configuration.md)** - Configure Housekeeper for your environment
- **[Best Practices](../advanced/best-practices.md)** - Advanced schema design patterns
- **[Examples](../examples/ecommerce-demo.md)** - See complete schema examples