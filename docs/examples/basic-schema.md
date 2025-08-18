# Basic Schema Example

A complete example showing how to create a simple but effective ClickHouse schema with Housekeeper.

## Overview

This example demonstrates a basic analytics platform schema with:
- User tracking and profiles
- Event logging with partitioning
- Simple aggregation views
- Basic dictionaries for lookups

## Project Structure

```
basic-example/
├── housekeeper.yaml
├── db/
│   ├── main.sql
│   ├── config.d/
│   │   └── _clickhouse.xml
│   └── migrations/
└── README.md
```

## Configuration

**housekeeper.yaml:**
```yaml
clickhouse:
  version: "25.7"
  config_dir: "db/config.d"
  cluster: "basic_cluster"

entrypoint: db/main.sql
dir: db/migrations

connection:
  host: localhost
  port: 9000
  database: default
  cluster: basic_cluster

migration:
  auto_approve: false
  backup_before: true
  timeout: 300s
```

## Schema Definition

**db/main.sql:**
```sql
-- Basic Analytics Schema
-- Simple yet production-ready schema for user analytics

-- Create analytics database
CREATE DATABASE analytics ON CLUSTER basic_cluster
ENGINE = Atomic 
COMMENT 'Basic analytics platform database';

-- Users table with profile information
CREATE TABLE analytics.users ON CLUSTER basic_cluster (
    id UInt64,
    email String,
    name String,
    signup_date Date,
    created_at DateTime DEFAULT now(),
    updated_at DateTime DEFAULT now(),
    
    -- Derived columns for analytics
    signup_month UInt32 MATERIALIZED toYYYYMM(signup_date),
    email_domain String MATERIALIZED splitByChar('@', email)[2]
) 
ENGINE = ReplacingMergeTree(updated_at)
ORDER BY id
PARTITION BY signup_month
SETTINGS index_granularity = 8192
COMMENT 'User profiles and account information';

-- Events table for tracking user interactions
CREATE TABLE analytics.events ON CLUSTER basic_cluster (
    id UUID DEFAULT generateUUIDv4(),
    user_id UInt64,
    event_type LowCardinality(String),
    timestamp DateTime DEFAULT now(),
    session_id String,
    page_url Nullable(String),
    
    -- Materialized columns for efficient querying
    date Date MATERIALIZED toDate(timestamp),
    hour UInt8 MATERIALIZED toHour(timestamp)
)
ENGINE = MergeTree()
PARTITION BY (toYYYYMM(timestamp), event_type)
ORDER BY (timestamp, user_id)
TTL timestamp + INTERVAL 90 DAY
SETTINGS index_granularity = 8192
COMMENT 'User event tracking with automatic data lifecycle';

-- Country lookup dictionary
CREATE DICTIONARY analytics.countries_dict ON CLUSTER basic_cluster (
    code FixedString(2),
    name String,
    continent LowCardinality(String)
)
PRIMARY KEY code
SOURCE(FILE(path '/opt/data/countries.csv' format 'CSVWithNames'))
LAYOUT(HASHED())
LIFETIME(86400)
COMMENT 'Country reference data for user geo-analytics';

-- Daily user activity summary
CREATE VIEW analytics.daily_activity AS
SELECT 
    date,
    count() as total_events,
    uniq(user_id) as active_users,
    uniq(session_id) as unique_sessions,
    countIf(event_type = 'page_view') as page_views,
    countIf(event_type = 'click') as clicks
FROM analytics.events
WHERE date >= today() - INTERVAL 30 DAY
GROUP BY date
ORDER BY date DESC;

-- User summary with aggregated metrics
CREATE VIEW analytics.user_summary AS
SELECT 
    u.id,
    u.email,
    u.name,
    u.signup_date,
    u.email_domain,
    e.total_events,
    e.last_activity,
    e.favorite_page
FROM analytics.users u
LEFT JOIN (
    SELECT 
        user_id,
        count() as total_events,
        max(timestamp) as last_activity,
        topK(1)(page_url)[1] as favorite_page
    FROM analytics.events
    WHERE timestamp >= now() - INTERVAL 30 DAY
    GROUP BY user_id
) e ON u.id = e.user_id
ORDER BY e.total_events DESC NULLS LAST;

-- Real-time hourly statistics
CREATE MATERIALIZED VIEW analytics.hourly_stats ON CLUSTER basic_cluster
ENGINE = SummingMergeTree((event_count, unique_users))
ORDER BY (date, hour, event_type)
POPULATE
AS SELECT 
    date,
    hour,
    event_type,
    count() as event_count,
    uniq(user_id) as unique_users
FROM analytics.events
GROUP BY date, hour, event_type;
```

## Key Features Demonstrated

### 1. Database Organization
- Single `analytics` database for related objects
- Cluster-aware DDL with `ON CLUSTER` clauses
- Descriptive comments for documentation

### 2. Table Design Patterns

#### Users Table (Mutable Data)
```sql
ENGINE = ReplacingMergeTree(updated_at)  -- Handle updates with version column
ORDER BY id                              -- Primary key ordering
PARTITION BY signup_month                -- Partition by time for lifecycle management
```

#### Events Table (Immutable Time-Series)
```sql
ENGINE = MergeTree()                     -- Standard MergeTree for append-only data
PARTITION BY (toYYYYMM(timestamp), event_type)  -- Multi-dimensional partitioning
ORDER BY (timestamp, user_id)           -- Optimize for time-range queries
TTL timestamp + INTERVAL 90 DAY         -- Automatic data retention
```

### 3. Performance Optimizations

#### Materialized Columns
```sql
-- Computed on insert, available for WHERE and ORDER BY
signup_month UInt32 MATERIALIZED toYYYYMM(signup_date),
date Date MATERIALIZED toDate(timestamp),
hour UInt8 MATERIALIZED toHour(timestamp)
```

#### LowCardinality for Repeated Values
```sql
event_type LowCardinality(String),       -- Optimize for limited set of values
continent LowCardinality(String)         -- Better compression and performance
```

#### Appropriate Data Types
```sql
id UInt64,                               -- Use smallest sufficient numeric type
code FixedString(2),                     -- Fixed-length strings for codes
page_url Nullable(String)                -- Optional fields as Nullable
```

### 4. Dictionary Integration
```sql
-- Simple file-based dictionary for reference data
SOURCE(FILE(path '/opt/data/countries.csv' format 'CSVWithNames'))
LAYOUT(HASHED())                         -- Fast lookup for small datasets
LIFETIME(86400)                          -- Daily refresh
```

### 5. View Patterns

#### Analytical Views
```sql
-- Complex aggregations for dashboards
SELECT 
    date,
    count() as total_events,
    uniq(user_id) as active_users,        -- Unique user count
    countIf(event_type = 'page_view') as page_views  -- Conditional counting
```

#### User Enrichment Views
```sql
-- Join user profiles with activity metrics
LEFT JOIN (
    SELECT 
        user_id,
        topK(1)(page_url)[1] as favorite_page  -- Most frequent page
    FROM analytics.events
    GROUP BY user_id
) e ON u.id = e.user_id
```

#### Real-time Materialized Views
```sql
-- Pre-aggregate data for fast queries
CREATE MATERIALIZED VIEW analytics.hourly_stats
ENGINE = SummingMergeTree((event_count, unique_users))  -- Automatic summing
POPULATE                                                 -- Backfill existing data
```

## Sample Data

### Users
```sql
INSERT INTO analytics.users (id, email, name, signup_date) VALUES
(1, 'alice@example.com', 'Alice Johnson', '2024-01-15'),
(2, 'bob@company.com', 'Bob Smith', '2024-01-20'),
(3, 'carol@university.edu', 'Carol Wilson', '2024-02-01');
```

### Events
```sql
INSERT INTO analytics.events (user_id, event_type, session_id, page_url) VALUES
(1, 'page_view', 'sess_1', '/dashboard'),
(1, 'click', 'sess_1', '/dashboard'),
(2, 'page_view', 'sess_2', '/products'),
(2, 'page_view', 'sess_2', '/product/123');
```

### Countries Dictionary Data
**countries.csv:**
```csv
code,name,continent
US,United States,North America
CA,Canada,North America
UK,United Kingdom,Europe
DE,Germany,Europe
JP,Japan,Asia
```

## Common Queries

### Daily Activity Report
```sql
SELECT * FROM analytics.daily_activity
WHERE date >= today() - INTERVAL 7 DAY
ORDER BY date DESC;
```

### Top Users by Activity
```sql
SELECT 
    name,
    email,
    total_events,
    last_activity
FROM analytics.user_summary
WHERE total_events IS NOT NULL
ORDER BY total_events DESC
LIMIT 10;
```

### Hourly Event Distribution
```sql
SELECT 
    hour,
    sum(event_count) as total_events,
    sum(unique_users) as total_users
FROM analytics.hourly_stats
WHERE date = today()
GROUP BY hour
ORDER BY hour;
```

### Email Domain Analysis
```sql
SELECT 
    email_domain,
    count() as user_count,
    avg(total_events) as avg_activity
FROM analytics.user_summary
WHERE email_domain != ''
GROUP BY email_domain
ORDER BY user_count DESC;
```

## Migration Workflow

### 1. Initialize Project
```bash
mkdir basic-analytics && cd basic-analytics
housekeeper init
```

### 2. Define Schema
Copy the schema definition to `db/main.sql`.

### 3. Generate Initial Migration
```bash
housekeeper diff
```

### 4. Apply Migration
```bash
housekeeper migrate --url localhost:9000
```

### 5. Evolve Schema
Add new table for product tracking:

```sql
-- Add to db/main.sql
CREATE TABLE analytics.products ON CLUSTER basic_cluster (
    id UInt64,
    name String,
    category LowCardinality(String),
    price Decimal64(2),
    created_at DateTime DEFAULT now()
) ENGINE = ReplacingMergeTree(created_at) ORDER BY id;
```

### 6. Generate Evolution Migration
```bash
housekeeper diff  # Creates migration with just the new table
```

## Best Practices Demonstrated

### 1. Schema Organization
- **Single Database**: Related objects in one database
- **Clear Names**: Descriptive table and column names
- **Comments**: Document purpose and important details

### 2. Performance Design
- **Appropriate Engines**: ReplacingMergeTree for mutable, MergeTree for immutable
- **Smart Partitioning**: By time and category for efficient queries
- **Materialized Columns**: Pre-compute common expressions

### 3. Data Lifecycle
- **TTL Policies**: Automatic cleanup of old event data
- **Retention Strategy**: Keep aggregated data longer than raw data

### 4. Analytics Patterns
- **Materialized Views**: Pre-aggregate frequently queried data
- **Analytical Views**: Complex queries as reusable views
- **Dictionary Lookups**: Reference data for enrichment

### 5. Cluster Readiness
- **ON CLUSTER**: All DDL prepared for distributed deployment
- **Partitioning**: Scales across multiple nodes
- **Replication Ready**: Uses engines compatible with clustering

## Next Steps

From this basic schema, you can evolve to:

1. **[E-commerce Demo](ecommerce-demo.md)** - More complex schema with products and orders
2. **[Complex Migrations](complex-migrations.md)** - Advanced migration scenarios
3. **[Cluster Management](../advanced/cluster-management.md)** - Deploy to distributed ClickHouse
4. **[Best Practices](../advanced/best-practices.md)** - Production optimization techniques

This basic example provides a solid foundation for most analytics use cases while demonstrating Housekeeper's key features and ClickHouse best practices.