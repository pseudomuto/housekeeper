# E-commerce Demo

This comprehensive example demonstrates how to build and manage a complete e-commerce analytics platform using Housekeeper and ClickHouse. The demo showcases real-world patterns, best practices, and advanced ClickHouse features.

## Overview

The e-commerce demo models a typical online shopping platform with:

- **Customer data management** with user profiles and segmentation
- **Product catalog** with categories and inventory tracking
- **Order processing** with line items and financial data
- **Event tracking** for user behavior analytics
- **Real-time dashboards** with materialized views
- **External integrations** with country and ML services

## Project Structure

```
examples/demo/
├── housekeeper.yaml          # Project configuration
├── db/
│   ├── main.sql              # Main schema entrypoint
│   ├── config.d/             # ClickHouse cluster configuration
│   ├── schemas/ecommerce/    # Modular schema files
│   │   ├── schema.sql        # Database coordinator
│   │   ├── tables/           # Table definitions
│   │   ├── dictionaries/     # Dictionary definitions
│   │   └── views/            # View definitions
│   └── migrations/           # Generated migration files
└── README.md                 # Detailed documentation
```

## Database Architecture

### Core Tables

#### Users Table
Customer profiles with versioning and data retention:

```sql
CREATE TABLE ecommerce.users ON CLUSTER demo (
    user_id UInt64,
    email String,
    first_name String,
    last_name String,
    registration_date Date DEFAULT today(),
    last_login_at DateTime DEFAULT now(),
    country_code FixedString(2),
    total_orders UInt32 DEFAULT 0,
    total_spent Decimal64(2) DEFAULT 0,
    segment_id UInt8 DEFAULT 0,
    preferences Map(String, String) DEFAULT map(),
    created_at DateTime DEFAULT now(),
    updated_at DateTime DEFAULT now(),
    version UInt64 MATERIALIZED toUnixTimestamp64Milli(now())
)
ENGINE = ReplacingMergeTree(version)
ORDER BY user_id
SETTINGS index_granularity = 8192;
```

#### Events Table
High-volume event tracking with partitioning:

```sql
CREATE TABLE ecommerce.events ON CLUSTER demo (
    event_id UInt64,
    user_id UInt64,
    session_id String,
    event_type LowCardinality(String),
    page_url Nullable(String),
    product_id Nullable(UInt64),
    timestamp DateTime DEFAULT now(),
    properties Map(String, String) DEFAULT map(),
    user_agent String CODEC(ZSTD),
    ip_address IPv4 DEFAULT toIPv4('0.0.0.0')
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(timestamp)
ORDER BY (user_id, timestamp, event_id)
TTL timestamp + INTERVAL 2 YEAR
SETTINGS index_granularity = 8192;
```

#### Products Table
Product catalog with rich metadata:

```sql
CREATE TABLE ecommerce.products ON CLUSTER demo (
    product_id UInt64,
    name String,
    description String,
    category_id UInt32,
    price Decimal64(2),
    cost Decimal64(2),
    weight_grams UInt32,
    dimensions Array(UInt32),
    tags Array(String),
    attributes Map(String, String) DEFAULT map(),
    created_at DateTime DEFAULT now(),
    updated_at DateTime DEFAULT now(),
    version UInt64 MATERIALIZED toUnixTimestamp64Milli(now())
)
ENGINE = ReplacingMergeTree(version)
ORDER BY product_id
SETTINGS index_granularity = 8192;
```

### External Data Integration

#### Countries Dictionary
External API integration for country data:

```sql
CREATE DICTIONARY ecommerce.countries_dict ON CLUSTER demo (
    code FixedString(2) IS_OBJECT_ID,
    name String INJECTIVE,
    continent String,
    population UInt64 DEFAULT 0
)
PRIMARY KEY code
SOURCE(HTTP(
    url 'https://api.example.com/countries'
    headers 'Authorization: Bearer token123'
    format 'JSONEachRow'
))
LAYOUT(HASHED(size_in_cells 300))
LIFETIME(MIN 3600 MAX 7200)
SETTINGS(max_threads = 2);
```

#### ML-Based User Segmentation
Real-time user segment classification:

```sql
CREATE DICTIONARY ecommerce.user_segments_dict ON CLUSTER demo (
    user_id UInt64 IS_OBJECT_ID,
    segment_id UInt8,
    segment_name String,
    score Float32 DEFAULT 0.0,
    confidence Float32 DEFAULT 0.0,
    last_updated DateTime DEFAULT now()
)
PRIMARY KEY user_id
SOURCE(HTTP(
    url 'https://ml-api.example.com/user-segments'
    headers 'Authorization: Bearer ml-token'
    format 'JSONEachRow'
))
LAYOUT(COMPLEX_KEY_HASHED(size_in_cells 1000000))
LIFETIME(MIN 300 MAX 600)
SETTINGS(max_threads = 4);
```

### Real-time Analytics

#### Product Performance Metrics
Continuous aggregation of product analytics:

```sql
CREATE MATERIALIZED VIEW ecommerce.mv_product_stats ON CLUSTER demo
ENGINE = SummingMergeTree()
ORDER BY (product_id, date)
POPULATE
AS SELECT
    product_id,
    toDate(timestamp) as date,
    countIf(event_type = 'product_view') as views,
    countIf(event_type = 'add_to_cart') as cart_adds,
    countIf(event_type = 'purchase') as purchases,
    round(countIf(event_type = 'add_to_cart') / countIf(event_type = 'product_view') * 100, 2) as cart_conversion_rate,
    round(countIf(event_type = 'purchase') / countIf(event_type = 'add_to_cart') * 100, 2) as purchase_conversion_rate
FROM ecommerce.events
WHERE product_id IS NOT NULL
GROUP BY product_id, date;
```

#### Hourly Event Aggregation
Real-time dashboard metrics:

```sql
CREATE MATERIALIZED VIEW ecommerce.mv_hourly_events ON CLUSTER demo
ENGINE = SummingMergeTree()
ORDER BY (event_type, hour)
POPULATE
AS SELECT
    event_type,
    toStartOfHour(timestamp) as hour,
    count() as event_count,
    uniq(user_id) as unique_users,
    uniq(session_id) as unique_sessions
FROM ecommerce.events
GROUP BY event_type, hour;
```

## Advanced Query Patterns

### Customer Journey Analysis

```sql
-- Complex customer journey with window functions
WITH customer_events AS (
    SELECT
        user_id,
        event_type,
        timestamp,
        lag(event_type) OVER (PARTITION BY user_id ORDER BY timestamp) as prev_event,
        lag(timestamp) OVER (PARTITION BY user_id ORDER BY timestamp) as prev_timestamp
    FROM ecommerce.events
    WHERE toDate(timestamp) = today()
),
journey_steps AS (
    SELECT
        user_id,
        event_type,
        prev_event,
        timestamp,
        dateDiff('second', prev_timestamp, timestamp) as step_duration
    FROM customer_events
    WHERE prev_event IS NOT NULL
)
SELECT
    concat(prev_event, ' -> ', event_type) as journey_step,
    count() as occurrences,
    avg(step_duration) as avg_duration_seconds,
    quantile(0.5)(step_duration) as median_duration_seconds
FROM journey_steps
GROUP BY journey_step
ORDER BY occurrences DESC
LIMIT 20;
```

### Sales Performance Dashboard

```sql
-- Comprehensive sales metrics with multiple JOINs
SELECT
    c.name as category_name,
    p.name as product_name,
    sum(oi.quantity) as total_sold,
    sum(oi.quantity * oi.unit_price) as total_revenue,
    avg(oi.unit_price) as avg_price,
    count(DISTINCT o.order_id) as order_count,
    count(DISTINCT o.user_id) as unique_customers,
    round(total_revenue / total_sold, 2) as revenue_per_unit
FROM ecommerce.order_items oi
INNER JOIN ecommerce.orders o ON oi.order_id = o.order_id
INNER JOIN ecommerce.products p ON oi.product_id = p.product_id
INNER JOIN ecommerce.categories_dict c ON p.category_id = c.category_id
WHERE o.order_date >= today() - INTERVAL 30 DAY
GROUP BY c.name, p.name
HAVING total_sold > 0
ORDER BY total_revenue DESC
LIMIT 50;
```

### User Segmentation Analysis

```sql
-- Advanced user analytics with external data
SELECT
    us.segment_name,
    count(DISTINCT u.user_id) as user_count,
    avg(u.total_spent) as avg_lifetime_value,
    avg(u.total_orders) as avg_orders,
    countIf(u.last_login_at >= now() - INTERVAL 7 DAY) as active_last_week,
    round(active_last_week / user_count * 100, 2) as activity_rate
FROM ecommerce.users u
LEFT JOIN ecommerce.user_segments_dict us ON u.user_id = us.user_id
LEFT JOIN ecommerce.countries_dict c ON u.country_code = c.code
GROUP BY us.segment_name
ORDER BY user_count DESC;
```

## Schema Evolution Example

### Migration 1: Initial Setup
```sql
-- 20240813190639.sql - Basic user tracking
CREATE DATABASE ecommerce ON CLUSTER demo ENGINE = Atomic;
CREATE TABLE ecommerce.users (...);
CREATE TABLE ecommerce.events (...);
```

### Migration 2: E-commerce Expansion
```sql
-- 20240814143146.sql - Add commerce features
CREATE TABLE ecommerce.products (...);
CREATE TABLE ecommerce.orders (...);
CREATE TABLE ecommerce.order_items (...);
ALTER TABLE ecommerce.users ADD COLUMN total_spent Decimal64(2) DEFAULT 0;
```

### Migration 3: Analytics Enhancement
```sql
-- 20240817120443.sql - Add analytics features
CREATE DICTIONARY ecommerce.countries_dict (...);
CREATE DICTIONARY ecommerce.user_segments_dict (...);
CREATE MATERIALIZED VIEW ecommerce.mv_product_stats (...);
CREATE VIEW ecommerce.daily_sales AS SELECT ...;
```

## Best Practices Demonstrated

### 1. Schema Organization
- **Modular files**: Separate concerns into focused files
- **Import system**: Use `-- housekeeper:import` for maintainability
- **Naming conventions**: Consistent, descriptive naming

### 2. Performance Optimization
- **Partitioning**: Monthly partitions for time-series data
- **Ordering keys**: Optimize for common query patterns
- **Materialized columns**: Pre-compute expensive operations
- **Codecs**: Compress large text fields with ZSTD

### 3. Data Management
- **TTL policies**: Automatic data retention for privacy compliance
- **Versioning**: ReplacingMergeTree for slowly changing dimensions
- **Default values**: Sensible defaults for optional fields

### 4. External Integration
- **Dictionary sources**: HTTP APIs for reference data
- **Authentication**: Secure external API access
- **Refresh strategies**: Balanced between freshness and performance

### 5. Real-time Analytics
- **Materialized views**: Continuous aggregation for dashboards
- **SummingMergeTree**: Efficient storage for metric data
- **Window functions**: Advanced analytical queries

## Deployment Strategies

### Development
```bash
# Start local ClickHouse with demo configuration
cd examples/demo
docker run -d \
  --name clickhouse-demo \
  -p 9000:9000 \
  -p 8123:8123 \
  -v $(pwd)/db/config.d:/etc/clickhouse-server/config.d \
  clickhouse/clickhouse-server:25.7

# Start development server and generate migration
housekeeper dev up
housekeeper diff
```

### Staging
```bash
# Generate migration for staging (use appropriate project directory)
housekeeper diff -d staging
```

### Production
```bash
# Generate migration for production (use appropriate project directory)
housekeeper diff -d production

# Apply after review
clickhouse-client --host prod-cluster \
  --queries-file db/migrations/20240817120443.sql
```

## Monitoring and Maintenance

### Key Metrics
```sql
-- Monitor table sizes
SELECT
    database,
    table,
    formatReadableSize(sum(bytes_on_disk)) as size,
    sum(rows) as row_count
FROM system.parts
WHERE database = 'ecommerce'
GROUP BY database, table
ORDER BY sum(bytes_on_disk) DESC;

-- Check replication status
SELECT * FROM system.replicas WHERE database = 'ecommerce';

-- Monitor dictionary updates
SELECT
    name,
    status,
    last_successful_update_time,
    loading_duration
FROM system.dictionaries
WHERE database = 'ecommerce';
```

### Optimization Opportunities
```sql
-- Identify hot partitions
SELECT
    partition,
    count() as part_count,
    sum(rows) as total_rows,
    max(modification_time) as last_modified
FROM system.parts
WHERE database = 'ecommerce' AND table = 'events'
GROUP BY partition
ORDER BY last_modified DESC;

-- Analyze query performance
SELECT
    query,
    elapsed,
    rows_read,
    bytes_read
FROM system.query_log
WHERE database = 'ecommerce'
ORDER BY elapsed DESC
LIMIT 10;
```

This e-commerce demo provides a comprehensive foundation for building production-ready analytics platforms with ClickHouse and Housekeeper, demonstrating real-world patterns and best practices.