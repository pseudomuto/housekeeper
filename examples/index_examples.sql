-- ClickHouse INDEX examples for CREATE TABLE statements

-- Table with various index types
CREATE TABLE search_data (
    id UInt64,
    title String,
    content String,
    tags Array(String),
    category LowCardinality(String),
    created_at DateTime,
    price Float64,
    
    -- Bloom filter index for string searches
    INDEX title_bloom title TYPE bloom_filter GRANULARITY 1,
    
    -- MinMax index for numeric ranges
    INDEX price_minmax price TYPE minmax GRANULARITY 3,
    
    -- Set index for categorical data
    INDEX category_set category TYPE set(100) GRANULARITY 1,
    
    -- Token bloom filter for full-text search
    INDEX content_tokens content TYPE tokenbf_v1(32768, 3, 0) GRANULARITY 1,
    
    -- Hypothesis index (experimental)
    INDEX tags_hypothesis tags TYPE hypothesis GRANULARITY 1,
    
    -- Expression-based index
    INDEX created_month toYYYYMM(created_at) TYPE minmax GRANULARITY 2
    
) ENGINE = MergeTree()
ORDER BY (category, created_at)
PARTITION BY toYYYYMM(created_at);

-- Table with indexes and constraints
CREATE TABLE products (
    id UInt64,
    name String,
    description String,
    price Decimal(10, 2),
    category_id UInt32,
    created_at DateTime DEFAULT now(),
    
    -- Index definitions
    INDEX name_bloom name TYPE bloom_filter GRANULARITY 1,
    INDEX price_minmax price TYPE minmax GRANULARITY 1,
    INDEX desc_tokens description TYPE tokenbf_v1(8192, 2, 0) GRANULARITY 1,
    
    -- Constraint definitions  
    CONSTRAINT positive_price CHECK price > 0
    
) ENGINE = MergeTree()
ORDER BY id;

-- Table with mixed definitions (columns, indexes, constraints)
CREATE TABLE user_activity (
    user_id UInt64,
    action String,
    timestamp DateTime,
    ip_address IPv4,
    user_agent String,
    session_id String,
    
    -- Indexes mixed with columns
    INDEX action_bloom action TYPE bloom_filter GRANULARITY 1,
    INDEX timestamp_minmax timestamp TYPE minmax GRANULARITY 1,
    INDEX ip_set ip_address TYPE set(1000) GRANULARITY 1,
    
    -- Constraints
    CONSTRAINT valid_timestamp CHECK timestamp >= '2020-01-01 00:00:00'
    
) ENGINE = MergeTree()
ORDER BY (user_id, timestamp)
PARTITION BY toYYYYMM(timestamp);