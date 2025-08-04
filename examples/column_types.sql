-- Example ClickHouse column definitions demonstrating various data types and options

-- Basic numeric types
user_id UInt64
age UInt8 DEFAULT 18
score Float32
balance Decimal(12, 2)

-- String types
name String
fixed_code FixedString(10)
email LowCardinality(String) COMMENT 'User email address'

-- Date and time types
created_at DateTime DEFAULT now()
updated_at DateTime64(3) TTL updated_at + INTERVAL 1 YEAR
birth_date Date

-- Nullable types
middle_name Nullable(String)
phone Nullable(FixedString(20))

-- Array types
tags Array(String) DEFAULT []
scores Array(Float32)
matrix Array(Array(Int32))

-- Tuple types
coordinates Tuple(lat Float64, lon Float64)
address Tuple(street String, city String, zip FixedString(5))
point Tuple(Float64, Float64)  -- unnamed tuple

-- Map types
settings Map(String, String)
user_scores Map(String, Float64)

-- Nested types
events Nested(
    timestamp DateTime,
    type String,
    value Float64
)

-- Complex expressions
full_name String MATERIALIZED concat(first_name, ' ', last_name)
age_days UInt32 ALIAS dateDiff('day', birth_date, today())
json_data String DEFAULT '{}' CODEC(ZSTD(3))

-- Multiple modifiers
temp_data String DEFAULT '' CODEC(LZ4HC(9)) TTL created_at + INTERVAL 30 DAY COMMENT 'Temporary user data'

-- Complex nested types
user_activity Array(Tuple(
    timestamp DateTime,
    action LowCardinality(String),
    metadata Map(String, String)
))

-- Nullable complex types
optional_location Nullable(Tuple(lat Float64, lon Float64))
optional_tags Nullable(Array(String))

-- LowCardinality with nullable
status LowCardinality(Nullable(String)) DEFAULT 'active'