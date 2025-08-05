-- Table operations with complex features
CREATE TABLE analytics.events (
id UInt64,
user_id UInt64,
event_type LowCardinality(String),
timestamp DateTime DEFAULT now(),
properties String DEFAULT '',
metadata Nullable(String) CODEC(ZSTD),
version UInt32 MATERIALIZED toUnixTimestamp(timestamp)
) ENGINE = MergeTree()
ORDER BY (user_id, timestamp)
PARTITION BY toYYYYMM(timestamp)
PRIMARY KEY (user_id)
SETTINGS index_granularity = 8192;

CREATE OR REPLACE TABLE warehouse.products (
product_id UInt64,
name String,
category LowCardinality(String),
price Decimal(10,2),
created_at DateTime DEFAULT now(),
tags Array(String) DEFAULT array()
) ENGINE = ReplacingMergeTree(created_at)
ORDER BY product_id
COMMENT 'Product catalog table';

ALTER TABLE analytics.events 
ADD COLUMN session_id String DEFAULT '' AFTER user_id,
MODIFY COLUMN metadata String COMMENT 'Updated metadata column',
DROP COLUMN version;