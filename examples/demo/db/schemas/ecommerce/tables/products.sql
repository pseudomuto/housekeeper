-- Products catalog
CREATE TABLE ecommerce.products ON CLUSTER demo (
    id UInt64,
    name String,
    category LowCardinality(String),
    subcategory LowCardinality(String),
    brand LowCardinality(String),
    price Decimal64(2),
    cost Decimal64(2),
    weight_grams UInt32,
    dimensions_cm Array(UInt32),
    in_stock Bool DEFAULT true,
    tags Array(String),
    metadata Nullable(String),
    created_at DateTime DEFAULT now(),
    updated_at DateTime DEFAULT now()
) ENGINE = ReplacingMergeTree(updated_at)
ORDER BY id
COMMENT 'Product catalog with versioning';