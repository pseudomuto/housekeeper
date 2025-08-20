CREATE DATABASE `ecommerce` ON CLUSTER `demo` ENGINE = Atomic COMMENT 'E-commerce analytics database';

CREATE TABLE `ecommerce`.`events` ON CLUSTER `demo` (
    `id`         UInt64,
    `user_id`    UInt64,
    `event_type` LowCardinality(String),
    `timestamp`  DateTime DEFAULT now(),
    `session_id` String,
    `page_url`   String,
    `user_agent` String DEFAULT '',
    `country`    LowCardinality(String) DEFAULT 'Unknown',
    `properties` Map(String, String) DEFAULT map(),
    `created_at` DateTime DEFAULT now()
)
ENGINE = MergeTree()
ORDER BY (`user_id`, `timestamp`)
PARTITION BY toYYYYMM(`timestamp`)
SETTINGS index_granularity = 8192
COMMENT 'User interaction events';

CREATE TABLE `ecommerce`.`order_items` ON CLUSTER `demo` (
    `order_id`        UInt64,
    `product_id`      UInt64,
    `quantity`        UInt32,
    `unit_price`      Decimal64(2),
    `total_price`     Decimal64(2) MATERIALIZED `quantity` * `unit_price`,
    `discount_amount` Decimal64(2) DEFAULT 0,
    `created_at`      DateTime DEFAULT now()
)
ENGINE = MergeTree()
ORDER BY (`order_id`, `product_id`)
COMMENT 'Individual items within orders';

CREATE TABLE `ecommerce`.`orders` ON CLUSTER `demo` (
    `id`                  UInt64,
    `user_id`             UInt64,
    `order_date`          Date,
    `order_timestamp`     DateTime,
    `status`              LowCardinality(String),
    `total_amount`        Decimal64(2),
    `currency`            LowCardinality(String) DEFAULT 'USD',
    `payment_method`      LowCardinality(String),
    `shipping_address_id` Nullable(UInt64),
    `discount_amount`     Decimal64(2) DEFAULT 0,
    `tax_amount`          Decimal64(2) DEFAULT 0,
    `created_at`          DateTime DEFAULT now(),
    `updated_at`          DateTime DEFAULT now()
)
ENGINE = MergeTree()
ORDER BY (`user_id`, `order_date`)
PARTITION BY toYYYYMM(`order_date`)
TTL `order_timestamp` + INTERVAL 7 YEAR
COMMENT 'Customer orders';

CREATE TABLE `ecommerce`.`products` ON CLUSTER `demo` (
    `id`            UInt64,
    `name`          String,
    `category`      LowCardinality(String),
    `subcategory`   LowCardinality(String),
    `brand`         LowCardinality(String),
    `price`         Decimal64(2),
    `cost`          Decimal64(2),
    `weight_grams`  UInt32,
    `dimensions_cm` Array(UInt32),
    `in_stock`      Bool DEFAULT true,
    `tags`          Array(String),
    `metadata`      Nullable(String),
    `created_at`    DateTime DEFAULT now(),
    `updated_at`    DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(`updated_at`)
ORDER BY `id`
COMMENT 'Product catalog with versioning';

CREATE TABLE `ecommerce`.`users` ON CLUSTER `demo` (
    `id`                UInt64,
    `email`             String,
    `first_name`        String,
    `last_name`         String,
    `date_of_birth`     Nullable(Date),
    `registration_date` Date,
    `country`           LowCardinality(String),
    `city`              String DEFAULT '',
    `is_premium`        Bool DEFAULT false,
    `total_orders`      UInt32 DEFAULT 0,
    `total_spent`       Decimal64(2) DEFAULT 0,
    `last_login_at`     Nullable(DateTime),
    `created_at`        DateTime DEFAULT now(),
    `updated_at`        DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(`updated_at`)
ORDER BY `id`
COMMENT 'User profiles with aggregate metrics';

CREATE TABLE `housekeeper`.`revisions` (
    `version`             String COMMENT 'The version (e.g. 20250101123045)',
    `executed_at`         DateTime(3, 'UTC') COMMENT 'The UTC time at which this attempt was executed',
    `execution_time_ms`   UInt64 COMMENT 'How long the migration took to run',
    `type`                String COMMENT 'The type of migration this is (normal, snapshot, etc)',
    `error`               Nullable(String) COMMENT 'The error message FROM the last attempt (if any)',
    `applied`             UInt32 COMMENT 'The number of applied statements',
    `total`               UInt32 COMMENT 'The total number of statements in the migration',
    `hash`                String COMMENT 'The h1 hash of the migration',
    `partial_hashes`      Array(String) COMMENT 'h1 hashes for each statement in the migration',
    `housekeeper_version` String COMMENT 'The version of housekeeper used to run the migration'
)
ENGINE = MergeTree()
ORDER BY `version`
PARTITION BY toYYYYMM(`executed_at`)
COMMENT 'Table used to track migrations';