-- housekeeper:snapshot
-- version: 20250906192107_snapshot
-- description: Normal test after refactoring
-- created_at: 2025-09-06T19:21:07Z
-- included_migrations: 001_test,20250906192056_snapshot
-- cumulative_hash: 1H+dwrpbsT+G+F+zV/2z/T1Yby2AnrTfdMpQO6xrGuU=

-- Cumulative SQL from all included migrations
CREATE TABLE `test` (
    `id` UInt64
)
ENGINE = Memory();

CREATE DATABASE IF NOT EXISTS `ecommerce` ON CLUSTER `demo` ENGINE = Atomic COMMENT 'E-commerce analytics database';

CREATE TABLE `ecommerce`.`categories_source` ON CLUSTER `demo` (
    `id`        UInt64,
    `name`      String,
    `parent_id` UInt64,
    `level`     UInt8,
    `is_active` Bool
)
ENGINE = Memory()
COMMENT 'Source data for categories dictionary';

CREATE TABLE `ecommerce`.`user_segments_source` ON CLUSTER `demo` (
    `user_id`      UInt64,
    `segment`      String,
    `score`        Float32,
    `last_updated` DateTime
)
ENGINE = Memory()
COMMENT 'Source data for user segments dictionary';

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

CREATE DICTIONARY `ecommerce`.`countries_dict` ON CLUSTER `demo` (
    `cca2`     String,
    `region`   String,
    `unMember` UInt8
)
PRIMARY KEY `cca2`
SOURCE(HTTP(url 'https://restcountries.com/v3.1/all?fields=cca2,region,unMember' format 'JSONEachRow' headers(header(name 'Content-Type' value 'application/json'))))
LAYOUT(HASHED())
LIFETIME(86400)
COMMENT 'Country codes with regions and UN membership status from REST Countries API using HTTP source';

CREATE DICTIONARY `ecommerce`.`categories_dict` ON CLUSTER `demo` (
    `id`        UInt64,
    `name`      String,
    `parent_id` UInt64 DEFAULT 0 HIERARCHICAL,
    `level`     UInt8 DEFAULT 0,
    `is_active` Bool DEFAULT true
)
PRIMARY KEY `id`
SOURCE(CLICKHOUSE(host 'localhost' port 9000 user 'default' password '' db 'ecommerce' table 'categories_source'))
LAYOUT(HASHED())
LIFETIME(MIN 300 MAX 3600)
COMMENT 'Product category hierarchy with parent relationships';

CREATE DICTIONARY `ecommerce`.`user_segments_dict` ON CLUSTER `demo` (
    `user_id`      UInt64,
    `segment`      String,
    `score`        Float32 DEFAULT 0.0,
    `last_updated` DateTime
)
PRIMARY KEY `user_id`
SOURCE(CLICKHOUSE(host 'localhost' port 9000 user 'default' password '' db 'ecommerce' table 'user_segments_source'))
LAYOUT(FLAT())
LIFETIME(1800)
SETTINGS(max_threads = 2)
COMMENT 'User segmentation from ML service';

CREATE VIEW `ecommerce`.`daily_sales` ON CLUSTER `demo`
AS SELECT
    `order_date`,
    count() AS `total_orders`,
    sum(`total_amount`) AS `total_revenue`,
    avg(`total_amount`) AS `avg_order_value`,
    uniq(`user_id`) AS `unique_customers`
FROM `ecommerce`.`orders`
WHERE `status` = 'completed'
GROUP BY `order_date`
ORDER BY `order_date` DESC;

CREATE VIEW `ecommerce`.`user_activity` ON CLUSTER `demo`
AS SELECT
    `u`.`id`,
    `u`.`email`,
    `u`.`country`,
    `u`.`total_orders`,
    `u`.`total_spent`,
    `last_event`.`last_activity`,
    CASE WHEN last_event.last_activity>=today()-7 THEN 'Active' WHEN last_event.last_activity>=today()-30 THEN 'Recent' ELSE 'Inactive' END AS `activity_status`
FROM `ecommerce`.`users` AS `u`
LEFT JOIN (SELECT
    `user_id`,
    max(`timestamp`) AS `last_activity`
FROM `ecommerce`.`events`
GROUP BY `user_id`) AS `last_event` ON `u`.`id` = `last_event`.`user_id`;

CREATE VIEW `ecommerce`.`top_products` ON CLUSTER `demo`
AS WITH
    `product_metrics` AS (
        SELECT
            `p`.`id`,
            `p`.`name`,
            `p`.`category`,
            `p`.`brand`,
            `p`.`price`,
            coalesce(`stats`.`total_quantity`, 0) AS `total_sold`,
            coalesce(`stats`.`total_revenue`, 0) AS `revenue`
        FROM `ecommerce`.`products` AS `p`
        LEFT JOIN (SELECT
            `product_id`,
            sum(`quantity`) AS `total_quantity`,
            sum(`total_price`) AS `total_revenue`
        FROM `ecommerce`.`order_items` AS `oi`
        JOIN `ecommerce`.`orders` AS `o` ON `oi`.`order_id` = `o`.`id`
        WHERE `o`.`status` = 'completed' AND `o`.`order_date` >= today() - 90
        GROUP BY `product_id`) AS `stats` ON `p`.`id` = `stats`.`product_id`
    )
SELECT
    `category`,
    `id` AS `product_id`,
    `name` AS `product_name`,
    `brand`,
    `price`,
    `total_sold`,
    `revenue`,
    row_number() OVER (PARTITION BY category ORDER BY revenue DESC) AS `rank_in_category`
FROM `product_metrics`
WHERE `total_sold` > 0
ORDER BY `category`, `rank_in_category`;

CREATE MATERIALIZED VIEW `ecommerce`.`mv_product_stats` ON CLUSTER `demo`
ENGINE = MergeTree() ORDER BY (`product_id`, `order_date`)
POPULATE
AS SELECT
    `oi`.`product_id`,
    `o`.`order_date`,
    count() AS `orders_count`,
    sum(`oi`.`quantity`) AS `total_quantity`,
    sum(`oi`.`total_price`) AS `total_revenue`,
    avg(`oi`.`unit_price`) AS `avg_price`
FROM `ecommerce`.`order_items` AS `oi`
JOIN `ecommerce`.`orders` AS `o` ON `oi`.`order_id` = `o`.`id`
WHERE `o`.`status` = 'completed'
GROUP BY `oi`.`product_id`, `o`.`order_date`;

CREATE MATERIALIZED VIEW `ecommerce`.`mv_hourly_events` ON CLUSTER `demo`
ENGINE = MergeTree() ORDER BY (`event_hour`, `event_type`)
AS SELECT
    toStartOfHour(`timestamp`) AS `event_hour`,
    `event_type`,
    `country`,
    count() AS `event_count`,
    uniq(`user_id`) AS `unique_users`,
    uniq(`session_id`) AS `unique_sessions`
FROM `ecommerce`.`events`
GROUP BY `event_hour`, `event_type`, `country`;