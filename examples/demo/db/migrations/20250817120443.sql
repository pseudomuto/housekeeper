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