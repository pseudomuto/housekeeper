CREATE TABLE `user-db`.`order-table` (
    `user-id`    UInt64,
    `order-id`   String,
    `order-date` Date,
    `select`     String,
    `group`      LowCardinality(String)
)
ENGINE = MergeTree()
ORDER BY (`user-id`, `order-date`);
