CREATE VIEW `analytics-db`.`daily-summary`
AS SELECT
    `order-date` AS `date`,
    count(*) AS `total-orders`
FROM `orders-table`
GROUP BY `order-date`;
