WITH
    `active_users` AS (
        SELECT *
        FROM `users`
        WHERE `active` = 1
    ),
    `recent_orders` AS (
        SELECT *
        FROM `orders`
        WHERE `date` > '2023-01-01'
    )
SELECT *
FROM `active_users`
JOIN `recent_orders` ON `active_users`.`id` = `recent_orders`.`user_id`;
