SELECT
    `u`.`id`,
    `u`.`name`,
    count(`o`.`id`) AS `order_count`,
    sum(`o`.`amount`) AS `total_spent`
FROM `users` AS `u`
LEFT JOIN `orders` AS `o` ON `u`.`id` = `o`.`user_id`
WHERE `u`.`active` = 1 AND `u`.`created_at` >= '2023-01-01'
GROUP BY `u`.`id`, `u`.`name`
HAVING count(`o`.`id`) > 0
ORDER BY `total_spent` DESC
LIMIT 100;
