SELECT *
FROM `users` AS `u`
LEFT JOIN `orders` AS `o` ON `u`.`id` = `o`.`user_id`;
