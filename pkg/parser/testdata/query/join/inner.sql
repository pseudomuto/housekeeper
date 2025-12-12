SELECT *
FROM `users` AS `u`
INNER JOIN `orders` AS `o` ON `u`.`id` = `o`.`user_id`;
