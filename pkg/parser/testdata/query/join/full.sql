SELECT *
FROM `users` AS `u`
FULL JOIN `orders` AS `o` ON `u`.`id` = `o`.`user_id`;
