SELECT *
FROM `users` AS `u`
RIGHT JOIN `orders` AS `o` ON `u`.`id` = `o`.`user_id`;
