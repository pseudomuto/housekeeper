SELECT *
FROM `users` AS `u`
JOIN `orders` AS `o` USING (`user_id`);
