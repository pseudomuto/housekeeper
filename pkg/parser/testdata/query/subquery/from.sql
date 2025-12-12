SELECT *
FROM (SELECT
    `id`,
    `name`
FROM `users`
WHERE `active` = 1) AS `active_users`;
