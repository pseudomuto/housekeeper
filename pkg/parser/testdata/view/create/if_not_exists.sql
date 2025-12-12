CREATE VIEW IF NOT EXISTS `users_view`
AS SELECT
    `id`,
    `name`
FROM `users`
WHERE `active` = 1;
