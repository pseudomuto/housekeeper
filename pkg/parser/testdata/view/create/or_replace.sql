CREATE OR REPLACE VIEW `analytics`.`updated_view`
AS SELECT
    `id`,
    `name`,
    `updated_at`
FROM `users`
ORDER BY `updated_at` DESC;
