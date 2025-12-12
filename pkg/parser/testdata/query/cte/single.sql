WITH
    `active_users` AS (
        SELECT *
        FROM `users`
        WHERE `active` = 1
    )
SELECT *
FROM `active_users`;
