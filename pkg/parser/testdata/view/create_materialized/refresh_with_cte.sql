CREATE MATERIALIZED VIEW `mv_cte`
REFRESH EVERY 10 SECONDS
APPEND TO `target`
AS WITH
    `pending` AS (
        SELECT `id`
        FROM `lifecycle`
        GROUP BY `id`
        HAVING max(`signal`) = 1
    )
SELECT
    `id`,
    min(`ts`) AS `min_ts`
FROM `raw`
WHERE `id` IN pending
GROUP BY `id`;
