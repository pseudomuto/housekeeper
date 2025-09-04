SELECT
    `domain`,
    `browser`,
    count(*) AS `total`
FROM `events`
WHERE `date` >= '2024-01-01'
GROUP BY ALL;
