CREATE VIEW `analytics`.`user_rankings`
AS SELECT
    `user_id`,
    `name`,
    `score`,
    row_number() OVER (ORDER BY score DESC) AS `rank`,
    rank() OVER (PARTITION BY category ORDER BY score DESC) AS `category_rank`
FROM `user_scores`
ORDER BY `score` DESC;
