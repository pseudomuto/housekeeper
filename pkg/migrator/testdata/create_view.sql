CREATE MATERIALIZED VIEW `analytics`.`mv_stats`
ENGINE = MergeTree() ORDER BY `date`
AS SELECT
    toDate(`timestamp`) AS `date`,
    count() AS `cnt`
FROM `events`
GROUP BY `date`;

CREATE VIEW `analytics`.`stats`
AS SELECT count(*) AS `total`
FROM `events`;