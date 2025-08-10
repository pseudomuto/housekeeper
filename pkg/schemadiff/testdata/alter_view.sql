DROP TABLE `analytics`.`mv_stats`;

CREATE MATERIALIZED VIEW `analytics`.`mv_stats`
ENGINE = MergeTree() ORDER BY `date`
AS SELECT
    toDate(`timestamp`) AS `date`,
    count() AS `cnt`
FROM `events`
GROUP BY `date`;

CREATE OR REPLACE VIEW `analytics`.`stats`
AS SELECT
    count(*) AS `total`,
    max(`timestamp`) AS `latest`
FROM `events`;