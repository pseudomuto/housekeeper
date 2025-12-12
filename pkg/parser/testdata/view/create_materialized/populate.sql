CREATE MATERIALIZED VIEW `mv_with_populate`
ENGINE = AggregatingMergeTree() ORDER BY `date`
POPULATE
AS SELECT
    toDate(`timestamp`) AS `date`,
    sum(`amount`) AS `total`
FROM `transactions`
GROUP BY `date`;
