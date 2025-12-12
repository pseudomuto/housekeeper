CREATE MATERIALIZED VIEW `mv_to_table`
TO `analytics`.`target_table`
AS SELECT *
FROM `source_table`
WHERE `status` = 'active';
