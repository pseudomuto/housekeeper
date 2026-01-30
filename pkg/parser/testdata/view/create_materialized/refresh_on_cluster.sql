CREATE MATERIALIZED VIEW `db`.`mv_refresh` ON CLUSTER `mycluster`
REFRESH EVERY 30 SECONDS
APPEND TO `db`.`target`
AS SELECT
    `id`,
    `name`
FROM `source`;
