CREATE TABLE `pg_import` AS `postgresql`('host:5432', 'database', 'table', 'user', 'password')
ENGINE = MergeTree()
ORDER BY `id`;
