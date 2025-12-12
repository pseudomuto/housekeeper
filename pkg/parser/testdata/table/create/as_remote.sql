CREATE TABLE `remote_copy` AS `remote`('host:9000', 'db', 'table')
ENGINE = MergeTree()
ORDER BY `id`;
