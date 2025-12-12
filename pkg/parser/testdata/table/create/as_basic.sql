CREATE TABLE `copy` AS `source`
ENGINE = MergeTree()
ORDER BY `id`;
