CREATE TABLE `cluster_data` AS `cluster`('my_cluster', 'default', 'events')
ENGINE = MergeTree()
ORDER BY `id`;
