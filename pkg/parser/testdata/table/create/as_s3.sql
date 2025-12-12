CREATE TABLE `s3_import` AS `s3Table`('https://bucket.s3.amazonaws.com/data.csv', 'CSV')
ENGINE = MergeTree()
ORDER BY tuple();
