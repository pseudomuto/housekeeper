CREATE ROLE IF NOT EXISTS `superuser` ON CLUSTER `production` SETTINGS `max_memory_usage` = 10000000000, `readonly` = 0;
