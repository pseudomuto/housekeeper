ALTER ROLE IF EXISTS `dbadmin` ON CLUSTER `staging` RENAME TO `database_admin` SETTINGS `max_memory_usage` = 20000000000;
