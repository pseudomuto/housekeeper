CREATE DICTIONARY IF NOT EXISTS `analytics`.`user_mapping` ON CLUSTER `production` (
    `user_id`   UInt64 IS_OBJECT_ID,
    `user_name` String,
    `group_id`  UInt32 HIERARCHICAL,
    `status`    String INJECTIVE
)
PRIMARY KEY `user_id`, `group_id`
SOURCE(ClickHouse(host 'localhost' port 9000 user 'default' password '' db 'users' table 'mapping'))
LAYOUT(COMPLEX_KEY_HASHED())
LIFETIME(MIN 60 MAX 3600)
SETTINGS(max_block_size = 8192, max_threads = 2)
COMMENT 'Complex user mapping dictionary';
