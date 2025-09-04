CREATE NAMED COLLECTION `my_s3_collection` AS
    `access_key_id` = 'AKIAIOSFODNN7EXAMPLE',
    `secret_access_key` = 'wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY',
    `endpoint` = 'https://s3.amazonaws.com/',
    `region` = 'us-east-1' NOT OVERRIDABLE;

CREATE OR REPLACE NAMED COLLECTION IF NOT EXISTS `kafka_config` ON CLUSTER `production` AS
    `kafka_broker_list` = 'localhost:9092',
    `kafka_topic_list` = 'events',
    `kafka_group_name` = 'clickhouse',
    `kafka_format` = 'JSONEachRow',
    `kafka_max_block_size` = 1048576 OVERRIDABLE
COMMENT 'Kafka configuration for events';

ALTER NAMED COLLECTION `kafka_config`
    SET `kafka_topic_list` = 'events,logs' OVERRIDABLE, `kafka_max_block_size` = 2097152 NOT OVERRIDABLE;

DROP NAMED COLLECTION IF EXISTS `old_s3_config` ON CLUSTER `production`;
