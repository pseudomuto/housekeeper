CREATE NAMED COLLECTION kafka_config AS
    kafka_broker_list = 'localhost:9092',
    kafka_topic_list = 'events',
    kafka_group_name = 'clickhouse',
    kafka_format = 'JSONEachRow',
    kafka_max_block_size = 1048576,
    kafka_skip_broken_messages = 1
OVERRIDABLE;