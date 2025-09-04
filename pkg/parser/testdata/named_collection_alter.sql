ALTER NAMED COLLECTION kafka_config
    SET kafka_topic_list = 'events,logs' OVERRIDABLE,
        kafka_max_block_size = 2097152 NOT OVERRIDABLE
    DELETE kafka_skip_broken_messages;