DROP TABLE `kafka_events`;

CREATE TABLE `kafka_events` (
    `id`        UInt64,
    `message`   String,
    `timestamp` DateTime
)
ENGINE = Kafka('broker:9092', 'topic', 'group', 'JSONEachRow');