-- Current state: Kafka table with basic columns
CREATE TABLE kafka_events (id UInt64, message String) ENGINE = Kafka('broker:9092', 'topic', 'group', 'JSONEachRow')
;
-- Target state: Kafka table with additional timestamp column (will use DROP+CREATE strategy)
CREATE TABLE kafka_events (id UInt64, message String, timestamp DateTime) ENGINE = Kafka('broker:9092', 'topic', 'group', 'JSONEachRow');