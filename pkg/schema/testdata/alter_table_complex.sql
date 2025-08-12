ALTER TABLE `events`
    MODIFY COLUMN `event_type` LowCardinality(String),
    MODIFY COLUMN `timestamp` DateTime DEFAULT now(),
    ADD COLUMN `data` Map(String, String),
    ADD COLUMN `metadata` Nullable(String);