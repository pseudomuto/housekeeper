CREATE TABLE `user_profiles` (
    `user_id`        UInt64,
    `profile_data`   Map(String, Nullable(String)),
    `tags`           Array(LowCardinality(String)),
    `coordinates`    Nullable(Tuple(`lat` Float64, `lon` Float64)),
    `computed_field` String,
    `age_alias`      UInt8,
    `default_data`   String
)
ENGINE = MergeTree()
ORDER BY `user_id`
PARTITION BY `user_id` % 100;
