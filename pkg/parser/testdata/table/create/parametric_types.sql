CREATE TABLE `measurements` (
    `id`              UInt64,
    `device_id`       FixedString(16),
    `value`           Decimal(10, 4),
    `precision_value` Decimal(38, 6),
    `created_at`      DateTime64(3, 'UTC'),
    `config_data`     String CODEC(LZ4HC(9))
)
ENGINE = MergeTree()
ORDER BY (`device_id`, `created_at`);
