CREATE TABLE `user_profiles` (
    `user_id`      UInt64,
    `email`        String,
    `age`          UInt8,
    `profile_data` Map(String, String),
    `created_at`   DateTime DEFAULT now(),
    INDEX `email_bloom` `email` TYPE  GRANULARITY 1,
    CONSTRAINT `valid_age` CHECK `age` BETWEEN 13 AND 120,
    CONSTRAINT `valid_email` CHECK `email` LIKE '%@%'
)
ENGINE = MergeTree()
ORDER BY `user_id`;
