CREATE TABLE `users` (
    `id`    UInt64,
    `name`  String,
    `email` String
)
ENGINE = MergeTree()
ORDER BY `id`;