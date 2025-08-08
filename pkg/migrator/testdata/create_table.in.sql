-- Current state: no table exists
;
-- Target state: create new users table with basic columns
CREATE TABLE users (
    id UInt64,
    name String,
    email String
) ENGINE = MergeTree()
ORDER BY id;