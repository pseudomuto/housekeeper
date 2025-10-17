-- Current state: table with basic columns
CREATE TABLE users (
    id UInt64,
    name String
) ENGINE = MergeTree()
ORDER BY id
;
-- Target state: table with additional email column
CREATE TABLE users (
    id UInt64,
    name String,
    email String
) ENGINE = MergeTree()
ORDER BY id;
