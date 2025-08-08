-- Current state: table with old name
CREATE TABLE old_users (id UInt64, name String) ENGINE = MergeTree() ORDER BY id
;
-- Target state: same table with new name
CREATE TABLE users (id UInt64, name String) ENGINE = MergeTree() ORDER BY id;