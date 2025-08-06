-- Users table definition
CREATE TABLE imports_db.users (
    id UInt64,
    username String,
    email String,
    created_at DateTime DEFAULT now()
) ENGINE = MergeTree() ORDER BY id;