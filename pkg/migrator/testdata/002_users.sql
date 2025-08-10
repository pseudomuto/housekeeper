-- Users table creation
CREATE TABLE test.users (
    id UInt64,
    email String,
    created_at DateTime DEFAULT now(),
    metadata Map(String, String) DEFAULT map()
) ENGINE = MergeTree() 
ORDER BY (id, created_at)
PARTITION BY toYYYYMM(created_at);