-- Basic dictionary
CREATE DICTIONARY users_dict
(
    id UInt64,
    name String,
    email String DEFAULT ''
)
PRIMARY KEY id
SOURCE(HTTP(url 'http://localhost/users.json' format 'JSONEachRow'))
LAYOUT(FLAT())
LIFETIME(MIN 0 MAX 1000);

-- Dictionary with OR REPLACE
CREATE OR REPLACE DICTIONARY users_dict
(
    id UInt64,
    name String DEFAULT 'Unknown',
    email String,
    department String EXPRESSION 'IT'
)
PRIMARY KEY id
SOURCE(MySQL(host 'localhost' port 3306 user 'root' password '' db 'test' table 'users'))
LAYOUT(HASHED())
LIFETIME(3600)
SETTINGS(max_threads = 4)
COMMENT 'User directory';

-- Dictionary with IF NOT EXISTS and cluster
CREATE DICTIONARY IF NOT EXISTS analytics.user_mapping ON CLUSTER production
(
    user_id UInt64 IS_OBJECT_ID,
    user_name String,
    group_id UInt32 HIERARCHICAL,
    status String INJECTIVE
)
PRIMARY KEY user_id, group_id
SOURCE(ClickHouse(host 'localhost' port 9000 user 'default' password '' db 'users' table 'mapping'))
LAYOUT(COMPLEX_KEY_HASHED())
LIFETIME(MIN 60 MAX 3600)
SETTINGS(max_block_size = 8192, max_threads = 2)
COMMENT 'Complex user mapping dictionary';

-- Simple dictionary with multiple columns and reversed MIN/MAX order
CREATE DICTIONARY simple_dict
(
    key1 String,
    key2 UInt32,
    value1 String DEFAULT 'N/A',
    value2 Float64 EXPRESSION '0.0'
)
PRIMARY KEY key1, key2
SOURCE(File(path '/data/dict.csv' format 'CSV'))
LAYOUT(COMPLEX_KEY_CACHE(size_in_cells 1000000))
LIFETIME(MAX 1200 MIN 300);

-- Dictionary with backtick identifiers for reserved keywords
CREATE DICTIONARY `user-dict`.`order-lookup`
(
    `user-id` UInt64 IS_OBJECT_ID,
    `order` String INJECTIVE,
    `select` String DEFAULT 'default_value'
)
PRIMARY KEY `user-id`
SOURCE(HTTP(url 'http://api.example.com/orders'))
LAYOUT(HASHED())
LIFETIME(3600);