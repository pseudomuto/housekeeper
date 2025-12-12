CREATE OR REPLACE DICTIONARY `users_dict` (
    `id`         UInt64,
    `name`       String DEFAULT 'Unknown',
    `email`      String,
    `department` String EXPRESSION 'IT'
)
PRIMARY KEY `id`
SOURCE(MySQL(host 'localhost' port 3306 user 'root' password '' db 'test' table 'users'))
LAYOUT(HASHED())
LIFETIME(3600)
SETTINGS(max_threads = 4)
COMMENT 'User directory';
