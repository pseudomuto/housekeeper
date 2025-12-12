CREATE DICTIONARY `reordered_dict` (
    `id`   UInt64,
    `name` String
)
PRIMARY KEY `id`
SOURCE(HTTP(url 'http://localhost/data.json' format 'JSONEachRow'))
LAYOUT(FLAT())
LIFETIME(MIN 300 MAX 3600)
SETTINGS(max_threads = 2)
COMMENT 'Dictionary with flexible clause ordering';
