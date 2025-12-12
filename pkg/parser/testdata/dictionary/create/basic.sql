CREATE DICTIONARY `users_dict` (
    `id`    UInt64,
    `name`  String,
    `email` String DEFAULT ''
)
PRIMARY KEY `id`
SOURCE(HTTP(url 'http://localhost/users.json' format 'JSONEachRow'))
LAYOUT(FLAT())
LIFETIME(MIN 0 MAX 1000);
