CREATE DICTIONARY `users_dict` (
    `id`   UInt64,
    `name` String
)
PRIMARY KEY `id`
SOURCE(HTTP(url 'http://localhost/users' format 'JSONEachRow' headers(header ('X-API-Key'))))
LAYOUT(FLAT())
LIFETIME(3600);
