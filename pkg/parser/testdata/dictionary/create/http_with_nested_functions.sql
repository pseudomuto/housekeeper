CREATE DICTIONARY `complex_dict` (
    `id`   UInt64,
    `data` String
)
PRIMARY KEY `id`
SOURCE(HTTP(url 'http://api.example.com/data' headers(list (header('auth-token')))))
LAYOUT(HASHED())
LIFETIME(MIN 300 MAX 1800);
