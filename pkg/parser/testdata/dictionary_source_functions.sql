-- Dictionary with simple function call in SOURCE
CREATE DICTIONARY users_dict
(
    id UInt64,
    name String
)
PRIMARY KEY id
SOURCE(HTTP(url 'http://localhost/users' format 'JSONEachRow' headers(header('X-API-Key'))))
LAYOUT(FLAT())
LIFETIME(3600);

-- Dictionary with nested function calls in SOURCE  
CREATE DICTIONARY complex_dict
(
    id UInt64,
    data String
)
PRIMARY KEY id
SOURCE(HTTP(url 'http://api.example.com/data' headers(list(header('auth-token')))))
LAYOUT(HASHED())
LIFETIME(MIN 300 MAX 1800);